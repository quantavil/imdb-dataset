"""
Automated unit & regression test suite for IMDb Data Pipeline.
"""

import gzip
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
import pytest

from db import (
    init_db,
    open_db,
    prune_unqualified_titles,
    recalculate_ranks,
    update_enrichment_batch,
    get_titles_needing_enrichment,
    update_episodes_batch,
    upsert_titles_batch,
)
from enrich import enrich_missing_posters, fetch_single_title_metadata, parse_suggestion_item
from export import build_tier_payload, export_dataset
from ingest import (
    fetch_qualifying_ratings,
    run_ingestion,
    stream_and_insert_basics,
    stream_and_update_episodes,
)


@pytest.fixture
def temp_db(tmp_path):
    """Provides a fresh temporary SQLite database."""
    db_file = tmp_path / "test_imdb.db"
    init_db(db_file)
    return db_file


def test_init_db_and_migration(temp_db):
    """Verify that init_db creates all expected columns and indexes."""
    with open_db(temp_db) as conn:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(titles);").fetchall()}
        expected_cols = {
            "imdb_id",
            "title",
            "original_title",
            "title_type",
            "year",
            "end_year",
            "rating",
            "vote_count",
            "runtime_minutes",
            "genres",
            "is_adult",
            "is_animation",
            "poster_url",
            "cast_members",
            "directors",
            "writers",
            "parent_id",
            "season_number",
            "episode_number",
            "popularity_rank",
            "rank",
            "updated_at",
            "enriched_at",
        }
        assert expected_cols.issubset(cols), f"Missing columns: {expected_cols - cols}"

        idx_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'idx_titles_rating_votes'"
        ).fetchone()["sql"]
        assert "imdb_id" in idx_sql


def test_legacy_is_anime_migration(tmp_path):
    """Verify that init_db renames legacy is_anime column to is_animation."""
    legacy_db = tmp_path / "legacy.db"
    with open_db(legacy_db) as conn:
        conn.execute("""
        CREATE TABLE titles (
            imdb_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            original_title TEXT,
            title_type TEXT NOT NULL,
            year INTEGER,
            end_year INTEGER,
            rating REAL NOT NULL,
            vote_count INTEGER NOT NULL,
            runtime_minutes INTEGER,
            genres TEXT,
            is_anime INTEGER DEFAULT 0
        );
        """)
        conn.execute(
            "INSERT INTO titles (imdb_id, title, title_type, rating, vote_count, is_anime) VALUES ('tt99', 'Old Anime', 'movie', 8.0, 5000, 1);"
        )

    # Run init_db which should migrate is_anime to is_animation
    init_db(legacy_db)

    with open_db(legacy_db) as conn:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(titles);").fetchall()}
        assert "is_animation" in cols
        row = conn.execute(
            "SELECT imdb_id, is_animation FROM titles WHERE imdb_id = 'tt99'"
        ).fetchone()
        assert row["is_animation"] == 1


def test_upsert_and_recalculate_ranks(temp_db):
    """Verify batch upsert and rank calculation logic."""
    sample_rows = [
        (
            "tt001",
            "Movie Low Votes",
            "Movie Low Votes",
            "movie",
            2020,
            None,
            9.9,
            150,
            120,
            "Drama",
            0,
            0,
        ),
        (
            "tt002",
            "Movie High Votes",
            "Movie High Votes",
            "movie",
            2021,
            None,
            9.9,
            50000,
            140,
            "Action",
            0,
            0,
        ),
        (
            "tt003",
            "Movie Low Rating",
            "Movie Low Rating",
            "movie",
            2022,
            None,
            7.5,
            100000,
            90,
            "Comedy",
            0,
            0,
        ),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    recalculate_ranks(db_path=temp_db)

    with open_db(temp_db) as conn:
        rows = conn.execute("SELECT imdb_id, rank FROM titles ORDER BY rank ASC").fetchall()
        assert rows[0]["imdb_id"] == "tt002"
        assert rows[0]["rank"] == 1
        assert rows[1]["imdb_id"] == "tt001"
        assert rows[1]["rank"] == 2
        assert rows[2]["imdb_id"] == "tt003"
        assert rows[2]["rank"] == 3


def test_deterministic_rank_ties(temp_db):
    """Verify that ties in rating and vote_count are broken deterministically by imdb_id ASC."""
    sample_rows = [
        ("tt999", "Movie B", "Movie B", "movie", 2020, None, 9.0, 5000, 100, "Drama", 0, 0),
        ("tt111", "Movie A", "Movie A", "movie", 2020, None, 9.0, 5000, 100, "Drama", 0, 0),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    recalculate_ranks(db_path=temp_db)

    with open_db(temp_db) as conn:
        rows = conn.execute("SELECT imdb_id, rank FROM titles ORDER BY rank ASC").fetchall()
        assert rows[0]["imdb_id"] == "tt111"
        assert rows[0]["rank"] == 1
        assert rows[1]["imdb_id"] == "tt999"
        assert rows[1]["rank"] == 2


def test_enrichment_parser_safe_none():
    """Verify that parse_suggestion_item handles NoneType 'i' and missing fields safely."""
    item_none_image = {"id": "tt1234567", "i": None, "s": "Actor One, Actor Two", "rank": 42}
    imdb_id, poster, cast, pop = parse_suggestion_item("tt1234567", [item_none_image])
    assert imdb_id == "tt1234567"
    assert poster is None
    assert cast == "Actor One, Actor Two"
    assert pop == 42

    item_valid = {
        "id": "tt1234567",
        "i": {"imageUrl": "https://example.com/poster.jpg"},
        "s": "Actor One",
        "rank": 1,
    }
    imdb_id, poster, cast, pop = parse_suggestion_item("tt1234567", [item_valid])
    assert poster == "https://example.com/poster.jpg"
    assert cast == "Actor One"
    assert pop == 1


def test_enrichment_batch_and_queue(temp_db):
    """Verify that update_enrichment_batch marks enriched_at."""
    sample_rows = [
        ("tt101", "Title 1", "Title 1", "movie", 2020, None, 8.0, 500, 100, "Drama", 0, 0),
        ("tt102", "Title 2", "Title 2", "movie", 2020, None, 8.5, 600, 100, "Drama", 0, 0),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    recalculate_ranks(db_path=temp_db)

    needing = get_titles_needing_enrichment(limit=10, db_path=temp_db)
    assert len(needing) == 2

    update_enrichment_batch([("tt102", None, None, None)], db_path=temp_db)

    needing_after = get_titles_needing_enrichment(limit=10, db_path=temp_db)
    assert len(needing_after) == 1
    assert needing_after[0]["imdb_id"] == "tt101"


def test_popularity_rank_cleared_when_unranked(temp_db):
    """Verify that popularity_rank properly updates to None when title drops off trending."""
    sample_rows = [
        (
            "tt401",
            "Trending Title",
            "Trending Title",
            "movie",
            2020,
            None,
            9.0,
            1000,
            100,
            "Drama",
            0,
            0,
        ),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    recalculate_ranks(db_path=temp_db)

    # First enrichment: has poster and popularity rank 5
    update_enrichment_batch(
        [("tt401", "https://example.com/poster.jpg", "Top Cast", 5)], db_path=temp_db
    )

    with open_db(temp_db) as conn:
        row = conn.execute(
            "SELECT poster_url, cast_members, popularity_rank FROM titles WHERE imdb_id = 'tt401'"
        ).fetchone()
        assert row["poster_url"] == "https://example.com/poster.jpg"
        assert row["popularity_rank"] == 5

    # Second enrichment: no longer trending (popularity_rank is None), but poster is omitted in CDN response
    update_enrichment_batch([("tt401", None, None, None)], db_path=temp_db)

    with open_db(temp_db) as conn:
        row = conn.execute(
            "SELECT poster_url, cast_members, popularity_rank FROM titles WHERE imdb_id = 'tt401'"
        ).fetchone()
        # Poster and cast preserved via COALESCE
        assert row["poster_url"] == "https://example.com/poster.jpg"
        assert row["cast_members"] == "Top Cast"
        # Popularity rank cleared to None (not stale)
        assert row["popularity_rank"] is None


def test_enrichment_7_day_ttl(temp_db):
    """Verify that titles with enriched_at older than 7 days are re-queued."""
    sample_rows = [
        ("tt301", "Fresh Title", "Fresh Title", "movie", 2020, None, 9.0, 1000, 100, "Drama", 0, 0),
        ("tt302", "Stale Title", "Stale Title", "movie", 2020, None, 9.0, 1000, 100, "Drama", 0, 0),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    recalculate_ranks(db_path=temp_db)

    # Stamp tt301 as fresh (now) and tt302 as stale (8 days ago)
    with open_db(temp_db) as conn:
        conn.execute("UPDATE titles SET enriched_at = CURRENT_TIMESTAMP WHERE imdb_id = 'tt301'")
        conn.execute(
            "UPDATE titles SET enriched_at = datetime('now', '-8 days') WHERE imdb_id = 'tt302'"
        )

    needing = get_titles_needing_enrichment(limit=10, db_path=temp_db)
    assert len(needing) == 1
    assert needing[0]["imdb_id"] == "tt302"


def test_export_tier_continuous_rank(temp_db):
    """Verify that build_tier_payload computes continuous 1..N ranks and includes new fields."""
    sample_rows = [
        (
            "tt001",
            "Low Vote Title",
            "Low Vote Title",
            "movie",
            2020,
            None,
            9.9,
            100,
            120,
            "Drama",
            0,
            0,
        ),
        (
            "tt002",
            "High Vote Title 1",
            "High Vote Title 1",
            "movie",
            2021,
            None,
            9.5,
            2000,
            140,
            "Action",
            0,
            0,
        ),
        (
            "tt003",
            "High Vote Title 2",
            "High Vote Title 2",
            "tv_episode",
            2022,
            None,
            9.0,
            1500,
            90,
            "Comedy",
            0,
            1,
        ),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    recalculate_ranks(db_path=temp_db)

    update_episodes_batch([("tt003", "ttParentSeries", 2, 5)], db_path=temp_db)

    with open_db(temp_db) as conn:
        payload_1k = build_tier_payload(conn, min_votes=1000)
        assert payload_1k["total"] == 2
        assert payload_1k["stats"]["total_episodes"] == 1
        assert payload_1k["stats"]["total_animation"] == 1
        assert "is_animation" in payload_1k["fields"]
        assert "directors" not in payload_1k["fields"]
        assert "parent_id" in payload_1k["fields"]

        row_003 = next(r for r in payload_1k["data"] if r[0] == "tt003")
        assert row_003[11] == 1  # is_animation
        assert row_003[16] == "ttParentSeries"
        assert row_003[17] == 2
        assert row_003[18] == 5


def test_export_dataset_full_and_gzip(temp_db, tmp_path):
    """Verify that export_dataset generates both titles.json and titles.json.gz atomically."""
    sample_rows = [
        (
            "tt601",
            "Export Title 1",
            "Export Title 1",
            "movie",
            2021,
            None,
            9.0,
            5000,
            120,
            "Drama",
            0,
            0,
        ),
        (
            "tt602",
            "Export Title 2",
            "Export Title 2",
            "movie",
            2022,
            None,
            8.5,
            3000,
            100,
            "Action",
            0,
            0,
        ),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    recalculate_ranks(db_path=temp_db)

    export_dir = tmp_path / "data_export"
    export_dataset(db_path=temp_db, output_dir=export_dir, min_votes=1000, export_gzip=True)

    json_file = export_dir / "titles.json"
    gz_file = export_dir / "titles.json.gz"

    assert json_file.exists()
    assert gz_file.exists()

    # Verify JSON content
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["total"] == 2
    assert len(data["data"]) == 2

    # Verify Gzip content
    with gzip.open(gz_file, "rt", encoding="utf-8") as f:
        data_gz = json.load(f)
    assert data_gz == data


def test_local_file_ingestion(temp_db, tmp_path):
    """Verify that ingest engine can read from local .tsv.gz files and parses animation genre."""
    ratings_gz = tmp_path / "title.ratings.tsv.gz"
    ratings_content = "tconst\taverageRating\tnumVotes\ntt99901\t8.5\t5000\ntt99902\t4.0\t50\n"
    with gzip.open(ratings_gz, "wt", encoding="utf-8") as f:
        f.write(ratings_content)

    basics_gz = tmp_path / "title.basics.tsv.gz"
    basics_content = (
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
        "tt99901\tmovie\tTest Animation\tTest Animation\t0\t2023\t\\N\t120\tAnimation,Adventure\n"
    )
    with gzip.open(basics_gz, "wt", encoding="utf-8") as f:
        f.write(basics_content)

    qualifying = fetch_qualifying_ratings(source=str(ratings_gz))
    assert "tt99901" in qualifying
    assert "tt99902" not in qualifying

    inserted = stream_and_insert_basics(qualifying, source=str(basics_gz), db_path=temp_db)
    assert "tt99901" in inserted
    assert "tt99902" not in inserted
    recalculate_ranks(db_path=temp_db)

    with open_db(temp_db) as conn:
        row = conn.execute("SELECT * FROM titles WHERE imdb_id = 'tt99901'").fetchone()
        assert row is not None
        assert row["title"] == "Test Animation"
        assert row["is_animation"] == 1
        assert conn.execute("SELECT 1 FROM titles WHERE imdb_id = 'tt99902'").fetchone() is None


def test_basics_filters_year_adult_and_type(temp_db, tmp_path):
    """Reject pre-1900, adult, and disallowed title types even when votes qualify."""
    ratings_gz = tmp_path / "title.ratings.tsv.gz"
    ratings_content = (
        "tconst\taverageRating\tnumVotes\n"
        "ttkeep1\t8.5\t5000\n"
        "ttold01\t8.5\t5000\n"
        "ttadlt1\t8.5\t5000\n"
        "ttgame1\t8.5\t5000\n"
    )
    with gzip.open(ratings_gz, "wt", encoding="utf-8") as f:
        f.write(ratings_content)

    basics_gz = tmp_path / "title.basics.tsv.gz"
    basics_content = (
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
        "ttkeep1\tmovie\tKeeper\tKeeper\t0\t2020\t\\N\t100\tDrama\n"
        "ttold01\tmovie\tClassic\tClassic\t0\t1895\t\\N\t100\tDrama\n"
        "ttadlt1\tmovie\tAdult\tAdult\t1\t2020\t\\N\t100\tDrama\n"
        "ttgame1\tvideoGame\tGame\tGame\t0\t2020\t\\N\t100\tAction\n"
    )
    with gzip.open(basics_gz, "wt", encoding="utf-8") as f:
        f.write(basics_content)

    qualifying = fetch_qualifying_ratings(source=str(ratings_gz))
    assert set(qualifying) == {"ttkeep1", "ttold01", "ttadlt1", "ttgame1"}

    inserted = stream_and_insert_basics(qualifying, source=str(basics_gz), db_path=temp_db)
    assert inserted == {"ttkeep1"}

    with open_db(temp_db) as conn:
        ids = {row["imdb_id"] for row in conn.execute("SELECT imdb_id FROM titles")}
        assert ids == {"ttkeep1"}


def test_prune_removes_titles_that_no_longer_qualify(temp_db):
    """Titles missing from this week's keep-set must be deleted, not frozen."""
    stale = (
        "ttstale",
        "Dropped Title",
        "Dropped Title",
        "movie",
        2020,
        None,
        9.0,
        5000,
        100,
        "Drama",
        0,
        0,
    )
    keep = (
        "ttkeep1",
        "Still Good",
        "Still Good",
        "movie",
        2021,
        None,
        8.5,
        4000,
        110,
        "Action",
        0,
        0,
    )
    upsert_titles_batch([stale, keep], db_path=temp_db)

    deleted = prune_unqualified_titles({"ttkeep1"}, db_path=temp_db)
    assert deleted == 1

    with open_db(temp_db) as conn:
        ids = {row["imdb_id"] for row in conn.execute("SELECT imdb_id FROM titles")}
        assert ids == {"ttkeep1"}


def test_prune_empty_keep_set_is_a_noop(temp_db):
    """Refuse to wipe the database if this week's insert set is empty."""
    upsert_titles_batch(
        [("ttkeep1", "X", "X", "movie", 2020, None, 8.0, 2000, 90, "Drama", 0, 0)],
        db_path=temp_db,
    )
    assert prune_unqualified_titles(set(), db_path=temp_db) == 0
    with open_db(temp_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM titles").fetchone()[0] == 1


def test_reingest_drops_title_below_vote_threshold(temp_db, tmp_path):
    """A title that falls below min_votes must disappear on the next ingest."""
    upsert_titles_batch(
        [
            (
                "ttdrop1",
                "Was Popular",
                "Was Popular",
                "movie",
                2020,
                None,
                8.0,
                5000,
                100,
                "Drama",
                0,
                0,
            )
        ],
        db_path=temp_db,
    )

    ratings_gz = tmp_path / "title.ratings.tsv.gz"
    with gzip.open(ratings_gz, "wt", encoding="utf-8") as f:
        f.write("tconst\taverageRating\tnumVotes\nttstay1\t8.5\t5000\nttdrop1\t8.0\t50\n")

    basics_gz = tmp_path / "title.basics.tsv.gz"
    with gzip.open(basics_gz, "wt", encoding="utf-8") as f:
        f.write(
            "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear"
            "\truntimeMinutes\tgenres\n"
            "ttstay1\tmovie\tStayer\tStayer\t0\t2021\t\\N\t100\tDrama\n"
            "ttdrop1\tmovie\tWas Popular\tWas Popular\t0\t2020\t\\N\t100\tDrama\n"
        )

    episodes_gz = tmp_path / "title.episode.tsv.gz"
    with gzip.open(episodes_gz, "wt", encoding="utf-8") as f:
        f.write("tconst\tparentTconst\tseasonNumber\tepisodeNumber\n")

    run_ingestion(
        ratings_file=str(ratings_gz),
        basics_file=str(basics_gz),
        episodes_file=str(episodes_gz),
        db_path=temp_db,
    )

    with open_db(temp_db) as conn:
        ids = {row["imdb_id"] for row in conn.execute("SELECT imdb_id FROM titles")}
        assert ids == {"ttstay1"}


def test_init_db_rebuilds_rating_index_with_imdb_id(tmp_path):
    """Older DBs whose index omitted imdb_id must be rebuilt."""
    legacy_db = tmp_path / "legacy_idx.db"
    with open_db(legacy_db) as conn:
        conn.execute(
            """
            CREATE TABLE titles (
                imdb_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                title_type TEXT NOT NULL,
                rating REAL NOT NULL,
                vote_count INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX idx_titles_rating_votes ON titles(rating DESC, vote_count DESC)"
        )

    init_db(legacy_db)

    with open_db(legacy_db) as conn:
        idx_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'idx_titles_rating_votes'"
        ).fetchone()["sql"]
        assert "imdb_id" in idx_sql


def test_episode_and_crew_ingestion_without_names(temp_db, tmp_path):
    """Verify streaming ingestion for title.episode and title.crew without name.basics."""
    sample_rows = [
        (
            "tt5001",
            "Ozymandias",
            "Ozymandias",
            "tv_episode",
            2013,
            None,
            10.0,
            200000,
            48,
            "Crime, Drama",
            0,
            0,
        ),
        (
            "tt5002",
            "Inception",
            "Inception",
            "movie",
            2010,
            None,
            8.8,
            2500000,
            148,
            "Action, Sci-Fi",
            0,
            0,
        ),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    qualifying = {"tt5001", "tt5002"}

    episodes_gz = tmp_path / "title.episode.tsv.gz"
    episodes_content = (
        "tconst\tparentTconst\tseasonNumber\tepisodeNumber\ntt5001\ttt0903747\t5\t14\n"
    )
    with gzip.open(episodes_gz, "wt", encoding="utf-8") as f:
        f.write(episodes_content)

    stream_and_update_episodes(qualifying, source=str(episodes_gz), db_path=temp_db)

    with open_db(temp_db) as conn:
        row_ep = conn.execute("SELECT * FROM titles WHERE imdb_id = 'tt5001'").fetchone()
        assert row_ep["parent_id"] == "tt0903747"
        assert row_ep["season_number"] == 5
        assert row_ep["episode_number"] == 14

        row_mov = conn.execute("SELECT * FROM titles WHERE imdb_id = 'tt5002'").fetchone()
        assert row_mov["parent_id"] is None


def test_network_failures_and_timeouts():
    """Verify that network 429s, 500s, timeouts, and malformed JSON fail safely."""
    mock_client_429 = MagicMock()
    mock_resp_429 = MagicMock()
    mock_resp_429.status_code = 429
    mock_client_429.get.return_value = mock_resp_429

    res = fetch_single_title_metadata(mock_client_429, "tt001")
    assert res == ("tt001", None, None, None, False)

    mock_client_timeout = MagicMock()
    mock_client_timeout.get.side_effect = TimeoutError("Connection timed out")

    res = fetch_single_title_metadata(mock_client_timeout, "tt002")
    assert res == ("tt002", None, None, None, False)

    mock_client_corrupt = MagicMock()
    mock_resp_corrupt = MagicMock()
    mock_resp_corrupt.status_code = 200
    mock_resp_corrupt.content = b"Invalid JSON"
    mock_client_corrupt.get.return_value = mock_resp_corrupt

    res = fetch_single_title_metadata(mock_client_corrupt, "tt003")
    assert res == ("tt003", None, None, None, False)


def test_enrich_infinite_loop_prevention(temp_db, monkeypatch):
    """Verify that enrich_missing_posters terminates safely even when all fetches fail."""
    sample_rows = [
        (
            "tt701",
            "Fail Title 1",
            "Fail Title 1",
            "movie",
            2020,
            None,
            9.0,
            1000,
            100,
            "Drama",
            0,
            0,
        ),
        (
            "tt702",
            "Fail Title 2",
            "Fail Title 2",
            "movie",
            2020,
            None,
            8.5,
            1000,
            100,
            "Drama",
            0,
            0,
        ),
    ]
    upsert_titles_batch(sample_rows, db_path=temp_db)
    recalculate_ranks(db_path=temp_db)

    def mock_failing_fetch(session, imdb_id, timeout=5):
        # Simulate network failure
        return (imdb_id, None, None, None, False)

    monkeypatch.setattr("enrich.fetch_single_title_metadata", mock_failing_fetch)

    # Should run and terminate cleanly without infinite looping
    enrich_missing_posters(limit=10, all_titles=True, max_workers=2, db_path=temp_db)

    with open_db(temp_db) as conn:
        needing = conn.execute("SELECT COUNT(*) FROM titles WHERE enriched_at IS NULL").fetchone()[
            0
        ]
        assert needing == 2  # Remains unenriched, but didn't hang


def test_freshness_checker(tmp_path, monkeypatch):
    """Verify that is_dataset_stale detects stale vs fresh JSON files."""
    from check_freshness import is_dataset_stale

    test_data_dir = tmp_path / "data"
    test_data_dir.mkdir(parents=True)
    monkeypatch.setattr("check_freshness.DATA_DIR", test_data_dir)

    # Case 1: Missing file -> Stale
    assert is_dataset_stale(threshold_days=5.0) is True

    now = datetime.now(timezone.utc)

    # Case 2: Fresh file (1 day old) -> Not stale
    fresh_payload = {
        "stats": {"last_updated": (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")}
    }
    with open(test_data_dir / "titles.json", "w") as f:
        json.dump(fresh_payload, f)
    assert is_dataset_stale(threshold_days=5.0) is False

    # Case 3: Stale file (8 days old) -> Stale
    stale_payload = {
        "stats": {"last_updated": (now - timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S")}
    }
    with open(test_data_dir / "titles.json", "w") as f:
        json.dump(stale_payload, f)
    assert is_dataset_stale(threshold_days=5.0) is True
