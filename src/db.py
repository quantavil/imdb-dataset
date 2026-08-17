"""
Database management and query operations for IMDb Leaderboard.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import List, Tuple, Optional, Union, Generator, Set

try:
    from config import DB_PATH
except ImportError:
    from .config import DB_PATH


def get_connection(db_path: Union[str, Path] = DB_PATH) -> sqlite3.Connection:
    """Returns a SQLite connection with WAL mode, busy timeout, and row factory."""
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


@contextmanager
def open_db(db_path: Union[str, Path] = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    """Context manager that commits on success, rolls back on error, and always closes the connection."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Union[str, Path] = DB_PATH):
    """Initializes tables, columns, migrations, and indexes."""
    with open_db(db_path) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS titles (
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
            is_adult INTEGER DEFAULT 0,
            is_animation INTEGER DEFAULT 0,
            poster_url TEXT,
            cast_members TEXT,
            directors TEXT,
            writers TEXT,
            parent_id TEXT,
            season_number INTEGER,
            episode_number INTEGER,
            popularity_rank INTEGER,
            rank INTEGER,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            enriched_at DATETIME
        );
        """)

        # 1. Dynamic schema migration for older databases (must run before index creation)
        existing_cols = {
            row["name"] for row in conn.execute("PRAGMA table_info(titles);").fetchall()
        }

        # Migrate legacy is_anime column to is_animation if present
        if "is_animation" not in existing_cols:
            if "is_anime" in existing_cols:
                conn.execute("ALTER TABLE titles RENAME COLUMN is_anime TO is_animation;")
            else:
                conn.execute("ALTER TABLE titles ADD COLUMN is_animation INTEGER DEFAULT 0;")

        migrations = [
            ("popularity_rank", "ALTER TABLE titles ADD COLUMN popularity_rank INTEGER;"),
            ("rank", "ALTER TABLE titles ADD COLUMN rank INTEGER;"),
            ("is_adult", "ALTER TABLE titles ADD COLUMN is_adult INTEGER DEFAULT 0;"),
            ("enriched_at", "ALTER TABLE titles ADD COLUMN enriched_at DATETIME;"),
            ("directors", "ALTER TABLE titles ADD COLUMN directors TEXT;"),
            ("writers", "ALTER TABLE titles ADD COLUMN writers TEXT;"),
            ("parent_id", "ALTER TABLE titles ADD COLUMN parent_id TEXT;"),
            ("season_number", "ALTER TABLE titles ADD COLUMN season_number INTEGER;"),
            ("episode_number", "ALTER TABLE titles ADD COLUMN episode_number INTEGER;"),
        ]
        for col_name, alter_sql in migrations:
            if col_name not in existing_cols:
                conn.execute(alter_sql)

        # 2. Indexes creation (after all columns are guaranteed to exist)
        conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_titles_rank ON titles(rank ASC);
        DROP INDEX IF EXISTS idx_titles_rating_votes;
        CREATE INDEX idx_titles_rating_votes ON titles(rating DESC, vote_count DESC, imdb_id ASC);
        CREATE INDEX IF NOT EXISTS idx_titles_enriched_rank ON titles(enriched_at, rank);
        CREATE INDEX IF NOT EXISTS idx_titles_parent ON titles(parent_id) WHERE parent_id IS NOT NULL;
        DROP INDEX IF EXISTS idx_titles_unenriched;
        """)


def upsert_titles_batch(rows: List[Tuple], db_path: Union[str, Path] = DB_PATH):
    """
    Upserts a batch of title records.
    Tuple structure:
    (imdb_id, title, original_title, title_type, year, end_year, rating, vote_count, runtime_minutes, genres, is_adult, is_animation)
    """
    with open_db(db_path) as conn:
        conn.executemany(
            """
        INSERT INTO titles (
            imdb_id, title, original_title, title_type, year, end_year,
            rating, vote_count, runtime_minutes, genres, is_adult, is_animation, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(imdb_id) DO UPDATE SET
            title = excluded.title,
            original_title = excluded.original_title,
            title_type = excluded.title_type,
            year = excluded.year,
            end_year = excluded.end_year,
            rating = excluded.rating,
            vote_count = excluded.vote_count,
            runtime_minutes = excluded.runtime_minutes,
            genres = excluded.genres,
            is_adult = excluded.is_adult,
            is_animation = excluded.is_animation,
            updated_at = CURRENT_TIMESTAMP;
        """,
            rows,
        )


def prune_unqualified_titles(
    keep_ids: Set[str], db_path: Union[str, Path] = DB_PATH
) -> int:
    """Delete titles whose imdb_id is not in keep_ids. Empty keep_ids is a no-op."""
    if not keep_ids:
        return 0
    with open_db(db_path) as conn:
        conn.execute("CREATE TEMP TABLE keep_ids (imdb_id TEXT PRIMARY KEY)")
        conn.executemany(
            "INSERT OR IGNORE INTO keep_ids (imdb_id) VALUES (?)",
            [(imdb_id,) for imdb_id in keep_ids],
        )
        cursor = conn.execute(
            "DELETE FROM titles WHERE imdb_id NOT IN (SELECT imdb_id FROM keep_ids)"
        )
        deleted = cursor.rowcount
        conn.execute("DROP TABLE keep_ids")
        return deleted


def recalculate_ranks(db_path: Union[str, Path] = DB_PATH):
    """Calculates overall leaderboard ranking based on rating, vote count, and deterministic imdb_id."""
    with open_db(db_path) as conn:
        conn.execute("""
        WITH ranked AS (
            SELECT imdb_id, ROW_NUMBER() OVER (ORDER BY rating DESC, vote_count DESC, imdb_id ASC) as new_rank
            FROM titles
        )
        UPDATE titles
        SET rank = ranked.new_rank
        FROM ranked
        WHERE titles.imdb_id = ranked.imdb_id;
        """)


def update_enrichment_batch(
    updates: List[Tuple[str, Optional[str], Optional[str], Optional[int]]],
    db_path: Union[str, Path] = DB_PATH,
):
    """
    Batched update of poster, cast, popularity rank, and enriched_at timestamp.
    Tuple structure: (imdb_id, poster_url, cast_members, popularity_rank)
    """
    if not updates:
        return

    reordered = [
        (poster_url, cast_members, popularity_rank, imdb_id)
        for imdb_id, poster_url, cast_members, popularity_rank in updates
    ]
    with open_db(db_path) as conn:
        conn.executemany(
            """
        UPDATE titles
        SET poster_url = COALESCE(?, poster_url),
            cast_members = COALESCE(?, cast_members),
            popularity_rank = ?,
            enriched_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE imdb_id = ?;
        """,
            reordered,
        )


def get_titles_needing_enrichment(
    limit: int = 2000,
    exclude_ids: Optional[Union[set, list]] = None,
    db_path: Union[str, Path] = DB_PATH,
) -> List[sqlite3.Row]:
    """Fetches titles where enriched_at IS NULL or older than 7 days TTL, ordered by leaderboard rank."""
    exclude_set = set(exclude_ids) if exclude_ids else None
    with open_db(db_path) as conn:
        # Fetch candidate rows and filter in Python to avoid SQLite parameter limits on large exclude_ids
        fetch_limit = limit + (len(exclude_set) if exclude_set else 0)
        cursor = conn.execute(
            """
            SELECT imdb_id
            FROM titles
            WHERE enriched_at IS NULL OR enriched_at < datetime('now', '-7 days')
            ORDER BY rank ASC
            LIMIT ?;
            """,
            (fetch_limit,),
        )
        if not exclude_set:
            return cursor.fetchall()

        results = []
        for row in cursor:
            if row["imdb_id"] not in exclude_set:
                results.append(row)
                if len(results) >= limit:
                    break
        return results


def update_crew_batch(
    updates: List[Tuple[str, Optional[str], Optional[str]]], db_path: Union[str, Path] = DB_PATH
):
    """
    Batched update of directors and writers.
    Tuple structure: (imdb_id, directors, writers)
    """
    if not updates:
        return
    reordered = [(directors, writers, imdb_id) for imdb_id, directors, writers in updates]
    with open_db(db_path) as conn:
        conn.executemany(
            """
        UPDATE titles
        SET directors = ?,
            writers = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE imdb_id = ?;
        """,
            reordered,
        )


def update_episodes_batch(
    updates: List[Tuple[str, Optional[str], Optional[int], Optional[int]]],
    db_path: Union[str, Path] = DB_PATH,
):
    """
    Batched update of parent TV series ID, season number, and episode number.
    Tuple structure: (imdb_id, parent_id, season_number, episode_number)
    """
    if not updates:
        return
    reordered = [
        (parent_id, season_number, episode_number, imdb_id)
        for imdb_id, parent_id, season_number, episode_number in updates
    ]
    with open_db(db_path) as conn:
        conn.executemany(
            """
        UPDATE titles
        SET parent_id = ?,
            season_number = ?,
            episode_number = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE imdb_id = ?;
        """,
            reordered,
        )
