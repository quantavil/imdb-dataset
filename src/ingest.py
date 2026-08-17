"""
Ingestion Engine: Streams and filters official IMDb datasets directly into SQLite.
Supports both direct zero-disk HTTP streaming and local .tsv.gz cached file ingestion.
"""

import argparse
import time
import gzip
import csv
import io
import os
from pathlib import Path
import urllib.request
from typing import Dict, Tuple, Optional, Union, Any, Set

try:
    from config import URLS, FILTER_CONFIG, DB_PATH
    from db import (
        init_db,
        upsert_titles_batch,
        prune_unqualified_titles,
        recalculate_ranks,
        update_crew_batch,
        update_episodes_batch,
    )
except ImportError:
    from .config import URLS, FILTER_CONFIG, DB_PATH
    from .db import (
        init_db,
        upsert_titles_batch,
        prune_unqualified_titles,
        recalculate_ranks,
        update_crew_batch,
        update_episodes_batch,
    )


def open_dataset_stream(url_or_path: str, max_retries: int = 3, retry_delay: float = 2.0):
    """
    Opens a binary stream from either a local file path or a remote HTTP URL with retries.
    """
    if os.path.exists(url_or_path):
        print(f"[*] Reading from local file: {url_or_path}")
        return open(url_or_path, "rb")

    print(f"[*] Streaming from remote URL: {url_or_path}")
    req = urllib.request.Request(
        url_or_path, headers={"User-Agent": "IMDb-Leaderboard-Pipeline/1.0"}
    )
    for attempt in range(1, max_retries + 1):
        try:
            return urllib.request.urlopen(req, timeout=120)
        except Exception as e:
            if attempt == max_retries:
                raise
            print(
                f"[!] Stream attempt {attempt}/{max_retries} failed ({e}). Retrying in {retry_delay}s..."
            )
            time.sleep(retry_delay)


def fetch_qualifying_ratings(source: Optional[str] = None) -> Dict[str, Tuple[float, int]]:
    """
    Streams title.ratings.tsv.gz and extracts qualifying title IDs.
    Returns: { 'tt1234567': (rating, vote_count) }
    """
    url_or_path = source or URLS["ratings"]
    t0 = time.time()
    qualifying = {}

    min_v = FILTER_CONFIG["min_votes"]
    min_r = FILTER_CONFIG["min_rating"]
    max_r = FILTER_CONFIG["max_rating"]

    with open_dataset_stream(url_or_path) as resp:
        with gzip.GzipFile(fileobj=resp) as gz:
            reader = csv.reader(io.TextIOWrapper(gz, encoding="utf-8"), delimiter="\t")
            _header = next(reader, None)  # tconst, averageRating, numVotes

            for row in reader:
                if len(row) == 3:
                    try:
                        rating = float(row[1])
                        votes = int(row[2])
                        if min_r <= rating <= max_r and votes >= min_v:
                            qualifying[row[0]] = (rating, votes)
                    except ValueError:
                        continue

    print(f"[+] Found {len(qualifying):,} qualifying rated titles in {time.time() - t0:.2f}s")
    return qualifying


def stream_and_insert_basics(
    qualifying_ratings: Dict[str, Tuple[float, int]],
    source: Optional[str] = None,
    batch_size: int = 2500,
    db_path: Union[str, Path] = DB_PATH,
) -> Set[str]:
    """
    Streams title.basics.tsv.gz (from URL or local file), matches against qualifying IDs,
    filters by year/type, and inserts records into SQLite in batches.
    Returns the set of inserted tconst IDs.
    """
    url_or_path = source or URLS["basics"]
    t0 = time.time()

    allowed_types = FILTER_CONFIG["allowed_types"]
    min_year = FILTER_CONFIG["min_year"]
    include_adult = FILTER_CONFIG["include_adult"]

    batch = []
    inserted_ids = set()

    with open_dataset_stream(url_or_path) as resp:
        with gzip.GzipFile(fileobj=resp) as gz:
            reader = csv.reader(io.TextIOWrapper(gz, encoding="utf-8"), delimiter="\t")
            _header = next(reader, None)
            # tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres

            for row in reader:
                if len(row) < 9:
                    continue

                tconst = row[0]
                if tconst not in qualifying_ratings:
                    continue

                raw_type = row[1]
                if raw_type not in allowed_types:
                    continue

                is_adult = 1 if row[4] == "1" else 0
                if is_adult and not include_adult:
                    continue

                try:
                    start_year = int(row[5]) if row[5] != "\\N" else None
                except ValueError:
                    start_year = None

                if start_year is not None and start_year < min_year:
                    continue

                try:
                    end_year = int(row[6]) if row[6] != "\\N" else None
                except ValueError:
                    end_year = None

                try:
                    runtime_mins = int(row[7]) if row[7] != "\\N" else None
                except ValueError:
                    runtime_mins = None

                primary_title = row[2]
                original_title = row[3] if row[3] != "\\N" else primary_title
                genres = row[8].replace("\\N", "").replace(",", ", ")

                # Check animation flag (matches any animated production)
                is_animation = 1 if "Animation" in genres else 0

                rating, votes = qualifying_ratings[tconst]
                mapped_type = allowed_types[raw_type]

                batch.append(
                    (
                        tconst,
                        primary_title,
                        original_title,
                        mapped_type,
                        start_year,
                        end_year,
                        rating,
                        votes,
                        runtime_mins,
                        genres,
                        is_adult,
                        is_animation,
                    )
                )
                inserted_ids.add(tconst)

                if len(batch) >= batch_size:
                    upsert_titles_batch(batch, db_path=db_path)
                    batch.clear()

            if batch:
                upsert_titles_batch(batch, db_path=db_path)
                batch.clear()

    print(
        f"[+] Successfully inserted {len(inserted_ids):,} titles into SQLite in {time.time() - t0:.2f}s"
    )
    return inserted_ids


def stream_and_update_episodes(
    qualifying_ids: Union[Dict[str, Any], Set[str]],
    source: Optional[str] = None,
    batch_size: int = 2500,
    db_path: Union[str, Path] = DB_PATH,
):
    """
    Streams title.episode.tsv.gz, matches against qualifying IDs,
    and updates parent_id, season_number, and episode_number.
    """
    url_or_path = source or URLS["episodes"]
    t0 = time.time()
    batch = []
    updated_count = 0

    with open_dataset_stream(url_or_path) as resp:
        with gzip.GzipFile(fileobj=resp) as gz:
            reader = csv.reader(io.TextIOWrapper(gz, encoding="utf-8"), delimiter="\t")
            _header = next(reader, None)  # tconst, parentTconst, seasonNumber, episodeNumber

            for row in reader:
                if len(row) < 4:
                    continue
                tconst = row[0]
                if tconst not in qualifying_ids:
                    continue

                parent_id = row[1] if row[1] != "\\N" else None
                try:
                    season_num = int(row[2]) if row[2] != "\\N" else None
                except ValueError:
                    season_num = None

                try:
                    ep_num = int(row[3]) if row[3] != "\\N" else None
                except ValueError:
                    ep_num = None

                batch.append((tconst, parent_id, season_num, ep_num))

                if len(batch) >= batch_size:
                    update_episodes_batch(batch, db_path=db_path)
                    updated_count += len(batch)
                    batch.clear()

            if batch:
                update_episodes_batch(batch, db_path=db_path)
                updated_count += len(batch)
                batch.clear()

    print(
        f"[+] Successfully updated {updated_count:,} TV episode mappings in {time.time() - t0:.2f}s"
    )


def stream_and_update_crew(
    qualifying_ids: Union[Dict[str, Any], Set[str]],
    crew_source: Optional[str] = None,
    batch_size: int = 2500,
    db_path: Union[str, Path] = DB_PATH,
):
    """
    Streams title.crew.tsv.gz, extracts director & writer IDs for qualifying titles,
    and updates titles in SQLite.
    """
    crew_url_or_path = crew_source or URLS["crew"]
    t0 = time.time()
    batch = []
    updated_count = 0

    with open_dataset_stream(crew_url_or_path) as resp:
        with gzip.GzipFile(fileobj=resp) as gz:
            reader = csv.reader(io.TextIOWrapper(gz, encoding="utf-8"), delimiter="\t")
            _header = next(reader, None)  # tconst, directors, writers

            for row in reader:
                if len(row) < 3:
                    continue
                tconst = row[0]
                if tconst not in qualifying_ids:
                    continue

                directors_raw = (
                    row[1].replace(",", ", ") if row[1] and row[1] != "\\N" else None
                )
                writers_raw = (
                    row[2].replace(",", ", ") if row[2] and row[2] != "\\N" else None
                )

                batch.append((tconst, directors_raw, writers_raw))
                if len(batch) >= batch_size:
                    update_crew_batch(batch, db_path=db_path)
                    updated_count += len(batch)
                    batch.clear()

            if batch:
                update_crew_batch(batch, db_path=db_path)
                updated_count += len(batch)
                batch.clear()

    print(
        f"[+] Successfully updated crew info for {updated_count:,} titles in {time.time() - t0:.2f}s"
    )


def run_ingestion(
    ratings_file: Optional[str] = None,
    basics_file: Optional[str] = None,
    crew_file: Optional[str] = None,
    episodes_file: Optional[str] = None,
    db_path: Union[str, Path] = DB_PATH,
):
    print("=" * 60)
    print("STARTING IMDB INGESTION PIPELINE")
    print("=" * 60)
    start_time = time.time()

    init_db(db_path=db_path)
    qualifying = fetch_qualifying_ratings(source=ratings_file)
    if not qualifying:
        print("[!] No qualifying titles found with current filters.")
        return

    inserted_ids = stream_and_insert_basics(qualifying, source=basics_file, db_path=db_path)
    if inserted_ids:
        pruned = prune_unqualified_titles(inserted_ids, db_path=db_path)
        if pruned:
            print(f"[*] Removed {pruned:,} titles that no longer meet filters")
    stream_and_update_episodes(inserted_ids, source=episodes_file, db_path=db_path)
    stream_and_update_crew(inserted_ids, crew_source=crew_file, db_path=db_path)

    print("[*] Calculating overall leaderboard ranking...")
    recalculate_ranks(db_path=db_path)

    total_time = time.time() - start_time
    print(f"[✅] INGESTION COMPLETE in {total_time:.2f} seconds!")


def main():
    parser = argparse.ArgumentParser(description="Ingest IMDb daily datasets into SQLite.")
    parser.add_argument(
        "--ratings-file", type=str, default=None, help="Optional path to local title.ratings.tsv.gz"
    )
    parser.add_argument(
        "--basics-file", type=str, default=None, help="Optional path to local title.basics.tsv.gz"
    )
    parser.add_argument(
        "--crew-file", type=str, default=None, help="Optional path to local title.crew.tsv.gz"
    )
    parser.add_argument(
        "--episodes-file",
        type=str,
        default=None,
        help="Optional path to local title.episode.tsv.gz",
    )
    parser.add_argument("--db-path", type=str, default=str(DB_PATH), help="Path to SQLite database")
    args = parser.parse_args()

    run_ingestion(
        ratings_file=args.ratings_file,
        basics_file=args.basics_file,
        crew_file=args.crew_file,
        episodes_file=args.episodes_file,
        db_path=args.db_path,
    )


if __name__ == "__main__":
    main()
