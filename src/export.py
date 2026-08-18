"""
Export Engine: Generates compact columnar JSON datasets (Votes >= 1,000) from SQLite.
"""

import argparse
import gzip
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional, Union
import sqlite3
import msgspec

try:
    from config import DATA_DIR, DB_PATH, EXPORT_CONFIG
    from db import open_db, init_db
except ImportError:
    from .config import DATA_DIR, DB_PATH, EXPORT_CONFIG
    from .db import open_db, init_db


def build_tier_payload(conn: sqlite3.Connection, min_votes: int) -> Dict[str, Any]:
    """Queries SQLite for titles with vote_count >= min_votes and returns payload with continuous tier ranks."""
    # 1. Stats for this specific tier
    stats_row = conn.execute(
        """
        SELECT
            COUNT(*) as total_titles,
            SUM(CASE WHEN title_type = 'movie' THEN 1 ELSE 0 END) as total_movies,
            SUM(CASE WHEN title_type IN ('tv_series', 'tv_miniseries') THEN 1 ELSE 0 END) as total_tv,
            SUM(CASE WHEN title_type = 'tv_episode' THEN 1 ELSE 0 END) as total_episodes,
            SUM(CASE WHEN is_animation = 1 THEN 1 ELSE 0 END) as total_animation,
            AVG(rating) as avg_rating,
            MAX(updated_at) as last_updated
        FROM titles
        WHERE vote_count >= ?;
    """,
        (min_votes,),
    ).fetchone()

    stats = {
        "total_titles": stats_row["total_titles"] or 0,
        "total_movies": stats_row["total_movies"] or 0,
        "total_tv": stats_row["total_tv"] or 0,
        "total_episodes": stats_row["total_episodes"] or 0,
        "total_animation": stats_row["total_animation"] or 0,
        "avg_rating": round(stats_row["avg_rating"] or 0.0, 2),
        "min_votes": min_votes,
        "last_updated": stats_row["last_updated"],
    }

    # 2. Query all titles in this tier ordered by rating DESC, vote_count DESC, imdb_id ASC
    cursor = conn.execute(
        """
        SELECT
            imdb_id, title, original_title, title_type,
            year, end_year, rating, vote_count, runtime_minutes,
            genres, is_adult, is_animation, poster_url, cast_members,
            popularity_rank, parent_id, season_number, episode_number
        FROM titles
        WHERE vote_count >= ?
        ORDER BY rating DESC, vote_count DESC, imdb_id ASC;
    """,
        (min_votes,),
    )
    rows = cursor.fetchall()

    fields = [
        "id",
        "title",
        "original_title",
        "type",
        "year",
        "end_year",
        "rating",
        "votes",
        "runtime",
        "genres",
        "is_adult",
        "is_animation",
        "poster",
        "cast",
        "popularity",
        "rank",
        "parent_id",
        "season",
        "episode",
    ]

    data = [
        [
            r["imdb_id"],
            r["title"],
            r["original_title"],
            r["title_type"],
            r["year"],
            r["end_year"],
            round(r["rating"], 1),
            r["vote_count"],
            r["runtime_minutes"],
            r["genres"],
            r["is_adult"],
            r["is_animation"],
            r["poster_url"],
            r["cast_members"],
            r["popularity_rank"],
            idx + 1,  # Continuous 1..N tier ranking for seamless frontend filtering
            r["parent_id"],
            r["season_number"],
            r["episode_number"],
        ]
        for idx, r in enumerate(rows)
    ]

    return {"stats": stats, "fields": fields, "total": len(data), "data": data}


def export_dataset(
    db_path: Union[str, Path] = DB_PATH,
    output_dir: Union[str, Path] = DATA_DIR,
    min_votes: Optional[int] = None,
    export_gzip: bool = True,
):
    """Generates the single compact data/titles.json (Votes >= 1,000) and titles.json.gz atomically."""
    init_db(db_path=db_path)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    with open_db(db_path) as conn:
        min_v = min_votes if min_votes is not None else EXPORT_CONFIG["min_votes"]
        output_file = target_dir / EXPORT_CONFIG["filename"]
        tmp_file = target_dir / f"{EXPORT_CONFIG['filename']}.tmp"

        print(f"[*] Exporting IMDb Universe (Votes >= {min_v}) to {output_file.name}...")
        payload = build_tier_payload(conn, min_votes=min_v)

        # Fast native C-level JSON serialization with msgspec (< 45ms)
        encoded_bytes = msgspec.json.encode(payload)

        # 1. Atomic write for uncompressed JSON
        tmp_file.write_bytes(encoded_bytes)
        os.replace(tmp_file, output_file)

        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        print(
            f"    -> Generated {output_file.name}: {payload['total']:,} titles ({file_size_mb:.2f} MB)"
        )

        # 2. Atomic write for pre-compressed gzip JSON asset
        if export_gzip:
            output_gz = target_dir / f"{EXPORT_CONFIG['filename']}.gz"
            tmp_gz = target_dir / f"{EXPORT_CONFIG['filename']}.gz.tmp"
            tmp_gz.write_bytes(gzip.compress(encoded_bytes, compresslevel=6))
            os.replace(tmp_gz, output_gz)
            gz_size_mb = output_gz.stat().st_size / (1024 * 1024)
            print(
                f"    -> Generated {output_gz.name}: ({gz_size_mb:.2f} MB - {100 * (1 - gz_size_mb / file_size_mb):.1f}% reduction)"
            )

        # 3. Remove any legacy / duplicate JSON files
        for legacy_name in ["titles_1k.json", "titles_100.json"]:
            legacy_file = target_dir / legacy_name
            if legacy_file.exists():
                legacy_file.unlink()
                print(f"    -> Removed duplicate {legacy_file.name}")

    print(f"\n[✅] Export complete in {time.time() - t0:.2f}s!")




def main():
    parser = argparse.ArgumentParser(
        description="Export IMDb titles dataset to compact columnar JSON."
    )
    parser.add_argument("--db-path", type=str, default=str(DB_PATH), help="Path to SQLite database")
    parser.add_argument(
        "--output-dir", type=str, default=str(DATA_DIR), help="Output directory for JSON files"
    )
    parser.add_argument(
        "--min-votes", type=int, default=None, help="Override minimum vote threshold"
    )
    parser.add_argument(
        "--no-gzip", action="store_true", help="Skip generating pre-compressed .gz file"
    )
    args = parser.parse_args()

    export_dataset(
        db_path=args.db_path,
        output_dir=args.output_dir,
        min_votes=args.min_votes,
        export_gzip=not args.no_gzip,
    )


if __name__ == "__main__":
    main()
