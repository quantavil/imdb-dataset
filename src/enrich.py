"""
Poster, Cast & Popularity Enrichment Engine: Uses IMDb Suggestion CDN to enrich titles.
"""

import argparse
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Tuple, List, Union, Set
import httpx
import msgspec

try:
    from config import URLS, ENRICH_CONFIG, DB_PATH
    from db import get_titles_needing_enrichment, update_enrichment_batch, init_db
except ImportError:
    from .config import URLS, ENRICH_CONFIG, DB_PATH
    from .db import get_titles_needing_enrichment, update_enrichment_batch, init_db

logger = logging.getLogger(__name__)


def parse_suggestion_item(
    imdb_id: str, items: list
) -> Tuple[str, Optional[str], Optional[str], Optional[int]]:
    """
    Safely parses items from IMDb Suggestion CDN JSON payload.
    Returns: (imdb_id, poster_url, cast_members, popularity_rank)
    """
    if not isinstance(items, list):
        return (imdb_id, None, None, None)

    for item in items:
        if isinstance(item, dict) and item.get("id") == imdb_id:
            # Safe image parsing without crashing on NoneType
            image_obj = item.get("i")
            poster_url = image_obj.get("imageUrl") if isinstance(image_obj, dict) else None
            cast_members = item.get("s")
            pop_rank = item.get("rank")
            return (imdb_id, poster_url, cast_members, pop_rank)

    return (imdb_id, None, None, None)


def create_http_client(max_workers: int) -> httpx.Client:
    """Creates a configured httpx.Client with connection pooling, HTTP/2, and retries."""
    limits = httpx.Limits(
        max_connections=max_workers * 2,
        max_keepalive_connections=max_workers,
        keepalive_expiry=30.0,
    )
    transport = httpx.HTTPTransport(retries=2, limits=limits, http2=True)
    return httpx.Client(
        transport=transport,
        headers={"User-Agent": "IMDb-Leaderboard-Pipeline/1.0"},
        timeout=httpx.Timeout(float(ENRICH_CONFIG["timeout_sec"])),
    )




def fetch_single_title_metadata(
    client: httpx.Client, imdb_id: str, timeout: Optional[float] = None
) -> Tuple[str, Optional[str], Optional[str], Optional[int], bool]:
    """
    Fetches poster URL, cast list, and MOVIEMETER popularity rank from IMDb Suggestion CDN.
    Returns: (imdb_id, poster_url, cast_members, popularity_rank, is_success)
    """
    url = URLS["suggestion_base"].format(id=imdb_id)
    t = timeout if timeout is not None else float(ENRICH_CONFIG["timeout_sec"])
    try:
        resp = client.get(url, timeout=t)
        if resp.status_code == 200:
            data = msgspec.json.decode(resp.content)
            items = data.get("d", []) if isinstance(data, dict) else []
            imdb_id, poster, cast, pop = parse_suggestion_item(imdb_id, items)
            return (imdb_id, poster, cast, pop, True)
        elif resp.status_code == 404:
            return (imdb_id, None, None, None, True)
        elif resp.status_code == 429:
            logger.warning(f"Rate limited (HTTP 429) for {imdb_id}")
            time.sleep(0.5)
        else:
            logger.warning(f"Unexpected HTTP {resp.status_code} for {imdb_id}")
    except Exception as e:
        logger.debug(f"Fetch failed for {imdb_id}: {e}")
    return (imdb_id, None, None, None, False)


def enrich_missing_posters(
    limit: int = 2000,
    all_titles: bool = False,
    max_workers: int = ENRICH_CONFIG["max_workers"],
    batch_size: int = ENRICH_CONFIG["batch_size"],
    db_path: Union[str, Path] = DB_PATH,
):
    """Enriches titles currently missing poster, cast, or popularity rank in SQLite."""
    init_db(db_path=db_path)

    client = create_http_client(max_workers)
    total_processed = 0
    total_with_meta = 0
    failed_ids: Set[str] = set()
    t0 = time.time()

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                fetch_chunk = min(limit - total_processed, 2000) if not all_titles else 2000
                if not all_titles and fetch_chunk <= 0:
                    break

                titles = get_titles_needing_enrichment(
                    limit=fetch_chunk, exclude_ids=failed_ids if failed_ids else None, db_path=db_path
                )
                if not titles:
                    if total_processed == 0 and not failed_ids:
                        print(
                            "[+] All qualifying titles are already enriched or fresh within 7-day TTL!"
                        )
                    break

                print(f"[*] Enriching batch of {len(titles)} titles (Workers: {max_workers})...")
                batch_enriched = 0
                pending_updates: List[Tuple[str, Optional[str], Optional[str], Optional[int]]] = []

                futures = {
                    executor.submit(fetch_single_title_metadata, client, row["imdb_id"]): row[
                        "imdb_id"
                    ]
                    for row in titles
                }

                for future in as_completed(futures):
                    imdb_id, poster_url, cast_members, pop_rank, is_success = future.result()
                    if is_success:
                        if poster_url or cast_members or pop_rank:
                            batch_enriched += 1
                        pending_updates.append((imdb_id, poster_url, cast_members, pop_rank))
                        if len(pending_updates) >= batch_size:
                            update_enrichment_batch(pending_updates, db_path=db_path)
                            pending_updates.clear()
                    else:
                        failed_ids.add(imdb_id)

                if pending_updates:
                    update_enrichment_batch(pending_updates, db_path=db_path)
                    pending_updates.clear()

                total_processed += len(titles)
                total_with_meta += batch_enriched
                print(
                    f"    -> Batch complete: {len(titles)} processed ({batch_enriched} with metadata). Total: {total_processed}"
                )

                if not all_titles and total_processed >= limit:
                    break

                # Polite pacing delay between batches
                time.sleep(0.05)
    finally:
        client.close()

    print(
        f"[✅] Finished processing {total_processed} titles ({total_with_meta} with CDN metadata) in {time.time() - t0:.2f} seconds!"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Enrich IMDb titles with posters, cast, and MOVIEMETER popularity."
    )
    parser.add_argument(
        "--limit", type=int, default=2000, help="Maximum number of titles to enrich in this run"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Continuously enrich all remaining titles until queue is empty",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=ENRICH_CONFIG["max_workers"],
        help="Number of concurrent workers",
    )
    parser.add_argument("--db-path", type=str, default=str(DB_PATH), help="Path to SQLite database")
    args = parser.parse_args()

    enrich_missing_posters(
        limit=args.limit,
        all_titles=args.all,
        max_workers=args.workers,
        db_path=args.db_path,
    )


if __name__ == "__main__":
    main()
