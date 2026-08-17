# AGENT.md

## Critical Blunders & Learnings
- **IMDb Search Scraping (AWS WAF)**: Direct scraping hits AWS WAF (202/Captcha). Fix: Stream official daily dumps from `datasets.imdbws.com`.
- **GitHub 100MB File Limit**: Raw `.tsv.gz` and unfiltered DBs exceed Git limits. Fix: Stream/filter in memory (`min_votes >= 1000`, `min_year >= 1900`), keeping DB at ~31.6MB.
- **Popularity Source**: Official TSVs lack live traffic ranks. Fix: Extract live MOVIEMETER rank (`item["rank"]`) from IMDb Suggestion CDN.
- **Stale Popularity vs COALESCE**: `popularity_rank` must update directly (`popularity_rank = ?`) to clear unranked titles, while static poster/cast preserve values via `COALESCE`.
- **Enrichment Failed ID Filtering**: Exclude failed IDs in-memory in Python to prevent both infinite `--all` loops and SQLite variable limit overflows.
- **Direct Crew IDs**: `name.basics.tsv.gz` is 308MB. Fix: Ingest raw `nm` crew IDs directly to maintain zero-disk streaming.
- **Deterministic Ranking**: Rating/vote ties cause rank flip-flops. Fix: `imdb_id ASC` as deterministic 3rd sort key.
- **Ingest Pruning & Crew Stale Clear**: Dropped titles froze and `\N` crew was skipped. Fix: Temp table `prune_unqualified_titles()` and always write crew rows to null out stale directors/writers.
- **Index Migration Rebuild**: `CREATE INDEX IF NOT EXISTS` leaves old definitions unchanged. Fix: Explicitly `DROP INDEX IF EXISTS idx_titles_rating_votes` in `init_db` before creating the 3-column index.

## Project Structure
- `.github/workflows/update.yml`: Monday run & Tuesday retry cron workflow (`42 9 * * 1,2`) publishing to GitHub Releases and deploying to GitHub Pages (`gh-pages`).
- `pyproject.toml` & `uv.lock`: Modern packaging with `uv`, console scripts (`imdb-*`), dependencies (`httpx[http2]`, `msgspec`), and dev tools (`pytest`, `ruff`, `mypy`).
- `src/config.py`: Ingestion thresholds (`min_votes: 1000`, `min_year: 1900`), dataset URLs, enrichment settings.
- `src/db.py`: SQLite schema, indexes, dynamic migrations, batch upserts, rank calculation, and connection manager.
- `src/ingest.py`: Zero-disk streaming filter for ratings, basics, episodes, and crew.
- `src/enrich.py`: Multithreaded HTTP/2 worker (`httpx` + `msgspec`) fetching HD posters, top cast, and MOVIEMETER popularity with 7-day TTL.
- `src/export.py`: Atomic exporter (`msgspec.json.encode`) generating compact columnar JSON (`data/titles.json`) and gzip (`data/titles.json.gz`).
- `src/check_freshness.py`: Freshness checker detecting stale dataset for Tuesday retry.
- `tests/test_pipeline.py`: Automated unit & regression test suite covering all pipeline components.
- `data/`: Generated JSON dataset (`titles.json`, 16.88 MB; `titles.json.gz`, 4.53 MB, git-ignored, distributed via GitHub Releases & GitHub Pages).
- `imdb.db`: SQLite database holding 103,229 indexed titles (~31.6 MB, git-ignored, distributed via GitHub Releases).
- `cache/`: Optional local `.tsv.gz` dumps (git-ignored).

## Execution Workflow
1. `uv sync --all-extras` — Installs all project dependencies and CLI binaries.
2. `uv run imdb-ingest` — Ingests and filters titles, TV episodes, and crew.
3. `uv run imdb-enrich --limit N` — Enriches titles with posters, cast, and MOVIEMETER popularity.
4. `uv run imdb-export` — Exports `data/titles.json` and `data/titles.json.gz`.
5. `uv run pytest` — Runs automated unit and regression test suite.
6. `uv run ruff check src tests` & `uv run mypy src` — Lints and typechecks codebase.
