# IMDb Data Pipeline & Leaderboard Engine

Lightweight Python & SQLite pipeline that streams official IMDb daily dumps, enriches titles with live MOVIEMETER popularity and official posters, and exports compact columnar JSON for browser leaderboards.

---

## Features

- **In-Memory Streaming**: Streams official IMDb `.tsv.gz` dumps over HTTP or local cache, discarding sub-1,000 vote noise in flight (0 MB disk waste).
- **Official Public Datasets**: Ingests `title.ratings`, `title.basics`, `title.crew` (directors/writers), and `title.episode` (season/episode mapping).
- **Live Popularity & Posters**: Multithreaded enrichment via IMDb Suggestion CDN (`v3.sg.media-imdb.com`) with a 7-day TTL cache for weekly popularity ranks, top cast, and HD posters.
- **Compact Columnar JSON & Gzip**: Exports `data/titles.json` (15.18 MB) and pre-compressed `data/titles.json.gz` (4.30 MB / 88,363 titles) with embedded summary stats.

---

## Project Structure

```text
imdb-dataset/
├── data/                    # Generated JSON datasets (git-ignored, distributed via GitHub Releases)
│   ├── titles.json          # Curated universe (Votes >= 1,000, 15.18 MB)
│   └── titles.json.gz       # Pre-compressed gzip export (4.30 MB)
├── imdb.db                  # Local SQLite database (git-ignored, ~26 MB)
├── cache/                   # Optional local raw .tsv.gz dumps (git-ignored)
├── pyproject.toml           # Project metadata, dependencies, and tool configs
└── src/                     # Pipeline source code
    ├── __init__.py          # Package initializer
    ├── config.py            # Dataset URLs, thresholds, and filter settings
    ├── db.py                # SQLite schema, indices, migrations, batch upserts, pruning
    ├── ingest.py            # Stream filter for ratings, basics, episodes, crew
    ├── enrich.py            # Multithreaded poster, cast, and popularity worker
    ├── export.py            # Columnar JSON dataset exporter (atomic + gzip)
    └── check_freshness.py   # Dataset staleness detector for CI skip and retry
```

---

## Database Summary (`imdb.db`)

* **Indexed Titles:** 88,363 (Filtered to `votes >= 1,000`, `year >= 1990`)
  * **Movies:** 37,076
  * **TV Episodes:** 35,666 (Linked with parent series, season, and episode number)
  * **TV Series & Miniseries:** 11,766
  * **Animation:** 13,193
* **Size:** ~26 MB

### Table Schema (`titles`)

| Column | Type | Description |
| :--- | :--- | :--- |
| `imdb_id` | `TEXT PRIMARY KEY` | IMDb identifier (e.g. `tt0903747`) |
| `title` | `TEXT` | Primary title in English / Romanized script |
| `original_title` | `TEXT` | Title in original release language |
| `title_type` | `TEXT` | `movie`, `tv_series`, `tv_miniseries`, `tv_movie`, `tv_episode`, `short` |
| `year` | `INTEGER` | Release year |
| `end_year` | `INTEGER` | Series finale year (or `NULL`) |
| `rating` | `REAL` | IMDb average rating (1.0 – 10.0) |
| `vote_count` | `INTEGER` | Total user votes |
| `runtime_minutes` | `INTEGER` | Runtime in minutes |
| `genres` | `TEXT` | Comma-separated genres |
| `is_adult` | `INTEGER` | `1` if adult content, `0` otherwise |
| `is_animation` | `INTEGER` | `1` if animated production, `0` otherwise |
| `poster_url` | `TEXT` | Official HD poster image URL |
| `cast_members` | `TEXT` | Top starring cast members |
| `directors` | `TEXT` | Primary director IDs (e.g. `nm0764527`) |
| `writers` | `TEXT` | Primary writer IDs (e.g. `nm0001851`) |
| `parent_id` | `TEXT` | Parent TV show IMDb ID (for episodes) |
| `season_number` | `INTEGER` | Season number (for episodes) |
| `episode_number` | `INTEGER` | Episode number (for episodes) |
| `popularity_rank` | `INTEGER` | Live MOVIEMETER traffic rank |
| `rank` | `INTEGER` | Overall leaderboard rank |
| `updated_at` | `DATETIME` | Last sync timestamp |
| `enriched_at` | `DATETIME` | Enrichment timestamp (refreshed via 7-day TTL) |

---

## Quick Start

### 1. Requirements & Setup

This project uses [`uv`](https://docs.astral.sh/uv/) for blazing fast dependency management:

```bash
# Sync virtual environment and install all dependencies
uv sync --all-extras
```

### 2. Run Pipeline

```bash
# Ingest all datasets (streams over HTTP by default, or point to local cache)
uv run imdb-ingest

# Optional: use local cached .tsv.gz files
uv run imdb-ingest \
  --ratings-file cache/title.ratings.tsv.gz \
  --basics-file cache/title.basics.tsv.gz \
  --crew-file cache/title.crew.tsv.gz \
  --episodes-file cache/title.episode.tsv.gz

# Enrich top titles with posters, cast & live popularity
uv run imdb-enrich --limit 2000

# Export compact JSON and gzip
uv run imdb-export

# Run test suite and linter
uv run pytest
uv run ruff check src tests
uv run mypy src
```

---

## Exported JSON Format (`data/titles.json`)

Compact array-of-arrays layout to minimize network transfer:

```json
{
  "stats": {
    "total_titles": 88363,
    "total_movies": 37076,
    "total_tv": 11766,
    "total_episodes": 35666,
    "total_animation": 13193,
    "avg_rating": 7.04,
    "min_votes": 1000,
    "last_updated": "2026-08-17"
  },
  "fields": [
    "id", "title", "original_title", "type", "year", "end_year",
    "rating", "votes", "runtime", "genres", "is_adult", "is_animation",
    "poster", "cast", "popularity", "rank", "directors", "parent_id", "season", "episode"
  ],
  "total": 88363,
  "data": [
    ["tt4283088", "Battle of the Bastards", "Battle of the Bastards", "tv_episode", 2016, null, 9.9, 312717, 60, "Drama, Fantasy", 0, 0, "https://...", "Kit Harington, Emilia Clarke", 25, 1, "nm0764527", "tt0944947", 6, 9]
  ]
}
```

---

## Automated Weekly Refresh (CI / GitHub Actions)

The repository includes a GitHub Actions workflow (`.github/workflows/update.yml`) configured for Monday runs and Tuesday retries:
* **Schedule**: Triggers **only on Mondays & Tuesdays at 09:42 UTC** (`42 9 * * 1,2`). **Wed–Sun are completely silent (0 triggers).**
* **Monday (Primary Run)**: Streams official daily dumps, enriches Monday MOVIEMETER popularity ranks, and publishes refreshed `titles.json`, `titles.json.gz`, and `imdb.db` directly to the permanent `latest` GitHub Release (zero Git repository bloat).
* **Tuesday (Auto-Retry if Monday Failed)**: Wakes up on Tuesday:
  * If Monday succeeded ($< 5$ days old) $\rightarrow$ **Skips immediately in 1 second**.
  * If Monday failed $\rightarrow$ **Automatically runs, refreshes, and republishes the release**.
* **Direct Static CDN Access**: Frontends can fetch the static URL directly:
  `https://github.com/<owner>/<repo>/releases/download/latest/titles.json.gz`
* **Manual Run**: Supports on-demand manual triggers via GitHub Actions `workflow_dispatch`.

---

## License & Attribution

- **Code:** [GPL-3.0](./LICENSE)
- **Data Attribution:** Information and metadata courtesy of [IMDb](https://www.imdb.com). Used strictly for personal, research, and non-commercial educational purposes under [IMDb Non-Commercial Datasets Terms of Service](https://developer.imdb.com/non-commercial-datasets/).
