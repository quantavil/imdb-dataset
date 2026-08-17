# IMDb Clean Dataset & Weekly Data Pipeline

[![Dataset Refresh](https://github.com/quantavil/imdb-dataset/actions/workflows/update.yml/badge.svg)](https://github.com/quantavil/imdb-dataset/actions/workflows/update.yml)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](./LICENSE)
[![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue?logo=python)](https://www.python.org)
[![Fast Package Manager: uv](https://img.shields.io/badge/Managed%20by-uv-DE5FE9?logo=astral)](https://docs.astral.sh/uv/)

A curated, noise-filtered, and pre-indexed IMDb dataset updated automatically every week. Features continuous rankings, HD poster images, top cast, TV series/episode mappings, and live MOVIEMETER popularity.

---

## 🚀 Instant Dataset Downloads & CORS Endpoints

The following links are **static and permanent**. They are automatically overwritten in-place every week on Monday, so downstream web apps and APIs can hardcode these URLs directly:

| Asset | Format | Size | Description | Download / Fetch URL |
| :--- | :--- | :--- | :--- | :--- |
| **`titles.json.gz` (CORS)** | Compressed JSON | **~4.5 MB** | **Recommended for Web & APIs.** GitHub Pages endpoint with native `Access-Control-Allow-Origin: *`. | [Download / Fetch `titles.json.gz`](https://quantavil.github.io/imdb-dataset/titles.json.gz) |
| **`titles.json` (CORS)** | Raw JSON | **~16.9 MB** | Uncompressed columnar JSON dataset for instant browser parsing. | [Download / Fetch `titles.json`](https://quantavil.github.io/imdb-dataset/titles.json) |
| **`imdb.db`** | SQLite 3 | **~31.6 MB** | Fully indexed relational database with pre-computed rankings. | [Download `imdb.db`](https://github.com/quantavil/imdb-dataset/releases/download/latest/imdb.db) |

> [!TIP]
> **Production Web Recommendation:** Use the GitHub Pages URL (`https://quantavil.github.io/imdb-dataset/titles.json.gz`) in client-side web applications. It serves native CORS headers and allows browsers to stream and decompress in under 30ms.

---

## 💡 Quick Start: Consuming the Dataset

### 1. JavaScript / TypeScript (Web Browser & Node.js with CORS)

```javascript
// Stream & decompress directly in modern browsers (or Node.js 18+)
async function loadIMDbDataset() {
  const response = await fetch(
    "https://quantavil.github.io/imdb-dataset/titles.json.gz"
  );
  const stream = response.body.pipeThrough(new DecompressionStream("gzip"));
  const jsonText = await new Response(stream).text();
  const dataset = JSON.parse(jsonText);

  // Map array rows to objects using the fields header
  const { fields, data } = dataset;
  const titles = data.map((row) =>
    Object.fromEntries(fields.map((field, i) => [field, row[i]]))
  );

  console.log(`Loaded ${titles.length} titles. Top title:`, titles[0].title);
  return titles;
}
```

### 2. Python (Pandas / JSON)

```python
import gzip
import json
import urllib.request

url = "https://github.com/quantavil/imdb-dataset/releases/download/latest/titles.json.gz"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

with urllib.request.urlopen(req) as resp, gzip.GzipFile(fileobj=resp) as gz:
    payload = json.loads(gz.read().decode("utf-8"))

fields = payload["fields"]
titles = [dict(zip(fields, row)) for row in payload["data"]]
print(f"Loaded {len(titles):,} titles. Top ranked: {titles[0]['title']} ({titles[0]['rating']}★)")
```

### 3. Python (SQLite Query)

```python
import sqlite3

# Connect to downloaded imdb.db
conn = sqlite3.connect("imdb.db")
conn.row_factory = sqlite3.Row

# Query top 10 animated movies released after 2010
cursor = conn.execute("""
    SELECT rank, title, year, rating, vote_count, poster_url
    FROM titles
    WHERE is_animation = 1 AND title_type = 'movie' AND year >= 2010
    ORDER BY rank ASC
    LIMIT 10;
""")

for row in cursor.fetchall():
    print(f"#{row['rank']} {row['title']} ({row['year']}) - {row['rating']}★ ({row['vote_count']:,} votes)")
```

---

## 📊 Dataset Specifications & Schema

* **Curated Universe:** 103,000+ titles filtered to $\ge 1,000$ votes and release year $\ge 1900$.
* **Breakdown:** ~48,900 Movies, ~12,800 TV Series/Miniseries, ~36,300 TV Episodes, and ~14,200 Animation titles.
* **Deterministic Rank Order:** Sorted by `rating DESC`, `vote_count DESC`, `imdb_id ASC`.

### JSON Columnar Layout (`titles.json`)

To eliminate key repetition over 103,000 records, titles are exported in a compact array-of-arrays structure:

```json
{
  "stats": {
    "total_titles": 103229,
    "total_movies": 48937,
    "total_tv": 12792,
    "total_episodes": 36337,
    "total_animation": 14228,
    "avg_rating": 6.99,
    "min_votes": 1000,
    "last_updated": "2026-08-17"
  },
  "fields": [
    "id", "title", "original_title", "type", "year", "end_year",
    "rating", "votes", "runtime", "genres", "is_adult", "is_animation",
    "poster", "cast", "popularity", "rank", "directors", "parent_id", "season", "episode"
  ],
  "total": 103229,
  "data": [
    ["tt4283088", "Battle of the Bastards", "Battle of the Bastards", "tv_episode", 2016, null, 9.9, 312717, 60, "Drama, Fantasy", 0, 0, "https://...", "Kit Harington, Emilia Clarke", 25, 1, "nm0764527", "tt0944947", 6, 9]
  ]
}
```

### Field Definitions

| Index | Field | Type | Description |
| :--- | :--- | :--- | :--- |
| `0` | `id` | `string` | IMDb identifier (e.g. `tt0903747`) |
| `1` | `title` | `string` | Primary English / Romanized title |
| `2` | `original_title` | `string` | Original language release title |
| `3` | `type` | `string` | `movie`, `tv_series`, `tv_miniseries`, `tv_movie`, `tv_episode`, `short` |
| `4` | `year` | `integer` | Release year |
| `5` | `end_year` | `integer \| null` | Series finale year (or `null`) |
| `6` | `rating` | `float` | IMDb weighted average rating (`1.0` – `10.0`) |
| `7` | `votes` | `integer` | Total registered user votes |
| `8` | `runtime` | `integer \| null` | Runtime duration in minutes |
| `9` | `genres` | `string` | Comma-delimited list of genres |
| `10` | `is_adult` | `0 \| 1` | `1` if categorized as adult content |
| `11` | `is_animation` | `0 \| 1` | `1` if animated production |
| `12` | `poster` | `string \| null` | High-resolution official poster image URL |
| `13` | `cast` | `string \| null` | Top starring cast members |
| `14` | `popularity` | `integer \| null` | Live IMDb MOVIEMETER traffic rank |
| `15` | `rank` | `integer` | Continuous 1..N leaderboard rank |
| `16` | `directors` | `string \| null` | Primary director IDs (e.g. `nm0764527`) |
| `17` | `parent_id` | `string \| null` | Parent TV Series IMDb ID (for TV episodes) |
| `18` | `season` | `integer \| null` | Season number (for TV episodes) |
| `19` | `episode` | `integer \| null` | Episode number (for TV episodes) |

---

## 🔄 Automated Weekly Update Schedule

Updates run automatically via GitHub Actions ([`.github/workflows/update.yml`](file:///.github/workflows/update.yml)):

* **Primary Run (Monday at 09:42 UTC):** Streams fresh daily dumps from `datasets.imdbws.com`, enriches Monday MOVIEMETER traffic ranks, rebuilds the database, and clobber-publishes assets to the `latest` GitHub Release tag.
* **Auto-Retry (Tuesday at 09:42 UTC):** Checks dataset age; if Monday's run succeeded, it exits in 1 second. If Monday failed, it automatically runs the full update.
* **Zero Downtime / Static URLs:** File URLs stay identical across all weekly updates.

---

## 🛠️ Pipeline Developer Guide

For developers contributing to or maintaining the ingestion and enrichment pipeline.

### Project Structure

```text
imdb-dataset/
├── data/                    # Generated exports (git-ignored, published to GitHub Releases)
│   ├── titles.json          # Columnar JSON (15.2 MB)
│   └── titles.json.gz       # Pre-compressed gzip export (4.3 MB)
├── imdb.db                  # Local SQLite database (git-ignored, ~26 MB)
├── cache/                   # Optional local raw .tsv.gz dumps (git-ignored)
├── pyproject.toml           # Project metadata, dependencies, and tool configs
├── src/                     # Core pipeline source code
│   ├── config.py            # Thresholds, URLs, and filter parameters
│   ├── db.py                # SQLite schema, indices, migrations, batch upserts, pruning
│   ├── ingest.py            # In-memory stream filtering (ratings, basics, crew, episodes)
│   ├── enrich.py            # Multithreaded HTTP/2 worker for posters, cast, and popularity
│   ├── export.py            # Atomic columnar JSON & gzip exporter
│   └── check_freshness.py   # Dataset staleness detector for CI skip/retry
└── tests/                   # Automated unit & regression test suite
```

### Local Setup & Execution

1. **Install [`uv`](https://docs.astral.sh/uv/) and sync dependencies:**
   ```bash
   uv sync --all-extras
   ```

2. **Run Pipeline Stages:**
   ```bash
   # 1. Ingest official dumps (streams directly over HTTP, 0 MB disk waste)
   uv run imdb-ingest

   # 2. Enrich top titles with posters, cast & live popularity (7-day TTL cache)
   uv run imdb-enrich --limit 2000

   # 3. Export compact columnar JSON and pre-compressed gzip
   uv run imdb-export
   ```

3. **Run Tests & Code Quality:**
   ```bash
   uv run pytest
   uv run ruff check src tests
   uv run mypy src
   ```

---

## ⚖️ License & Attribution

* **Pipeline Code:** [GPL-3.0 License](./LICENSE)
* **Data Attribution:** Movie and television metadata courtesy of [IMDb](https://www.imdb.com). Used strictly for personal, research, and non-commercial educational purposes under the [IMDb Non-Commercial Datasets Terms of Service](https://developer.imdb.com/non-commercial-datasets/).
