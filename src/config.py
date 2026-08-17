"""
IMDb Leaderboard Pipeline Configuration
"""

from pathlib import Path

# Base Paths (Root is one level up from src/)
SRC_DIR = Path(__file__).parent.resolve()
BASE_DIR = SRC_DIR.parent.resolve()
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "imdb.db"

# Official Dataset URLs
URLS = {
    "ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "crew": "https://datasets.imdbws.com/title.crew.tsv.gz",
    "episodes": "https://datasets.imdbws.com/title.episode.tsv.gz",
    "suggestion_base": "https://v3.sg.media-imdb.com/suggestion/x/{id}.json",
}

# Ingestion Filter Settings (Votes >= 1,000)
FILTER_CONFIG = {
    "min_votes": 1000,
    "min_rating": 1.0,
    "max_rating": 10.0,
    "min_year": 1990,
    "allowed_types": {
        "movie": "movie",
        "tvSeries": "tv_series",
        "tvMiniSeries": "tv_miniseries",
        "tvMovie": "tv_movie",
        "tvEpisode": "tv_episode",
        "short": "short",
    },
    "include_adult": False,
}

# Export Settings
EXPORT_CONFIG = {
    "min_votes": 1000,
    "filename": "titles.json",
    "description": "IMDb Leaderboard Universe (Votes >= 1,000)",
}

# Poster Enrichment Settings
ENRICH_CONFIG = {
    "batch_size": 50,
    "max_workers": 12,
    "timeout_sec": 5,
}
