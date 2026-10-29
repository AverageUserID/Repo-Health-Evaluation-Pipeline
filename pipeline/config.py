"""Shared paths and settings for the pipeline."""

import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
LOADED_MARKER = RAW_DIR / "_LOADED"
DB_PATH = PROJECT_DIR / "packages.duckdb"
LOG_DIR = PROJECT_DIR / "logs"
PLOT_DIR = PROJECT_DIR / "plots"

API_URL = "https://api.github.com"
PER_PAGE = 100

# PRs and reviews come from GraphQL by default; PIPELINE_API=rest uses the REST
# endpoints instead (commits and releases always use REST). Kept for the comparison.
API_MODE = os.environ.get("PIPELINE_API", "graphql")

# Window start is fixed by the assignment; override only for faster development runs.
WINDOW_START = os.environ.get("PIPELINE_START", "2024-01-01T00:00:00Z")

REPOS = [
    r.strip()
    for r in os.environ.get(
        "PIPELINE_REPOS", "scikit-learn/scikit-learn,dask/dask,plotly/plotly.py"
    ).split(",")
    if r.strip()
]

# Commits can be dated before they land on the default branch (rebases, long-lived PRs),
# so incremental runs look back this far past the watermark. Task 2 upserts the overlap.
COMMIT_LOOKBACK_DAYS = 7
