"""Task 1: pull every page of every endpoint from GitHub into data/raw/ as JSON files."""

import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import httpx
from prefect import task
from prefect.tasks import exponential_backoff

from pipeline.config import (
    COMMIT_LOOKBACK_DAYS,
    DB_PATH,
    LOADED_MARKER,
    PER_PAGE,
    RAW_DIR,
    REPOS,
    WINDOW_START,
)
from pipeline.github import TransientGitHubError, get_client
from pipeline.logs import TeeLogger

ISO = "%Y-%m-%dT%H:%M:%SZ"


def _is_transient(task, task_run, state) -> bool:
    """Retry only network blips and 5xx; a 404 will not fix itself."""
    try:
        state.result()
    except (httpx.TransportError, TransientGitHubError):
        return True
    except Exception:
        return False
    return False


RETRY = dict(
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    retry_jitter_factor=0.5,
    retry_condition_fn=_is_transient,
)


def repo_dir(repo: str) -> Path:
    return RAW_DIR / repo.replace("/", "__")


def read_landing(path: Path) -> dict | None:
    """Return a landed page, or None if it is missing or was cut off mid-write."""
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_landing(path: Path, page: dict) -> None:
    """Write to a temp file and rename, so a crash never leaves a half-written page."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(page))
    os.replace(tmp, path)


def paginate(repo: str, endpoint: str, path: str, params: dict, out_dir: Path, stop=None) -> dict:
    """Follow Link rel="next" from page 1, skipping pages already on disk.

    Each landed file stores the next-page URL, so a resumed run can walk past
    landed pages without calling the API. `stop(records)` ends pagination early.
    """
    client = get_client()
    url = str(httpx.URL(path, params={**params, "per_page": PER_PAGE}))
    stats = {"pages": 0, "fetched": 0, "records": 0}
    page_no = 1

    while url:
        file = out_dir / f"page_{page_no:04d}.json"
        landed = read_landing(file)
        if landed is None:
            resp = client.get(url)
            try:
                data = resp.json()
            except json.JSONDecodeError as exc:
                raise TransientGitHubError(f"Unparseable body from {resp.request.url}") from exc
            landed = {
                "repo": repo,
                "endpoint": endpoint,
                "url": str(resp.request.url),
                "page": page_no,
                "fetched_at": datetime.now(timezone.utc).strftime(ISO),
                "next_url": resp.links.get("next", {}).get("url"),
                "data": data,
            }
            write_landing(file, landed)
            stats["fetched"] += 1

        stats["pages"] += 1
        stats["records"] += len(landed["data"])
        if stop and landed["data"] and stop(landed["data"]):
            break
        url = landed["next_url"]
        page_no += 1
    return stats


def read_watermarks() -> dict[tuple[str, str], str]:
    """Newest updated_at per (repo, endpoint) from Task 2's _load_state table."""
    if not DB_PATH.exists():
        return {}
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        exists = con.execute(
            "select count(*) from information_schema.tables where table_name = '_load_state'"
        ).fetchone()[0]
        if not exists:
            return {}
        rows = con.execute("select repo, endpoint, max_updated_at from _load_state").fetchall()
    return {(repo, ep): ts.astimezone(timezone.utc).strftime(ISO) for repo, ep, ts in rows if ts}


def _log_stats(log: TeeLogger, repo: str, endpoint: str, stats: dict, requests: int) -> None:
    log.info(
        f"{repo} {endpoint}: {stats['records']} records, {stats['pages']} pages "
        f"({stats['fetched']} fetched, {stats['pages'] - stats['fetched']} already on disk), "
        f"{requests} requests"
    )


@task(**RETRY)
def extract_commits(repo: str, since: str) -> dict:
    log, client = TeeLogger("extract"), get_client()
    start = client.requests
    stats = paginate(
        repo, "commits", f"/repos/{repo}/commits", {"since": since}, repo_dir(repo) / "commits"
    )
    _log_stats(log, repo, "commits", stats, client.requests - start)
    return stats


@task(**RETRY)
def extract_pulls(repo: str, since: str) -> dict:
    """PRs have no date filter: newest-updated first, stop once a page passes `since`."""
    log, client = TeeLogger("extract"), get_client()
    start = client.requests
    stats = paginate(
        repo,
        "pulls",
        f"/repos/{repo}/pulls",
        {"state": "all", "sort": "updated", "direction": "desc"},
        repo_dir(repo) / "pulls",
        stop=lambda records: records[-1]["updated_at"] < since,
    )
    _log_stats(log, repo, "pulls", stats, client.requests - start)
    return stats


@task(**RETRY)
def extract_releases(repo: str) -> dict:
    log, client = TeeLogger("extract"), get_client()
    start = client.requests
    stats = paginate(repo, "releases", f"/repos/{repo}/releases", {}, repo_dir(repo) / "releases")
    _log_stats(log, repo, "releases", stats, client.requests - start)
    return stats


def prs_needing_reviews(repo: str, since: str) -> list[int]:
    """PR numbers created in the window and updated since the last load, from landed pages."""
    numbers = set()
    for file in sorted((repo_dir(repo) / "pulls").glob("page_*.json")):
        for pr in read_landing(file)["data"]:
            if pr["created_at"] >= WINDOW_START and pr["updated_at"] >= since:
                numbers.add(pr["number"])
    return sorted(numbers)


@task(**RETRY)
def extract_reviews(repo: str, since: str) -> dict:
    """One paginated call per PR, landed under reviews/<number>/page_0001.json."""
    log, client = TeeLogger("extract"), get_client()
    start = client.requests
    numbers = prs_needing_reviews(repo, since)
    log.info(f"{repo} reviews: {len(numbers)} PRs need review pages")

    totals = {"pages": 0, "fetched": 0, "records": 0}
    for i, number in enumerate(numbers, 1):
        stats = paginate(
            repo,
            "reviews",
            f"/repos/{repo}/pulls/{number}/reviews",
            {},
            repo_dir(repo) / "reviews" / str(number),
        )
        totals = {k: totals[k] + stats[k] for k in totals}
        if i % 500 == 0:
            log.info(f"{repo} reviews: {i}/{len(numbers)} PRs done")
    _log_stats(log, repo, "reviews", totals, client.requests - start)
    return totals


@task
def extract() -> dict:
    """Task 1. Resume an interrupted run, or start fresh from the _load_state watermark."""
    log, client = TeeLogger("extract"), get_client()
    start = client.requests

    if LOADED_MARKER.exists():
        log.info("Found _LOADED: previous run fully loaded, clearing data/raw/ and running incrementally")
        shutil.rmtree(RAW_DIR)
        watermarks = read_watermarks()
    elif RAW_DIR.exists():
        log.info("No _LOADED marker: resuming, pages already in data/raw/ will be skipped")
        watermarks = read_watermarks()
    else:
        log.info(f"No data/raw/: full extract from {WINDOW_START}")
        watermarks = {}

    summary = {}
    for repo in REPOS:
        pulls_since = watermarks.get((repo, "pulls"), WINDOW_START)
        commits_since = watermarks.get((repo, "commits"), WINDOW_START)
        if commits_since != WINDOW_START:
            lookback = datetime.strptime(commits_since, ISO) - timedelta(days=COMMIT_LOOKBACK_DAYS)
            commits_since = max(WINDOW_START, lookback.strftime(ISO))
        log.info(f"{repo}: commits since {commits_since}, PRs updated since {pulls_since}")

        summary[repo] = {
            "commits": extract_commits(repo, commits_since),
            "pulls": extract_pulls(repo, pulls_since),
            "reviews": extract_reviews(repo, pulls_since),
            "releases": extract_releases(repo),
        }

    requests = client.requests - start
    log.info(f"Extract finished: {requests} API requests this run")
    return {"requests": requests, "repos": summary}
