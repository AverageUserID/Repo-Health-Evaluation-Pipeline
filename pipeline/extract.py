"""Task 1: pull every page of every endpoint from GitHub into data/raw/ as JSON files."""

import json
import os
import shutil
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import httpx
from prefect import task, unmapped
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import exponential_backoff

from pipeline.config import (
    API_MODE,
    COMMIT_LOOKBACK_DAYS,
    CONCURRENCY,
    DB_PATH,
    LOADED_MARKER,
    PER_PAGE,
    RAW_DIR,
    REPOS,
    WINDOW_START,
)
from pipeline.github import GraphQLTimeout, TransientGitHubError, get_client
from pipeline.graphql import PULLS_QUERY, REVIEWS_QUERY, rest_pull, rest_review
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


def read_landing(path: Path, resume_key: str = "next_url") -> dict | None:
    """Return a landed page, or None if it is missing or was cut off mid-write.

    `resume_key` is the field a resume follows (REST next_url, GraphQL next_cursor); a
    page without it was landed by the other API mode, which can't be resumed mid-run.
    """
    try:
        page = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if resume_key not in page:
        raise RuntimeError(
            f"{path} was landed in the other API mode; finish the run with that "
            f"PIPELINE_API or delete data/raw/ to start over"
        )
    return page


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
    start = client.thread_requests
    stats = paginate(
        repo, "commits", f"/repos/{repo}/commits", {"since": since}, repo_dir(repo) / "commits"
    )
    _log_stats(log, repo, "commits", stats, client.thread_requests - start)
    return stats


@task(**RETRY)
def extract_pulls(repo: str, since: str) -> dict:
    """PRs have no date filter: newest-updated first, stop once a page passes `since`."""
    log, client = TeeLogger("extract"), get_client()
    start = client.thread_requests
    stats = paginate(
        repo,
        "pulls",
        f"/repos/{repo}/pulls",
        {"state": "all", "sort": "updated", "direction": "desc"},
        repo_dir(repo) / "pulls",
        stop=lambda records: records[-1]["updated_at"] < since,
    )
    _log_stats(log, repo, "pulls", stats, client.thread_requests - start)
    return stats


@task(**RETRY)
def extract_releases(repo: str) -> dict:
    log, client = TeeLogger("extract"), get_client()
    start = client.thread_requests
    stats = paginate(repo, "releases", f"/repos/{repo}/releases", {}, repo_dir(repo) / "releases")
    _log_stats(log, repo, "releases", stats, client.thread_requests - start)
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
    start = client.thread_requests
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
    _log_stats(log, repo, "reviews", totals, client.thread_requests - start)
    return totals


# GraphQL page sizes to try, largest first; GitHub times out on big nested queries.
GRAPHQL_PAGE_SIZES = (100, 50, 25)


def _query_pulls(repo: str, cursor: str | None, log: TeeLogger) -> dict:
    """One page of PRs with nested reviews, halving the page size on a server timeout."""
    owner, name = repo.split("/")
    for size in GRAPHQL_PAGE_SIZES:
        try:
            data = get_client().graphql(
                PULLS_QUERY, {"owner": owner, "name": name, "size": size, "cursor": cursor}
            )
            return data
        except GraphQLTimeout as exc:
            log.warning(f"{repo} GraphQL timeout at {size} PRs per page ({exc}), retrying smaller")
    raise TransientGitHubError(f"{repo} GraphQL timed out even at {GRAPHQL_PAGE_SIZES[-1]} PRs")


def _all_reviews(repo: str, pr: dict) -> list[dict]:
    """The PR's nested reviews, plus follow-up pages if it has more than 100."""
    reviews, conn = list(pr["reviews"]["nodes"]), pr["reviews"]
    owner, name = repo.split("/")
    while conn["pageInfo"]["hasNextPage"]:
        data = get_client().graphql(REVIEWS_QUERY, {
            "owner": owner, "name": name, "number": pr["number"],
            "cursor": conn["pageInfo"]["endCursor"],
        })
        conn = data["repository"]["pullRequest"]["reviews"]
        reviews += conn["nodes"]
    return reviews


@task(**RETRY)
def extract_pulls_graphql(repo: str, since: str) -> dict:
    """PRs and their reviews in one paginated GraphQL query, newest-updated first.

    Lands the same files as the REST tasks (pulls/page_N.json and
    reviews/<number>/page_0001.json, REST field names). Each page's reviews are written
    before the page itself, so a landed PR page means its reviews are landed too. Like
    REST, reviews are kept for PRs created in the window and updated since `since`.
    """
    log, client = TeeLogger("extract"), get_client()
    start_requests, start_points = client.thread_requests, client.thread_points
    out_dir = repo_dir(repo) / "pulls"
    stats = {"pages": 0, "fetched": 0, "records": 0, "reviews": 0, "review_prs": 0}
    cursor, page_no = None, 1

    while True:
        file = out_dir / f"page_{page_no:04d}.json"
        landed = read_landing(file, resume_key="next_cursor")
        if landed is None:
            data = _query_pulls(repo, cursor, log)
            conn = data["repository"]["pullRequests"]
            fetched_at = datetime.now(timezone.utc).strftime(ISO)
            for pr in conn["nodes"]:
                if pr["createdAt"] >= WINDOW_START and pr["updatedAt"] >= since:
                    reviews = [rest_review(r, repo, pr["number"]) for r in _all_reviews(repo, pr)]
                    write_landing(repo_dir(repo) / "reviews" / str(pr["number"]) / "page_0001.json", {
                        "repo": repo, "endpoint": "reviews", "api": "graphql", "page": 1,
                        "fetched_at": fetched_at, "next_cursor": None, "data": reviews,
                    })
                    stats["review_prs"] += 1
                    stats["reviews"] += len(reviews)
            landed = {
                "repo": repo,
                "endpoint": "pulls",
                "api": "graphql",
                "page": page_no,
                "cursor": cursor,
                "fetched_at": fetched_at,
                "next_cursor": conn["pageInfo"]["endCursor"] if conn["pageInfo"]["hasNextPage"] else None,
                "rate_limit": data["rateLimit"],
                "data": [rest_pull(pr) for pr in conn["nodes"]],
            }
            write_landing(file, landed)
            stats["fetched"] += 1

        stats["pages"] += 1
        stats["records"] += len(landed["data"])
        if landed["data"] and landed["data"][-1]["updated_at"] < since:
            break
        cursor = landed["next_cursor"]
        if not cursor:
            break
        page_no += 1

    log.info(
        f"{repo} pulls+reviews (graphql): {stats['records']} PRs, {stats['pages']} pages "
        f"({stats['fetched']} fetched, {stats['pages'] - stats['fetched']} already on disk), "
        f"{stats['reviews']} reviews for {stats['review_prs']} PRs fetched, "
        f"{client.thread_requests - start_requests} requests, "
        f"{client.thread_points - start_points} points, "
        f"{client.budgets['graphql']['remaining']} points left"
    )
    return stats


class ExtractError(Exception):
    """One or more repos failed to extract; their landed pages are kept for a resume."""


@task
def extract_repo(repo: str, watermarks: dict[tuple[str, str], str]) -> dict:
    """Every endpoint for one repo, in order. Repos run in parallel, one per worker
    thread, all sharing the one rate-limited client."""
    log, client = TeeLogger("extract"), get_client()
    t_start, worker = time.monotonic(), threading.current_thread().name
    r_start = client.thread_requests
    pulls_since = watermarks.get((repo, "pulls"), WINDOW_START)
    commits_since = watermarks.get((repo, "commits"), WINDOW_START)
    if commits_since != WINDOW_START:
        lookback = datetime.strptime(commits_since, ISO) - timedelta(days=COMMIT_LOOKBACK_DAYS)
        commits_since = max(WINDOW_START, lookback.strftime(ISO))
    log.info(f"{repo} started on {worker}: commits since {commits_since}, "
             f"PRs updated since {pulls_since}")

    summary = {"commits": extract_commits(repo, commits_since)}
    # PRs + reviews are timed and counted on their own: the REST vs GraphQL comparison.
    t0, r0, p0 = time.monotonic(), client.thread_requests, client.thread_points
    if API_MODE == "graphql":
        summary["pulls_reviews"] = extract_pulls_graphql(repo, pulls_since)
    else:
        summary["pulls"] = extract_pulls(repo, pulls_since)
        summary["reviews"] = extract_reviews(repo, pulls_since)
    log.info(
        f"{repo} PRs+reviews via {API_MODE}: {client.thread_requests - r0} requests, "
        f"{client.thread_points - p0} GraphQL points, {time.monotonic() - t0:.1f}s"
    )
    summary["releases"] = extract_releases(repo)

    summary["seconds"] = round(time.monotonic() - t_start, 1)
    summary["requests"] = client.thread_requests - r_start
    log.info(f"{repo} finished in {summary['seconds']}s on {worker}, "
             f"{summary['requests']} requests")
    return summary


@task
def extract() -> dict:
    """Task 1. Resume an interrupted run, or start fresh from the _load_state watermark,
    then fetch the repos in parallel (PIPELINE_CONCURRENCY at a time)."""
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

    log.info(f"PRs and reviews via {API_MODE}; {len(REPOS)} repos, concurrency {CONCURRENCY}")
    t0 = time.monotonic()
    with ThreadPoolTaskRunner(max_workers=CONCURRENCY) as runner:
        futures = runner.map(extract_repo, {"repo": REPOS, "watermarks": unmapped(watermarks)})
        futures.wait()

    # Wait for every repo before failing, so healthy repos finish landing their pages.
    summary, failures = {}, []
    for repo, future in zip(REPOS, futures):
        try:
            summary[repo] = future.result()
        except Exception as exc:
            log.error(f"{repo} failed: {type(exc).__name__}: {exc}")
            failures.append(f"{repo}: {exc}")
    if failures:
        raise ExtractError(f"{len(failures)} repo(s) failed; rerun to resume: " + " | ".join(failures))

    wall = time.monotonic() - t0
    summed = sum(r["seconds"] for r in summary.values())
    requests = client.requests - start
    log.info(
        f"Extract finished: {requests} API requests ({client.points} GraphQL points); "
        f"concurrency {CONCURRENCY}, wall {wall:.1f}s, sum of repo times {summed:.1f}s "
        f"({summed / wall:.2f}x)"
    )
    return {"requests": requests, "api": API_MODE, "concurrency": CONCURRENCY,
            "seconds": round(wall, 1), "repos": summary}
