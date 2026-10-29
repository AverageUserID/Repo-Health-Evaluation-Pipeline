"""Rate-limited GitHub REST and GraphQL client shared by all extract tasks."""

import os
import threading
import time
from datetime import datetime

import httpx

from pipeline.config import API_URL
from pipeline.logs import TeeLogger

# Sleep until reset once fewer than this many remain in a budget. REST counts requests;
# GraphQL counts points (a 100-PR query with nested reviews costs about 1).
RATE_LIMIT_BUFFER = {"core": 20, "graphql": 100}
# How many 403/429 secondary-limit waits to absorb before giving up on a request.
MAX_SECONDARY_WAITS = 5
# Most HTTP requests in flight at once across all threads; GitHub's secondary limits
# penalize bursts of concurrent requests.
MAX_IN_FLIGHT = 4


class TransientGitHubError(Exception):
    """A failure worth retrying (5xx, truncated body, repeated secondary limits)."""


class GraphQLTimeout(TransientGitHubError):
    """GitHub gave up on a GraphQL query (502/504); a smaller page usually succeeds."""


class GitHubAPIError(Exception):
    """A non-retryable failure (404, 401, bad request, invalid query)."""


class ExtractInterrupted(Exception):
    """The run was interrupted (Ctrl-C); worker threads stop before their next request."""


class GitHubClient:
    """Wraps httpx with rate-limit tracking and an API odometer.

    REST ("core") and GraphQL have separate hourly budgets, tracked separately. All
    threads share one client, so it is the shared limiter for concurrent repo fetches:
    each request reserves budget under the lock before it is sent, and a semaphore caps
    how many requests are in flight at once.
    """

    def __init__(self, token: str):
        self.http = httpx.Client(
            base_url=API_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=60.0,
        )
        self.lock = threading.Lock()
        self.requests = 0  # every HTTP call, REST or GraphQL (the odometer)
        self.points = 0  # GraphQL points spent (sum of rateLimit.cost)
        # Per-thread counts, so each repo's logs show its own requests while repos run
        # in parallel (one repo per worker thread).
        self.local = threading.local()
        self.budgets = {name: {"remaining": None, "reset_at": None} for name in RATE_LIMIT_BUFFER}
        self.in_flight = threading.Semaphore(MAX_IN_FLIGHT)
        # Set when the run is interrupted. Ctrl-C only reaches the main thread, so worker
        # threads check this before every request and stop at the next page boundary.
        self.stop = threading.Event()

    @property
    def thread_requests(self) -> int:
        """HTTP requests made by the calling thread."""
        return getattr(self.local, "requests", 0)

    @property
    def thread_points(self) -> int:
        """GraphQL points spent by the calling thread."""
        return getattr(self.local, "points", 0)

    def _reserve(self, resource: str) -> None:
        """Wait out the budget if it is at the buffer, then reserve one unit (a request,
        or about one GraphQL point) before sending. Reserving under the lock means
        parallel threads can't all pass the check at the edge of the buffer. A thread
        that sleeps holds the lock, so every thread pauses until the reset."""
        with self.lock:
            budget = self.budgets[resource]
            if budget["remaining"] is not None and budget["remaining"] <= RATE_LIMIT_BUFFER[resource]:
                wait = max(0.0, budget["reset_at"] - time.time()) + 5
                TeeLogger("extract").warning(
                    f"Rate limit ({resource}): {budget['remaining']} left, sleeping {wait:.0f}s until reset"
                )
                if self.stop.wait(wait):  # sleeps, but wakes at once if interrupted
                    raise ExtractInterrupted("Interrupted during rate-limit wait")
                budget["remaining"] = None
            if budget["remaining"] is not None:
                budget["remaining"] -= 1

    def _update_budget(self, resource: str, remaining: int, reset_at: float) -> None:
        """Take the server's count, unless an older response arrives after newer
        reservations (same window, higher count): then keep the lower, reserved value.
        Call with the lock held."""
        budget = self.budgets[resource]
        same_window = budget["reset_at"] is not None and reset_at <= budget["reset_at"]
        if same_window and budget["remaining"] is not None:
            remaining = min(remaining, budget["remaining"])
        self.budgets[resource] = {"remaining": remaining, "reset_at": reset_at}

    def _track(self, resp: httpx.Response) -> None:
        self.local.requests = self.thread_requests + 1
        with self.lock:
            self.requests += 1
            if "X-RateLimit-Remaining" in resp.headers:
                resource = resp.headers.get("X-RateLimit-Resource", "core")
                if resource in self.budgets:
                    self._update_budget(
                        resource,
                        int(resp.headers["X-RateLimit-Remaining"]),
                        float(resp.headers["X-RateLimit-Reset"]),
                    )

    def _send(self, resource: str, method: str, url: str, **kwargs) -> httpx.Response:
        """Send one request, sleeping through primary and secondary rate limits."""
        for _ in range(MAX_SECONDARY_WAITS):
            if self.stop.is_set():
                raise ExtractInterrupted(f"Interrupted before {method} {url}")
            self._reserve(resource)
            with self.in_flight:
                resp = self.http.request(method, url, **kwargs)
            self._track(resp)

            if resp.status_code in (403, 429):
                if "Retry-After" in resp.headers:
                    wait = int(resp.headers["Retry-After"]) + 1
                elif resp.headers.get("X-RateLimit-Remaining") == "0":
                    wait = max(0.0, float(resp.headers["X-RateLimit-Reset"]) - time.time()) + 5
                elif resp.status_code == 403:
                    raise GitHubAPIError(f"403 Forbidden for {resp.request.url}: {resp.text[:200]}")
                else:
                    wait = 60
                TeeLogger("extract").warning(
                    f"{resp.status_code} rate limited on {resp.request.url}, waiting {wait:.0f}s"
                )
                if self.stop.wait(wait):
                    raise ExtractInterrupted("Interrupted during rate-limit wait")
                continue
            return resp
        raise TransientGitHubError(f"Still rate limited after {MAX_SECONDARY_WAITS} waits: {url}")

    def get(self, url: str, params: dict | None = None) -> httpx.Response:
        """GET one REST page. Raises on anything but success."""
        resp = self._send("core", "GET", url, params=params)
        if resp.status_code >= 500:
            raise TransientGitHubError(f"{resp.status_code} from {resp.request.url}")
        if resp.status_code >= 400:
            raise GitHubAPIError(f"{resp.status_code} for {resp.request.url}: {resp.text[:200]}")
        return resp

    def graphql(self, query: str, variables: dict) -> dict:
        """Run one GraphQL query and return its `data`. The query must select
        rateLimit { cost remaining resetAt }, which updates the GraphQL budget.

        GraphQL reports most errors inside a 200 response: RATE_LIMITED waits for the
        reset and retries; anything else is a bad query or missing object and fails.
        """
        for _ in range(MAX_SECONDARY_WAITS):
            resp = self._send("graphql", "POST", "/graphql", json={"query": query, "variables": variables})
            if resp.status_code in (502, 504):
                raise GraphQLTimeout(f"{resp.status_code} from GraphQL")
            if resp.status_code >= 500:
                raise TransientGitHubError(f"{resp.status_code} from GraphQL")
            if resp.status_code >= 400:
                raise GitHubAPIError(f"{resp.status_code} from GraphQL: {resp.text[:200]}")
            try:
                body = resp.json()
            except ValueError as exc:
                raise TransientGitHubError("Unparseable GraphQL body") from exc

            rate = (body.get("data") or {}).get("rateLimit")
            if rate:
                self.local.points = self.thread_points + rate["cost"]
                with self.lock:
                    self.points += rate["cost"]
                    self._update_budget(
                        "graphql",
                        rate["remaining"],
                        datetime.fromisoformat(rate["resetAt"].replace("Z", "+00:00")).timestamp(),
                    )

            errors = body.get("errors")
            if not errors:
                return body["data"]
            if any(e.get("type") == "RATE_LIMITED" for e in errors):
                with self.lock:
                    self.budgets["graphql"]["remaining"] = 0
                    if self.budgets["graphql"]["reset_at"] is None:
                        self.budgets["graphql"]["reset_at"] = time.time() + 60
                continue  # _reserve sleeps until the reset
            if any("timeout" in e.get("message", "").lower() for e in errors):
                raise GraphQLTimeout(errors[0]["message"])
            raise GitHubAPIError(f"GraphQL errors: {errors[:3]}")
        raise TransientGitHubError(f"Still rate limited after {MAX_SECONDARY_WAITS} waits: GraphQL")


_client: GitHubClient | None = None


def get_client() -> GitHubClient:
    """Return the process-wide client, created on first use from GH_PAT."""
    global _client
    if _client is None:
        token = os.environ.get("GH_PAT")
        if not token:
            from prefect.blocks.system import Secret

            token = Secret.load(os.environ.get("GH_PAT_BLOCK", "gh-pat")).get()
        _client = GitHubClient(token)
    return _client
