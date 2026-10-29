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


class TransientGitHubError(Exception):
    """A failure worth retrying (5xx, truncated body, repeated secondary limits)."""


class GraphQLTimeout(TransientGitHubError):
    """GitHub gave up on a GraphQL query (502/504); a smaller page usually succeeds."""


class GitHubAPIError(Exception):
    """A non-retryable failure (404, 401, bad request, invalid query)."""


class GitHubClient:
    """Wraps httpx with rate-limit tracking and an API odometer.

    REST ("core") and GraphQL have separate hourly budgets, tracked separately. The
    lock makes the budget checks and counters safe to share across threads, so repos
    can be fetched concurrently through this same client.
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
        self.budgets = {name: {"remaining": None, "reset_at": None} for name in RATE_LIMIT_BUFFER}

    def _wait_for_budget(self, resource: str) -> None:
        with self.lock:
            budget = self.budgets[resource]
            if budget["remaining"] is None or budget["remaining"] > RATE_LIMIT_BUFFER[resource]:
                return
            wait = max(0.0, budget["reset_at"] - time.time()) + 5
            TeeLogger("extract").warning(
                f"Rate limit ({resource}): {budget['remaining']} left, sleeping {wait:.0f}s until reset"
            )
            time.sleep(wait)
            budget["remaining"] = None

    def _track(self, resp: httpx.Response) -> None:
        with self.lock:
            self.requests += 1
            if "X-RateLimit-Remaining" in resp.headers:
                resource = resp.headers.get("X-RateLimit-Resource", "core")
                if resource in self.budgets:
                    self.budgets[resource] = {
                        "remaining": int(resp.headers["X-RateLimit-Remaining"]),
                        "reset_at": float(resp.headers["X-RateLimit-Reset"]),
                    }

    def _send(self, resource: str, method: str, url: str, **kwargs) -> httpx.Response:
        """Send one request, sleeping through primary and secondary rate limits."""
        for _ in range(MAX_SECONDARY_WAITS):
            self._wait_for_budget(resource)
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
                time.sleep(wait)
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
                with self.lock:
                    self.points += rate["cost"]
                    self.budgets["graphql"] = {
                        "remaining": rate["remaining"],
                        "reset_at": datetime.fromisoformat(rate["resetAt"].replace("Z", "+00:00")).timestamp(),
                    }

            errors = body.get("errors")
            if not errors:
                return body["data"]
            if any(e.get("type") == "RATE_LIMITED" for e in errors):
                with self.lock:
                    self.budgets["graphql"]["remaining"] = 0
                    if self.budgets["graphql"]["reset_at"] is None:
                        self.budgets["graphql"]["reset_at"] = time.time() + 60
                continue  # _wait_for_budget sleeps until the reset
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
