"""GraphQL queries for PRs and reviews, and the mapping to the REST JSON shape.

Landed files use REST field names and values, so Task 2, dbt, and Task 4 read either
API's files the same way, and a PR or review has the same key in both modes.
"""

from pipeline.config import API_URL

RATE_LIMIT = "rateLimit { cost remaining resetAt }"
ACTOR = "author { __typename login }"
REVIEW_FIELDS = f"databaseId state submittedAt {ACTOR}"

# Newest-updated PRs first, each with its first 100 reviews (about 1 point per query).
PULLS_QUERY = f"""
query($owner: String!, $name: String!, $size: Int!, $cursor: String) {{
  repository(owner: $owner, name: $name) {{
    pullRequests(first: $size, after: $cursor, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{
        number state createdAt updatedAt closedAt mergedAt {ACTOR}
        reviews(first: 100) {{
          pageInfo {{ hasNextPage endCursor }}
          nodes {{ {REVIEW_FIELDS} }}
        }}
      }}
    }}
  }}
  {RATE_LIMIT}
}}
"""

# Follow-up for the rare PR with more than 100 reviews.
REVIEWS_QUERY = f"""
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
  repository(owner: $owner, name: $name) {{
    pullRequest(number: $number) {{
      reviews(first: 100, after: $cursor) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{ {REVIEW_FIELDS} }}
      }}
    }}
  }}
  {RATE_LIMIT}
}}
"""


def rest_user(actor: dict | None) -> dict | None:
    """GraphQL author -> REST user. Deleted accounts have no author (REST: null user).
    REST bot logins end in [bot] (dependabot[bot]); GraphQL drops the suffix."""
    if actor is None:
        return None
    login, kind = actor["login"], actor["__typename"]
    if kind == "Bot" and not login.endswith("[bot]"):
        login += "[bot]"
    return {"login": login, "type": kind}


def rest_pull(pr: dict) -> dict:
    return {
        "number": pr["number"],
        # REST has only open/closed; a merged PR is closed with merged_at set.
        "state": "open" if pr["state"] == "OPEN" else "closed",
        "user": rest_user(pr["author"]),
        "created_at": pr["createdAt"],
        "updated_at": pr["updatedAt"],
        "closed_at": pr["closedAt"],
        "merged_at": pr["mergedAt"],
    }


def rest_review(review: dict, repo: str, number: int) -> dict:
    return {
        "id": review["databaseId"],
        "state": review["state"],
        "submitted_at": review["submittedAt"],
        "user": rest_user(review["author"]),
        "pull_request_url": f"{API_URL}/repos/{repo}/pulls/{number}",
    }
