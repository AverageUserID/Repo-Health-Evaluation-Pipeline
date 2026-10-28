"""Task 5: score each repo's health from mart_health_inputs and rank the repos."""

import duckdb
import pandas as pd
from prefect import task

from pipeline.config import DB_PATH
from pipeline.logs import TeeLogger

# Scored metric -> (dimension, direction, NULL rule). Direction +1 means higher is
# healthier, -1 lower. Definitions are in models/marts/schema.yml. Every metric is a
# share, median, ratio, or size-relative slope, so a big repo can't win by volume alone.
# NULL rules follow what the NULL means:
#   zero  - NULL means "none of these", which is the best case: fill the raw value with 0
#   worst - NULL means nothing to measure (no reviews, merges, releases): score 0
#   skip  - NULL means the window is too short to tell: leave it out and renormalize
METRICS = {
    "commit_trend_pct": ("activity", +1, "skip"),
    "median_hours_to_first_review": ("responsiveness", -1, "worst"),
    "pct_reviewed_within_7d": ("responsiveness", +1, "skip"),
    "merge_rate": ("throughput", +1, "worst"),
    "pct_open_stale": ("backlog", -1, "zero"),
    "open_backlog_growth_pct": ("backlog", -1, "skip"),
    "release_interval_cv": ("release_cadence", -1, "worst"),
    "bus_factor": ("contributors", +1, "worst"),
}

# Dimension weights, ordered by ASSESSING-HEALTH.md: responsiveness first, bus factor and
# contributor diversity second, backlog and acceptance next, raw activity last.
WEIGHTS = {
    "responsiveness": 0.25,
    "contributors": 0.20,
    "backlog": 0.20,
    "throughput": 0.15,
    "release_cadence": 0.10,
    "activity": 0.10,
}


def score_metrics(inputs: pd.DataFrame, log: TeeLogger) -> pd.DataFrame:
    """One row per (repo, metric): raw value, NULL rule applied, and a 0-1 min-max
    score across repos where 1 is the healthiest repo and 0 the least healthy."""
    raw, rows = inputs.set_index("repo"), []
    for metric, (dimension, direction, rule) in METRICS.items():
        value = raw[metric].astype(float)
        missing = value.isna()
        if rule == "zero":
            value = value.fillna(0.0)
        lo, hi = value.min(), value.max()
        scaled = (value - lo) / (hi - lo) if hi > lo else value * 0 + 0.5
        score = scaled if direction > 0 else 1 - scaled
        if rule == "worst":
            score = score.fillna(0.0)
        for repo in value.index:
            applied = rule if missing[repo] else None
            if applied:
                log.warning(f"{repo} {metric} is NULL: rule '{rule}' applied")
            rows.append((repo, metric, dimension, direction, raw.at[repo, metric], applied,
                         score[repo]))
    return pd.DataFrame(rows, columns=["repo", "metric", "dimension", "direction", "value",
                                       "null_rule", "score"])


def rank_repos(metric_scores: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    """Dimension subscore = mean of its non-NULL metric scores; overall score = weighted
    mean of the repo's non-NULL subscores (weights renormalized over those)."""
    subscores = (metric_scores.groupby(["repo", "dimension"])["score"].mean()
                 .unstack()[list(weights)])
    w = pd.Series(weights)
    present = subscores.notna()
    score = (subscores.fillna(0) * w).sum(axis=1) / (present * w).sum(axis=1)
    ranked = subscores.assign(score=score).reset_index()
    ranked.insert(0, "rank", ranked["score"].rank(ascending=False, method="min").astype(int))
    ranked = ranked[["rank", "repo", "score", *weights]].sort_values(["rank", "repo"])
    return ranked.reset_index(drop=True)


def _write(con: duckdb.DuckDBPyConnection, name: str, df: pd.DataFrame) -> None:
    con.register("_df", df)
    con.execute(f"create or replace table {name} as select * from _df")
    con.unregister("_df")


@task
def model() -> list[dict]:
    """Task 5. Score, rank, check the ranking against equal weights, and write
    health_scores (the ranking) and health_metric_scores (how each score was built)."""
    log = TeeLogger("model")
    # Not read_only, for the same reason as Task 4 (dbt-duckdb holds the database open).
    with duckdb.connect(str(DB_PATH)) as con:
        inputs = con.execute(
            f"select repo, {', '.join(METRICS)} from mart_health_inputs order by repo"
        ).df()
        metric_scores = score_metrics(inputs, log)
        ranked = rank_repos(metric_scores, WEIGHTS)
        equal = rank_repos(metric_scores, {d: 1 / len(WEIGHTS) for d in WEIGHTS})

        log.info("Metric values and 0-1 scores (1 = healthiest of the three)\n"
                 + metric_scores.round(3).to_string(index=False))
        log.info("Weights: " + ", ".join(f"{d} {w:.0%}" for d, w in WEIGHTS.items()))
        log.info("Health ranking (score and subscores 0-1)\n"
                 + ranked.round(3).to_string(index=False))
        same = list(ranked["repo"]) == list(equal["repo"])
        log.info("Sensitivity, equal weights: "
                 + ", ".join(f"{r.rank}. {r.repo} ({r.score:.3f})" for r in equal.itertuples())
                 + (" - same order" if same else " - ORDER CHANGES"))

        _write(con, "health_scores", ranked)
        _write(con, "health_metric_scores", metric_scores)
    return ranked.to_dict("records")
