"""Task 4: validate the pipeline end to end in Python (no dbt) and describe the data."""

import duckdb
import pandas as pd
from prefect import task

from pipeline.config import DB_PATH, REPOS
from pipeline.extract import repo_dir
from pipeline.load import ENDPOINTS, READ_JSON
from pipeline.logs import TeeLogger

# How many offending keys to quote in an error message.
MAX_EXAMPLES = 5

# Raw table -> staging model, with the staging key and the rows staging is EXPECTED to
# keep. These exclusions are written out again here on purpose, independently of dbt,
# so a staging model that drops more (or less) than documented is caught.
STAGING = {
    "commits": {"model": "stg_commits", "key": "sha", "keep": "true"},
    "pulls": {"model": "stg_pull_requests", "key": "number", "keep": "true"},
    "reviews": {
        "model": "stg_pr_reviews",
        "key": "review_id",
        "keep": "(payload->>'state') <> 'PENDING'",
    },
    "releases": {
        "model": "stg_releases",
        "key": "release_id",
        "keep": "not coalesce((payload->>'draft')::BOOLEAN, false)",
    },
}


class ValidationError(Exception):
    """The pipeline lost or invented records between stages; the message names them."""


def _examples(df: pd.DataFrame) -> str:
    return ", ".join(f"{r.repo} {r.id}" for r in df.head(MAX_EXAMPLES).itertuples())


def check_files_in_raw(con: duckdb.DuckDBPyConnection, log: TeeLogger) -> list[str]:
    """1a. Every distinct (repo, id) in the landed files appears in its raw table.

    After an incremental run data/raw/ holds only that run's files (earlier ones were
    deleted once loaded), so this verifies the files on disk now.
    """
    errors, rows = [], []
    for endpoint, spec in ENDPOINTS.items():
        for repo in REPOS:
            if not any(repo_dir(repo).glob(spec["glob"])):
                continue
            landed = f"""
                with landed as (
                    select distinct page.repo, ({spec['id']})::{spec['id_type']} as id
                    from {READ_JSON} page,
                         unnest(json_extract(page.data, '$[*]')) t(rec)
                )
            """
            params = {"glob": str(repo_dir(repo) / spec["glob"])}
            n_landed = con.execute(landed + "select count(*) from landed", params).fetchall()[0][0]
            missing = con.execute(
                landed + f"""
                select l.repo, l.id from landed l
                anti join {spec['table']} r on r.repo = l.repo and r.id = l.id
                """,
                params,
            ).df()
            rows.append((repo, endpoint, n_landed, len(missing)))
            if len(missing):
                errors.append(
                    f"{spec['table']}: {len(missing)} landed {repo} records missing, "
                    f"e.g. {_examples(missing)}"
                )
    table = pd.DataFrame(rows, columns=["repo", "endpoint", "landed_keys", "missing_in_raw"])
    log.info("1a. Landed files -> raw tables\n" + table.to_string(index=False))
    return errors


def check_raw_in_staging(con: duckdb.DuckDBPyConnection, log: TeeLogger) -> list[str]:
    """1b. Each staging model holds exactly the raw keys it should (raw minus exclusions)."""
    errors, rows = [], []
    for endpoint, spec in STAGING.items():
        raw, model, key = ENDPOINTS[endpoint]["table"], spec["model"], spec["key"]
        expected = f"with expected as (select repo, id from {raw} where {spec['keep']}) "
        missing = con.execute(expected + f"""
            select e.repo, e.id from expected e
            anti join {model} s on s.repo = e.repo and s.{key} = e.id
        """).df()
        unexpected = con.execute(expected + f"""
            select s.repo, s.{key} as id from {model} s
            anti join expected e on e.repo = s.repo and e.id = s.{key}
        """).df()
        n_raw, n_expected, n_staged = con.execute(expected + f"""
            select (select count(*) from {raw}), (select count(*) from expected),
                   (select count(*) from {model})
        """).fetchall()[0]
        rows.append((raw, n_raw, n_raw - n_expected, n_expected, model, n_staged,
                     len(missing), len(unexpected)))
        if len(missing):
            errors.append(f"{model}: {len(missing)} raw keys missing, e.g. {_examples(missing)}")
        if len(unexpected):
            errors.append(
                f"{model}: {len(unexpected)} keys not in {raw}, e.g. {_examples(unexpected)}"
            )
    table = pd.DataFrame(rows, columns=["raw", "raw_rows", "excluded", "expected", "staging",
                                        "staged_rows", "missing", "unexpected"])
    log.info("1b. Raw tables -> staging models\n" + table.to_string(index=False))
    return errors


def check_monthly_commits(con: duckdb.DuckDBPyConnection, log: TeeLogger) -> list[str]:
    """1c. Every repo has commits in every complete month of the window. A month with
    no commits is reported as a warning, not an error: the commits endpoint returns
    every commit in the window, so an empty month is a real quiet month (low activity,
    which Task 5 scores), not missing data. Checks 1a and 1b catch missing data."""
    gaps = con.execute("""
        select repo, strftime(month, '%Y-%m') as month
        from mart_repo_monthly
        where not is_partial_month and commits = 0
        order by repo, month
    """).df()
    months = con.execute(
        "select count(distinct month) from mart_repo_monthly where not is_partial_month"
    ).fetchall()[0][0]
    log.info(f"1c. Commits in every complete month: {months} complete months per repo, "
             f"{len(gaps)} repo-months with no commits")
    if len(gaps):
        listed = ", ".join(f"{r.repo} {r.month}" for r in gaps.itertuples())
        log.warning(f"Months with no commits (reported, not a failure): {listed}")
    return []


# Tukey's inner fence: a value is an outlier if it is above Q3 + 1.5 x IQR for its repo.
TUKEY_K = 1.5

# Each scanned measure: one row per record with (repo, record, value). Merge times come
# from the fact table (never recomputed); weekly commits fill empty weeks with 0 and skip
# the partial first and last weeks, so a stub week isn't compared with full weeks.
OUTLIER_MEASURES = {
    "hours_to_merge": """
        select repo, pr_key as record, hours_to_merge as value
        from fct_pull_requests
        where is_merged and not is_bot
    """,
    "weekly_commits": """
        with weeks as (
            select unnest(generate_series(
                -- first Monday on or after window_start; last Monday whose week has ended
                date_trunc('week', window_start + interval 6 day),
                date_trunc('week', run_time) - interval 7 day,
                interval 7 day
            )) as week
            from run_context
        ),
        counts as (
            select repo, date_trunc('week', committed_at) as week, count(*) as n
            from stg_commits
            group by all
        )
        select r.repo, 'week of ' || strftime(w.week, '%Y-%m-%d') as record,
               coalesce(c.n, 0)::DOUBLE as value
        from repos r
        cross join weeks w
        left join counts c on c.repo = r.repo and c.week = w.week
    """,
}


def detect_outliers(con: duckdb.DuckDBPyConnection, log: TeeLogger) -> pd.DataFrame:
    """Flag values above each repo's Tukey fence; keep them, and record them in
    validation_outliers. Medians and shares, which the health metrics use, are barely
    moved by a long tail, and a slow merge or busy week is real behavior, not bad data.
    """
    fences, flagged = [], []
    for measure, sql in OUTLIER_MEASURES.items():
        values = f"with v as ({sql}), "
        stats = con.execute(values + f"""
            q as (
                select repo, count(*) as n,
                       quantile_cont(value, 0.25) as q1,
                       quantile_cont(value, 0.75) as q3
                from v group by repo
            )
            select repo, n, q1, q3, q3 + {TUKEY_K} * (q3 - q1) as fence from q order by repo
        """).df()
        stats.insert(0, "measure", measure)
        fences.append(stats)
        flagged.append(con.execute(values + f"""
            q as (
                select repo, quantile_cont(value, 0.25) as q1, quantile_cont(value, 0.75) as q3
                from v group by repo
            )
            select '{measure}' as measure, v.repo, v.record, v.value,
                   q.q3 + {TUKEY_K} * (q.q3 - q.q1) as fence, 'keep' as treatment
            from v join q using (repo)
            where v.value > q.q3 + {TUKEY_K} * (q.q3 - q.q1)
            order by v.repo, v.value desc
        """).df())

    fences = pd.concat(fences, ignore_index=True)
    flagged = pd.concat(flagged, ignore_index=True)
    log.info(
        f"2. Outliers: value > Q3 + {TUKEY_K} x IQR, per repo (hours for merges, commits "
        f"for weeks)\n" + fences.round(1).to_string(index=False)
    )
    log.info(
        f"{len(flagged)} flagged records, all KEPT (treatment = keep):\n"
        + (flagged.round(1).to_string(index=False) if len(flagged) else "(none)")
    )
    con.register("_flagged", flagged)
    con.execute("create or replace table validation_outliers as select * from _flagged")
    con.unregister("_flagged")
    return flagged


def describe(con: duckdb.DuckDBPyConnection, log: TeeLogger) -> pd.DataFrame:
    """3. Per-repo descriptive statistics, read from the marts (durations not recomputed).

    Medians and 90th percentiles because these durations are heavily right-skewed.
    Human PRs only, matching the health metrics.
    """
    stats = con.execute("""
        with prs as (
            select
                repo,
                median(hours_to_first_review)                as median_first_review_h,
                quantile_cont(hours_to_first_review, 0.9)    as p90_first_review_h,
                median(hours_to_merge)                       as median_merge_h,
                quantile_cont(hours_to_merge, 0.9)           as p90_merge_h
            from fct_pull_requests
            where not is_bot
            group by repo
        )
        select h.repo, p.median_first_review_h, p.p90_first_review_h,
               p.median_merge_h, p.p90_merge_h, h.commits_per_week, h.releases_per_year
        from mart_health_inputs h
        left join prs p using (repo)
        order by h.repo
    """).df()
    log.info("3. Descriptive statistics per repo (hours; human PRs)\n"
             + stats.round(2).to_string(index=False))
    return stats


@task
def validate() -> dict:
    """Task 4. Completeness checks (any failure fails the flow), then outlier detection
    and descriptive statistics, which are reported but never fail the flow.
    """
    log = TeeLogger("validate")
    # Not read_only: dbt-duckdb keeps the database open in this process after Task 3, and
    # DuckDB refuses a second connection with a different configuration. Task 4 only reads.
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute("set TimeZone = 'UTC'")
        errors = (
            check_files_in_raw(con, log)
            + check_raw_in_staging(con, log)
            + check_monthly_commits(con, log)
        )
        if errors:
            for error in errors:
                log.error(error)
            raise ValidationError(
                f"{len(errors)} completeness check(s) failed: " + " | ".join(errors)
            )
        log.info("Completeness checks passed")

        flagged = detect_outliers(con, log)
        stats = describe(con, log)
    return {"outliers": len(flagged), "stats": stats.to_dict("records")}
