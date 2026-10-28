"""Task 2: load landed JSON pages from data/raw/ into raw_* tables in packages.duckdb."""

import duckdb
from prefect import task
from prefect.tasks import exponential_backoff

from pipeline.config import DB_PATH, LOADED_MARKER, RAW_DIR, REPOS
from pipeline.extract import repo_dir
from pipeline.logs import TeeLogger

# Per endpoint: target table, where its pages live, and how to pull the key columns
# out of each record (`rec`). updated_at is the record's last-changed time: commits use
# the committer date (what GitHub's `since` filters on) and reviews, which have no
# updated time, use submitted_at. Ids keep their natural type: a commit's sha is text,
# everything else is a positive integer (review ids already exceed 32 bits, hence BIGINT).
# `id_valid` is the shape a well-formed id must have.
ENDPOINTS = {
    "commits": {
        "table": "raw_commits",
        "glob": "commits/page_*.json",
        "id_type": "VARCHAR",
        "id": "rec->>'sha'",
        "id_valid": "regexp_full_match(id, '[0-9a-f]{40}')",
        "updated_at": "rec->'commit'->'committer'->>'date'",
    },
    "pulls": {
        "table": "raw_pull_requests",
        "glob": "pulls/page_*.json",
        "id_type": "BIGINT",
        "id": "rec->>'number'",
        "id_valid": "id > 0",
        "updated_at": "rec->>'updated_at'",
    },
    "reviews": {
        "table": "raw_pr_reviews",
        "glob": "reviews/*/page_*.json",
        "id_type": "BIGINT",
        "id": "rec->>'id'",
        "id_valid": "id > 0",
        "updated_at": "rec->>'submitted_at'",
    },
    "releases": {
        "table": "raw_releases",
        "glob": "releases/page_*.json",
        "id_type": "BIGINT",
        "id": "rec->>'id'",
        "id_valid": "id > 0",
        "updated_at": "rec->>'updated_at'",
    },
}

# Declare the landing file's top-level fields so read_json never guesses types;
# each record stays raw JSON until we extract from it.
READ_JSON = (
    "read_json($glob, columns={repo: 'VARCHAR', fetched_at: 'TIMESTAMPTZ', data: 'JSON'}, "
    "filename=true)"
)

# How many offending records to quote in an error message.
MAX_EXAMPLES = 5


class LoadError(Exception):
    """Landed data that can't be loaded as-is; the message names the files involved."""


def stage_endpoint(con: duckdb.DuckDBPyConnection, endpoint: str) -> int:
    """Read every landed page of one endpoint, all repos, into a temp table `_stage_<endpoint>`.

    One row per record. Values are cast with try_cast and the raw text kept beside them,
    so a bad value is reported with its file instead of failing the whole statement.
    Fails if the rows staged don't match the records in the files (see check_staged).
    """
    spec, log = ENDPOINTS[endpoint], TeeLogger("load")
    stage = f"_stage_{endpoint}"
    con.execute(f"""
        create or replace temp table {stage} (
            repo VARCHAR, id {spec['id_type']}, updated_at TIMESTAMPTZ,
            fetched_at TIMESTAMPTZ, source_file VARCHAR, payload JSON,
            id_raw VARCHAR, updated_at_raw VARCHAR
        )
    """)

    for repo in REPOS:
        pattern = repo_dir(repo) / spec["glob"]
        if not any(repo_dir(repo).glob(spec["glob"])):
            log.info(f"{repo} {endpoint}: no landed files, skipping")
            continue
        try:
            con.execute(
                f"""
                insert into {stage}
                select page.repo,
                       try_cast({spec['id']} as {spec['id_type']}),
                       try_cast({spec['updated_at']} as TIMESTAMPTZ),
                       page.fetched_at,
                       page.filename,
                       rec,
                       {spec['id']},
                       {spec['updated_at']}
                from {READ_JSON} page,
                     unnest(json_extract(page.data, '$[*]')) t(rec)
                where page.repo = $repo
                """,
                {"glob": str(pattern), "repo": repo},
            )
            staged, expected, files = con.execute(
                f"""
                select (select count(*) from {stage} where repo = $repo),
                       sum(json_array_length(data)),
                       count(*)
                from {READ_JSON}
                """,
                {"glob": str(pattern), "repo": repo},
            ).fetchall()[0]
        except duckdb.Error as exc:
            raise LoadError(f"Staging {repo} {endpoint} from {pattern} failed: {exc}") from exc

        if staged != expected:
            raise LoadError(
                f"{repo} {endpoint}: staged {staged} rows but files hold {expected} records"
            )
        log.info(f"{repo} {endpoint}: staged {staged} records from {files} files")

    check_staged(con, endpoint)
    return con.execute(f"select count(*) from {stage}").fetchall()[0][0]


def check_staged(con: duckdb.DuckDBPyConnection, endpoint: str) -> None:
    """Fail with file names and raw values if any staged record has a bad key or timestamp."""
    spec, log = ENDPOINTS[endpoint], TeeLogger("load")
    stage = f"_stage_{endpoint}"
    problems = {
        "missing id": "id_raw is null",
        f"id is not a valid {spec['id_type']}": "id_raw is not null and id is null",
        "id has the wrong shape": f"id is not null and not ({spec['id_valid']})",
        "updated_at is not a timestamp": "updated_at_raw is not null and updated_at is null",
        "landing file has no fetched_at": "fetched_at is null",
    }
    errors = []
    for problem, condition in problems.items():
        rows = con.execute(f"""
            select source_file, id_raw, updated_at_raw, count(*) over ()
            from {stage} where {condition}
            order by source_file limit {MAX_EXAMPLES}
        """).fetchall()
        if rows:
            examples = "; ".join(f"{f} (id={i!r}, updated_at={u!r})" for f, i, u, _ in rows)
            errors.append(f"{rows[0][3]} records: {problem}, e.g. {examples}")
    if errors:
        raise LoadError(f"{endpoint}: " + " | ".join(errors))

    missing = con.execute(f"select count(*) from {stage} where updated_at is null").fetchall()[0][0]
    if missing:
        log.warning(f"{endpoint}: {missing} records have no updated_at (kept, value is NULL)")


def upsert_endpoint(con: duckdb.DuckDBPyConnection, endpoint: str) -> tuple[int, int]:
    """Upsert `_stage_<endpoint>` into its raw table on (repo, id). Returns row counts before/after.

    A record can be staged twice (a PR updated mid-run lands on two pages), and DuckDB
    rejects the same key twice in one INSERT OR REPLACE, so keep only its latest copy.
    """
    spec, log = ENDPOINTS[endpoint], TeeLogger("load")
    table = spec["table"]
    con.execute(f"""
        create table if not exists {table} (
            repo VARCHAR, id {spec['id_type']}, updated_at TIMESTAMPTZ,
            fetched_at TIMESTAMPTZ, source_file VARCHAR, payload JSON,
            primary key (repo, id)
        )
    """)
    before = con.execute(f"select count(*) from {table}").fetchall()[0][0]
    try:
        con.execute(f"""
            insert or replace into {table}
            select repo, id, updated_at, fetched_at, source_file, payload
            from _stage_{endpoint}
            qualify row_number() over (
                partition by repo, id
                order by fetched_at desc, updated_at desc nulls last, source_file desc
            ) = 1
        """)
    except duckdb.Error as exc:
        raise LoadError(f"Upserting {endpoint} into {table} failed: {exc}") from exc
    after = con.execute(f"select count(*) from {table}").fetchall()[0][0]
    log.info(f"{table}: {before} rows before, {after} rows after upsert")
    return before, after


def update_load_state(con: duckdb.DuckDBPyConnection) -> None:
    """Record one row per (repo, endpoint): the watermark Task 1 reads, plus row counts.

    The watermark is max(updated_at) over the WHOLE raw table, not just this load, so a run
    with nothing new keeps it. It uses only GitHub's timestamps, and a PR updated mid-run
    after its page was fetched is newer than the watermark, so the next run picks it up.
    """
    log = TeeLogger("load")
    con.execute("""
        create table if not exists _load_state (
            repo VARCHAR, endpoint VARCHAR, max_updated_at TIMESTAMPTZ,
            row_count BIGINT, staged_this_load BIGINT, loaded_at TIMESTAMPTZ,
            primary key (repo, endpoint)
        )
    """)
    for endpoint, spec in ENDPOINTS.items():
        con.execute(
            f"""
            insert or replace into _load_state
            select r.repo, $endpoint, max(r.updated_at), count(*),
                   coalesce(any_value(s.n), 0), now()
            from {spec['table']} r
            left join (select repo, count(*) n from _stage_{endpoint} group by repo) s
                using (repo)
            group by r.repo
            """,
            {"endpoint": endpoint},
        )
    for row in con.execute(
        "select repo, endpoint, max_updated_at, row_count, staged_this_load "
        "from _load_state order by repo, endpoint"
    ).fetchall():
        log.info("_load_state {} {}: watermark {}, {} rows ({} staged this load)".format(*row))


def load_all(con: duckdb.DuckDBPyConnection) -> dict:
    """Stage and upsert every endpoint in ONE transaction. Returns row counts per table.

    Any failure rolls the whole load back, including _load_state, so the raw tables
    never hold half a load and always agree with their watermarks.
    """
    if not RAW_DIR.exists():
        raise LoadError(f"{RAW_DIR} does not exist; run Task 1 first")
    counts = {}
    con.begin()
    try:
        for endpoint in ENDPOINTS:
            stage_endpoint(con, endpoint)
            before, after = upsert_endpoint(con, endpoint)
            counts[ENDPOINTS[endpoint]["table"]] = {"before": before, "after": after}
        update_load_state(con)
        con.commit()
    except Exception:
        con.rollback()
        TeeLogger("load").error("Load failed, rolled back: raw tables are unchanged")
        raise
    return counts


def _is_transient(task, task_run, state) -> bool:
    """Retry only when the database file is locked (e.g. the DuckDB CLI has it open)."""
    try:
        state.result()
    except duckdb.IOException:
        return True
    except Exception:
        return False
    return False


@task(retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=10),
      retry_condition_fn=_is_transient)
def load() -> dict:
    """Task 2. Load data/raw/ into packages.duckdb twice, then mark it loaded.

    The second pass over the same files must leave every row count unchanged, which
    proves the upsert is idempotent on every run. _LOADED is touched only after both
    passes commit and agree. A crash before then is safe: the next run resumes with
    nothing new to fetch, and reloading is idempotent.
    """
    log = TeeLogger("load")
    with duckdb.connect(str(DB_PATH)) as con:
        log.info("Load pass 1 of 2")
        first = load_all(con)
        log.info("Load pass 2 of 2 (same files, counts must not change)")
        second = load_all(con)

    changed = []
    for table, one in first.items():
        two = second[table]
        log.info(
            f"{table}: pass 1 {one['before']} -> {one['after']}, "
            f"pass 2 {two['before']} -> {two['after']}"
        )
        if two["before"] != one["after"] or two["after"] != one["after"]:
            changed.append(table)
    if changed:
        raise LoadError(f"Second load of the same files changed row counts in {changed}")

    LOADED_MARKER.touch()
    log.info(f"Load committed and idempotent, wrote {LOADED_MARKER.name} marker")
    return second


if __name__ == "__main__":
    # Standalone reload (no marker): run twice to show identical row counts.
    with duckdb.connect(str(DB_PATH)) as con:
        load_all(con)
