"""Task 3: run `dbt build` (staging, facts, marts, and all tests) from the flow."""

import json
import os

from prefect import task
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipeline.config import DB_PATH, PROJECT_DIR, REPOS, WINDOW_START
from pipeline.logs import TeeLogger


class DbtBuildError(Exception):
    """dbt could not build a model or a test failed; the message names the nodes."""


def dbt_vars() -> dict:
    """The same window and repos Task 1 fetched, so dbt never analyzes a different set."""
    return {"window_start": WINDOW_START, "repos": REPOS}


@task
def dbt_build() -> dict:
    """Task 3. Build every model and run every test; fail the flow if anything fails.

    Each node's result is logged before raising, so the singular tests' outcomes always
    reach the screen and logs/dbt_build.log. dbt's own log is logs/dbt.log.
    """
    log = TeeLogger("dbt_build")
    # profiles.yml reads this, so the database is found wherever the flow is started.
    os.environ["DUCKDB_PATH"] = str(DB_PATH)
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(project_dir=PROJECT_DIR, profiles_dir=PROJECT_DIR),
        raise_on_failure=False,
    )
    log.info(f"dbt build with vars {dbt_vars()}")
    result = runner.invoke(["build", "--vars", json.dumps(dbt_vars())])

    if result.result is None:
        raise DbtBuildError(f"dbt build did not run: {result.exception}")

    counts, failed = {}, []
    for node in result.result.results:
        status = str(node.status)
        counts[status] = counts.get(status, 0) + 1
        line = (
            f"{node.node.resource_type} {node.node.name}: {status.upper()} "
            f"({node.execution_time:.2f}s)"
        )
        if status in ("error", "fail"):
            failed.append(node.node.name)
            log.error(f"{line} - {node.message}")
        elif status == "warn":
            log.warning(f"{line} - {node.message}")
        else:
            log.info(line)

    log.info(f"dbt build summary: {counts}")
    if failed or not result.success:
        raise DbtBuildError(f"dbt build failed: {failed or result.exception}")
    return counts
