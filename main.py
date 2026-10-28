"""Package health pipeline: one Prefect flow running the six tasks in order."""

from prefect import flow

from pipeline.extract import extract
from pipeline.github import get_client
from pipeline.load import load
from pipeline.logs import TeeLogger
from pipeline.transform import dbt_build


@flow(name="vra7dy-package-health-flow", log_prints=True)
def package_health_flow() -> dict:
    extracted = extract()
    load()
    dbt_build()
    # Tasks 4-6 (validate, model, plot) are added here next.

    TeeLogger("flow").info(f"API odometer: {get_client().requests} requests this flow run")
    return extracted


if __name__ == "__main__":
    package_health_flow()
