"""Package health pipeline: one Prefect flow running the six tasks in order."""

from prefect import flow

from pipeline.extract import extract
from pipeline.github import get_client
from pipeline.logs import TeeLogger


@flow(name="package-health", log_prints=True)
def package_health_flow() -> dict:
    extracted = extract()
    # Tasks 2-6 (load, dbt build, validate, model, plot) are added here next.

    TeeLogger("flow").info(f"API odometer: {get_client().requests} requests this flow run")
    return extracted


if __name__ == "__main__":
    package_health_flow()
