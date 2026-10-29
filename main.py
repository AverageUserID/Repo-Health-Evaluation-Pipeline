"""Package health pipeline: one Prefect flow running the six tasks in order."""

from prefect import flow

from pipeline.extract import extract
from pipeline.github import get_client
from pipeline.load import load
from pipeline.logs import TeeLogger
from pipeline.model import model
from pipeline.plot import plot
from pipeline.transform import dbt_build
from pipeline.validate import validate


@flow(name="vra7dy-package-health-flow", log_prints=True)
def package_health_flow() -> dict:
    extracted = extract()
    load()
    dbt_build()
    validate()
    model()
    plot()

    client = get_client()
    TeeLogger("flow").info(
        f"API odometer: {client.requests} requests ({client.points} GraphQL points) this flow run"
    )
    return extracted


if __name__ == "__main__":
    package_health_flow()
