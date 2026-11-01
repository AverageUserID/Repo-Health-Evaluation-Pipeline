"""Register the monthly deployment of the package health flow on the class Prefect server.

Run `python deploy.py` with the uvasds profile active. Job pods clone this repo's main
branch at run time, so push before deploying or triggering a run. The repo is private:
the worker clones it with the token in the Secret block, which the flow also uses for
the GitHub API (pipeline/github.py).
"""

from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

from main import package_health_flow

GIT_URL = "https://github.com/AverageUserID/DS3022-Project2.git"
WORK_POOL = "uvasds-k8s-pool"
PAT_BLOCK = "vra7dy-pat"

if __name__ == "__main__":
    # runs midnight Eastern on the 28th of every month
    package_health_flow.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            # The block itself, not .get(): the deployment stores only a reference.
            credentials={"access_token": Secret.load(PAT_BLOCK)},
        ),
        entrypoint="main.py:package_health_flow",
    ).deploy(
        name="vra7dy-deployment",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )
