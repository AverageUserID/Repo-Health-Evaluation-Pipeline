# Prefect Deployments

All previous in-class examples of Prefect flows have run (executed) on student laptops, while their logging and output has been visible through the SDS Prefect server https://sds-prefect.pods.uvarc.io/.

However, an advanced feature of Prefect is that it can run scheduled or "detached" flows on the server itself. 

Here's how to set that up and enable it:

## Blocks / Secrets

1. Keep your entire flow script and all related files in a single repository. You will need the repo URL as part of the deployment.
2. Any private ENV variables your script depends on (such as your `GH_PAT`) must first be published to the Prefect Server as "Blocks" (i.e. a feature that can store secret variables you can call within your code).
   1. Go to https://sds-prefect.pods.uvarc.io/v2/blocks
   2. Select "+ New storage block"
   3. Select "Secret" (a generic secret), then "Create"
   4. Give the block a unique **name**, using your computing ID, such as `nem2p-pat` (lowercase letters, numbers, and dashes only). The value should be of the "password" type and contain the actual value of your PAT.
   5. Click "Save".
3. To use a Prefect Block within your flow, use this syntax:

    ```
    from prefect.blocks.system import Secret

    my_secret = Secret.load("nem2p-pat")

    # Access the stored secret in a reusable object
    secret = my_secret.get()
    ```

## Deployments

See the [reference example](https://gist.github.com/nmagee/177f916afefbc7ae5bcfa7e213ca5dd9) for all details and setup, but the crux is found in the `__main__` handler at the bottom:

```
if __name__ == "__main__":
    # runs midnight Eastern on the 28th of every month
    <flow_function_name>.from_source(
        source=GIT_URL,
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        name="package-health",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )
```

In the reference example a GitHub Gist repository was used, but any normal GitHub repo will work, using the `https://` prefix with the `.git` suffix.

### Private repositories

If your repo is private, the Prefect worker needs your PAT to clone it. A PAT with no scopes can only read public repositories, so generate a new one that can also read private repositories, and store it in your Secret block in place of the old one:

1. Go to GitHub → Settings → Developer settings → Personal access tokens → **Tokens (classic)** → "Generate new token (classic)".
2. Set an expiration that lasts past the end of the semester (60 days).
3. Check the **`repo`** scope. No other scopes are needed.
4. Copy the token and update the value of your Secret block (e.g. `nem2p-pat`).

A fine-grained token also works if you prefer narrower access: grant it access to your project repository with **Contents: Read-only** permission.

Instead of passing `GIT_URL` as a plain string, wrap it in a `GitRepository` object and pass your Secret block as the credential:

```
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository

# be sure to use your PAT block below
if __name__ == "__main__":
    <flow_function_name>.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load("nem2p-pat")},
        ),
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        ...  # same deploy() arguments as above
    )
```

Pass the loaded `Secret` block itself, not `Secret.load(...).get()`. The deployment then stores only a reference to the block, and the worker fetches the token at run time, so the PAT is never written into the deployment's configuration.

The entrypoint should be the filename+primary flow function name, like this:

    main.py:package_health_flow

Keep the following values as written above:

- `WORK_POOL = "uvasds-k8s-pool"`
- `job_variables` and its values

To publish your deployment once written (and the entire code including your GIT_URL is correct and its content up-to-date), run this command from your project, pointing at the flow's entrypoint:

    prefect deploy ./main.py:<flow_function_name> \
        --name nem2p-deployment \
        --pool uvasds-k8s-pool

## Test your deployment

You should see your deployment listed in https://sds-prefect.pods.uvarc.io/v2/deployments

Select it to see details and any previous runs. Use the blue "Run" button in the upper-right to trigger a run, which you can then follow like any other run.

**For this project, trigger your deployment manually once.** The first scheduled run may not fall before the due date, so a manual run from the "Run" button is what you submit. Copy its log into `DEPLOYMENTS.md` along with your deployment command.

See the [NYC-Taxi-Monthly](https://sds-prefect.pods.uvarc.io/v2/deployments/deployment/987975de-1d75-48d3-810e-251d9e077b16?tab=Runs) sample deployment and try running it for yourself.

The savvy BSDS student will recognize that some outputs such as plots will not be available since they are built into the remote environment executing the code. This is okay ;^)

(In production the plots would be copied off to remote storage like S3, etc.)

---

## vra7dy deployment

**Deployment:** `vra7dy-package-health-flow/vra7dy-deployment` on `uvasds-k8s-pool`, at midnight Eastern on the 28th of every month (`Cron("0 0 28 * *", timezone="America/New_York")`), with the job variables above. The code is in [`deploy.py`](deploy.py), so `python main.py` still runs the flow locally.

- **Private repo:** job pods clone `main` with a `GitRepository` whose credential is the `vra7dy-pat` Secret block (a fine-grained token with Contents: Read-only on this repo). The flow uses the same block for the GitHub API when `GH_PAT` is unset, as it is in the pod ([`pipeline/github.py`](pipeline/github.py)).
- **No state between runs:** each pod starts from a fresh clone with no `data/raw/` or `packages.duckdb`, so every server run is a full extract from 2024-01-01 (about 4 minutes and under 100 GraphQL points with GraphQL), not an incremental one. Incremental runs are shown locally in the README. As with the plots, keeping state between server runs would mean copying `data/raw/` and the database to remote storage such as S3.

### Deployment command

Run from the project with the `uvasds` profile active, after pushing `main`:

```bash
python deploy.py
```

Output:

```
Successfully created/updated all deployments!

                            Deployments
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┓
┃ Name                                         ┃ Status  ┃ Details ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━┩
│ vra7dy-package-health-flow/vra7dy-deployment │ applied │         │
└──────────────────────────────────────────────┴─────────┴─────────┘
```

### Manually triggered run

Flow run `arboreal-kestrel` (`7027732c-03ee-40fa-94c6-90874ba2f39d`), triggered from the Run button on 2026-11-01. It finished **Completed** in 3 min 50 s (17:01:25 to 17:05:15 UTC):

- **Extract:** 175 API requests (94 GraphQL points) in 176.6 s wall time across 3 concurrent repos.
- **Load:** both passes gave the same counts.
- **dbt:** PASS=108.
- **Completeness checks:** passed, with one warning: dask had no commits in Oct 2026.
- **Ranking:** 1. scikit-learn (0.709), 2. plotly.py (0.481), 3. dask (0.380). Equal weights give the same order.

The full log below was exported from the Prefect server: time (UTC), level (20 = INFO, 30 = WARNING), and logger.

<details>
<summary>Full flow run log (click to expand)</summary>

```
17:01:14 | 20 | prefect.flow_runs.worker - Worker 'KubernetesWorker 9b5579bd-3a84-4b0b-a05a-28e2a22d6e11' submitting flow run '7027732c-03ee-40fa-94c6-90874ba2f39d'
17:01:14 | 20 | prefect.flow_runs.worker - Creating Kubernetes job...
17:01:14 | 20 | prefect.flow_runs.worker - Kubernetes job 'arboreal-kestrel-5h4ct' created in namespace 'uvasds-services'
17:01:14 | 20 | prefect.flow_runs.worker - Flow run 'arboreal-kestrel' submitted to infrastructure
17:01:19 | 20 | prefect.flow_runs - Running 1 deployment pull step(s)
17:01:19 | 20 | prefect.flow_runs - Executing deployment step: git_clone
17:01:19 | 20 | prefect.flow_runs - Deployment step 'git_clone' completed successfully
17:01:20 | 20 | prefect.flow_runs - All deployment steps completed successfully
17:01:25 | 20 | prefect.flow_runs - Beginning flow run 'arboreal-kestrel' for flow 'vra7dy-package-health-flow'
17:01:25 | 20 | prefect.task_runs - No data/raw/: full extract from 2024-01-01T00:00:00Z
17:01:25 | 20 | prefect.task_runs - PRs and reviews via graphql; 3 repos, concurrency 3
17:01:25 | 20 | prefect.task_runs - scikit-learn/scikit-learn started on ThreadPoolExecutor-1_0: commits since 2024-01-01T00:00:00Z, PRs updated since 2024-01-01T00:00:00Z
17:01:25 | 20 | prefect.task_runs - dask/dask started on ThreadPoolExecutor-1_1: commits since 2024-01-01T00:00:00Z, PRs updated since 2024-01-01T00:00:00Z
17:01:25 | 20 | prefect.task_runs - plotly/plotly.py started on ThreadPoolExecutor-1_2: commits since 2024-01-01T00:00:00Z, PRs updated since 2024-01-01T00:00:00Z
17:01:32 | 20 | prefect.task_runs - dask/dask commits: 1371 records, 14 pages (14 fetched, 0 already on disk), 14 requests
17:01:32 | 20 | prefect.task_runs - Finished in state Completed()
17:01:37 | 20 | prefect.task_runs - plotly/plotly.py commits: 2827 records, 29 pages (29 fetched, 0 already on disk), 29 requests
17:01:37 | 20 | prefect.task_runs - Finished in state Completed()
17:01:42 | 20 | prefect.task_runs - scikit-learn/scikit-learn commits: 3345 records, 34 pages (34 fetched, 0 already on disk), 34 requests
17:01:42 | 20 | prefect.task_runs - Finished in state Completed()
17:01:55 | 20 | prefect.task_runs - plotly/plotly.py pulls+reviews (graphql): 900 PRs, 9 pages (9 fetched, 0 already on disk), 1592 reviews for 684 PRs fetched, 10 requests, 10 points, 4978 points left
17:01:55 | 20 | prefect.task_runs - Finished in state Completed()
17:01:55 | 20 | prefect.task_runs - plotly/plotly.py PRs+reviews via graphql: 10 requests, 10 GraphQL points, 18.0s
17:01:56 | 20 | prefect.task_runs - plotly/plotly.py releases: 109 records, 2 pages (2 fetched, 0 already on disk), 2 requests
17:01:56 | 20 | prefect.task_runs - Finished in state Completed()
17:01:56 | 20 | prefect.task_runs - plotly/plotly.py finished in 31.5s on ThreadPoolExecutor-1_2, 41 requests
17:01:56 | 20 | prefect.task_runs - Finished in state Completed()
17:02:02 | 20 | prefect.task_runs - dask/dask pulls+reviews (graphql): 1500 PRs, 15 pages (15 fetched, 0 already on disk), 1985 reviews for 1224 PRs fetched, 15 requests, 15 points, 4974 points left
17:02:02 | 20 | prefect.task_runs - Finished in state Completed()
17:02:02 | 20 | prefect.task_runs - dask/dask PRs+reviews via graphql: 15 requests, 15 GraphQL points, 29.7s
17:02:03 | 20 | prefect.task_runs - dask/dask releases: 44 records, 1 pages (1 fetched, 0 already on disk), 1 requests
17:02:03 | 20 | prefect.task_runs - Finished in state Completed()
17:02:03 | 20 | prefect.task_runs - dask/dask finished in 37.9s on ThreadPoolExecutor-1_1, 30 requests
17:02:03 | 20 | prefect.task_runs - Finished in state Completed()
17:04:21 | 20 | prefect.task_runs - scikit-learn/scikit-learn pulls+reviews (graphql): 6400 PRs, 64 pages (64 fetched, 0 already on disk), 18258 reviews for 5207 PRs fetched, 69 requests, 69 points, 4927 points left
17:04:21 | 20 | prefect.task_runs - Finished in state Completed()
17:04:21 | 20 | prefect.task_runs - scikit-learn/scikit-learn PRs+reviews via graphql: 69 requests, 69 GraphQL points, 158.6s
17:04:21 | 20 | prefect.task_runs - scikit-learn/scikit-learn releases: 51 records, 1 pages (1 fetched, 0 already on disk), 1 requests
17:04:21 | 20 | prefect.task_runs - Finished in state Completed()
17:04:21 | 20 | prefect.task_runs - scikit-learn/scikit-learn finished in 176.5s on ThreadPoolExecutor-1_0, 104 requests
17:04:21 | 20 | prefect.task_runs - Finished in state Completed()
17:04:21 | 20 | prefect.task_runs - Extract finished: 175 API requests (94 GraphQL points); concurrency 3, wall 176.6s, sum of repo times 245.9s (1.39x)
17:04:21 | 20 | prefect.task_runs - Finished in state Completed()
17:04:21 | 20 | prefect.task_runs - Load pass 1 of 2
17:04:22 | 20 | prefect.task_runs - scikit-learn/scikit-learn commits: staged 3345 records from 34 files
17:04:22 | 20 | prefect.task_runs - dask/dask commits: staged 1371 records from 14 files
17:04:22 | 20 | prefect.task_runs - plotly/plotly.py commits: staged 2827 records from 29 files
17:04:22 | 20 | prefect.task_runs - raw_commits: 0 rows before, 7543 rows after upsert
17:04:23 | 20 | prefect.task_runs - scikit-learn/scikit-learn pulls: staged 6400 records from 64 files
17:04:23 | 20 | prefect.task_runs - dask/dask pulls: staged 1500 records from 15 files
17:04:23 | 20 | prefect.task_runs - plotly/plotly.py pulls: staged 900 records from 9 files
17:04:23 | 20 | prefect.task_runs - raw_pull_requests: 0 rows before, 8800 rows after upsert
17:04:24 | 20 | prefect.task_runs - scikit-learn/scikit-learn reviews: staged 18258 records from 5207 files
17:04:25 | 20 | prefect.task_runs - dask/dask reviews: staged 1985 records from 1224 files
17:04:25 | 20 | prefect.task_runs - plotly/plotly.py reviews: staged 1592 records from 684 files
17:04:25 | 20 | prefect.task_runs - raw_pr_reviews: 0 rows before, 21835 rows after upsert
17:04:25 | 20 | prefect.task_runs - scikit-learn/scikit-learn releases: staged 51 records from 1 files
17:04:25 | 20 | prefect.task_runs - dask/dask releases: staged 44 records from 1 files
17:04:25 | 20 | prefect.task_runs - plotly/plotly.py releases: staged 109 records from 2 files
17:04:25 | 20 | prefect.task_runs - raw_releases: 0 rows before, 204 rows after upsert
17:04:25 | 20 | prefect.task_runs - _load_state dask/dask commits: watermark 2026-09-29 10:11:02+00:00, 1371 rows (1371 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state dask/dask pulls: watermark 2026-11-01 16:32:39+00:00, 1500 rows (1500 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state dask/dask releases: watermark 2026-08-24 19:22:50+00:00, 44 rows (44 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state dask/dask reviews: watermark 2026-09-29 10:10:55+00:00, 1985 rows (1985 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state plotly/plotly.py commits: watermark 2026-10-30 22:37:41+00:00, 2827 rows (2827 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state plotly/plotly.py pulls: watermark 2026-11-01 15:26:58+00:00, 900 rows (900 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state plotly/plotly.py releases: watermark 2026-09-15 19:15:12+00:00, 109 rows (109 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state plotly/plotly.py reviews: watermark 2026-10-31 19:39:07+00:00, 1592 rows (1592 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state scikit-learn/scikit-learn commits: watermark 2026-11-01 15:58:02+00:00, 3345 rows (3345 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state scikit-learn/scikit-learn pulls: watermark 2026-11-01 16:27:09+00:00, 6400 rows (6400 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state scikit-learn/scikit-learn releases: watermark 2026-09-11 09:49:47+00:00, 51 rows (51 staged this load)
17:04:25 | 20 | prefect.task_runs - _load_state scikit-learn/scikit-learn reviews: watermark 2026-11-01 16:27:09+00:00, 18258 rows (18258 staged this load)
17:04:26 | 20 | prefect.task_runs - Load pass 2 of 2 (same files, counts must not change)
17:04:27 | 20 | prefect.task_runs - scikit-learn/scikit-learn commits: staged 3345 records from 34 files
17:04:27 | 20 | prefect.task_runs - dask/dask commits: staged 1371 records from 14 files
17:04:28 | 20 | prefect.task_runs - plotly/plotly.py commits: staged 2827 records from 29 files
17:04:28 | 20 | prefect.task_runs - raw_commits: 7543 rows before, 7543 rows after upsert
17:04:28 | 20 | prefect.task_runs - scikit-learn/scikit-learn pulls: staged 6400 records from 64 files
17:04:28 | 20 | prefect.task_runs - dask/dask pulls: staged 1500 records from 15 files
17:04:28 | 20 | prefect.task_runs - plotly/plotly.py pulls: staged 900 records from 9 files
17:04:28 | 20 | prefect.task_runs - raw_pull_requests: 8800 rows before, 8800 rows after upsert
17:04:30 | 20 | prefect.task_runs - scikit-learn/scikit-learn reviews: staged 18258 records from 5207 files
17:04:30 | 20 | prefect.task_runs - dask/dask reviews: staged 1985 records from 1224 files
17:04:30 | 20 | prefect.task_runs - plotly/plotly.py reviews: staged 1592 records from 684 files
17:04:30 | 20 | prefect.task_runs - raw_pr_reviews: 21835 rows before, 21835 rows after upsert
17:04:30 | 20 | prefect.task_runs - scikit-learn/scikit-learn releases: staged 51 records from 1 files
17:04:30 | 20 | prefect.task_runs - dask/dask releases: staged 44 records from 1 files
17:04:30 | 20 | prefect.task_runs - plotly/plotly.py releases: staged 109 records from 2 files
17:04:30 | 20 | prefect.task_runs - raw_releases: 204 rows before, 204 rows after upsert
17:04:31 | 20 | prefect.task_runs - _load_state dask/dask commits: watermark 2026-09-29 10:11:02+00:00, 1371 rows (1371 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state dask/dask pulls: watermark 2026-11-01 16:32:39+00:00, 1500 rows (1500 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state dask/dask releases: watermark 2026-08-24 19:22:50+00:00, 44 rows (44 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state dask/dask reviews: watermark 2026-09-29 10:10:55+00:00, 1985 rows (1985 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state plotly/plotly.py commits: watermark 2026-10-30 22:37:41+00:00, 2827 rows (2827 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state plotly/plotly.py pulls: watermark 2026-11-01 15:26:58+00:00, 900 rows (900 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state plotly/plotly.py releases: watermark 2026-09-15 19:15:12+00:00, 109 rows (109 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state plotly/plotly.py reviews: watermark 2026-10-31 19:39:07+00:00, 1592 rows (1592 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state scikit-learn/scikit-learn commits: watermark 2026-11-01 15:58:02+00:00, 3345 rows (3345 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state scikit-learn/scikit-learn pulls: watermark 2026-11-01 16:27:09+00:00, 6400 rows (6400 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state scikit-learn/scikit-learn releases: watermark 2026-09-11 09:49:47+00:00, 51 rows (51 staged this load)
17:04:31 | 20 | prefect.task_runs - _load_state scikit-learn/scikit-learn reviews: watermark 2026-11-01 16:27:09+00:00, 18258 rows (18258 staged this load)
17:04:32 | 20 | prefect.task_runs - raw_commits: pass 1 0 -> 7543, pass 2 7543 -> 7543
17:04:32 | 20 | prefect.task_runs - raw_pull_requests: pass 1 0 -> 8800, pass 2 8800 -> 8800
17:04:32 | 20 | prefect.task_runs - raw_pr_reviews: pass 1 0 -> 21835, pass 2 21835 -> 21835
17:04:32 | 20 | prefect.task_runs - raw_releases: pass 1 0 -> 204, pass 2 204 -> 204
17:04:32 | 20 | prefect.task_runs - Load committed and idempotent, wrote _LOADED marker
17:04:32 | 20 | prefect.task_runs - Finished in state Completed()
17:04:32 | 20 | prefect.task_runs - dbt build with vars {'window_start': '2024-01-01T00:00:00Z', 'repos': ['scikit-learn/scikit-learn', 'dask/dask', 'plotly/plotly.py']}
17:04:33 | 20 | prefect.task_runs - Running with dbt=1.12.6
17:04:33 | 20 | prefect.task_runs - Registered adapter: duckdb=1.11.0
17:04:33 | 20 | prefect.task_runs - Unable to do partial parsing because saved manifest not found. Starting full parse.
17:04:37 | 20 | prefect.task_runs - Found 10 models, 98 data tests, 5 sources, 504 macros
17:04:38 | 20 | prefect.task_runs - Concurrency: 1 threads (target='dev')
17:04:38 | 20 | prefect.task_runs - 1 of 108 START sql view model main.repos ....................................... [RUN]
17:04:38 | 20 | prefect.task_runs - 1 of 108 OK created sql view model main.repos .................................. [[32mOK[0m in 0.12s]
17:04:38 | 20 | prefect.task_runs - 2 of 108 START sql view model main.run_context ................................. [RUN]
17:04:38 | 20 | prefect.task_runs - 2 of 108 OK created sql view model main.run_context ............................ [[32mOK[0m in 0.12s]
17:04:38 | 20 | prefect.task_runs - 3 of 108 START sql view model main.stg_commits ................................. [RUN]
17:04:38 | 20 | prefect.task_runs - 3 of 108 OK created sql view model main.stg_commits ............................ [[32mOK[0m in 0.09s]
17:04:38 | 20 | prefect.task_runs - 4 of 108 START sql view model main.stg_pr_reviews .............................. [RUN]
17:04:38 | 20 | prefect.task_runs - 4 of 108 OK created sql view model main.stg_pr_reviews ......................... [[32mOK[0m in 0.08s]
17:04:38 | 20 | prefect.task_runs - 5 of 108 START sql view model main.stg_pull_requests ........................... [RUN]
17:04:38 | 20 | prefect.task_runs - 5 of 108 OK created sql view model main.stg_pull_requests ...................... [[32mOK[0m in 0.11s]
17:04:38 | 20 | prefect.task_runs - 6 of 108 START sql view model main.stg_releases ................................ [RUN]
17:04:38 | 20 | prefect.task_runs - 6 of 108 OK created sql view model main.stg_releases ........................... [[32mOK[0m in 0.15s]
17:04:38 | 20 | prefect.task_runs - 7 of 108 START test not_null_repos_repo ........................................ [RUN]
17:04:39 | 20 | prefect.task_runs - 7 of 108 PASS not_null_repos_repo .............................................. [[32mPASS[0m in 0.16s]
17:04:39 | 20 | prefect.task_runs - 8 of 108 START test unique_repos_repo .......................................... [RUN]
17:04:39 | 20 | prefect.task_runs - 8 of 108 PASS unique_repos_repo ................................................ [[32mPASS[0m in 0.18s]
17:04:39 | 20 | prefect.task_runs - 9 of 108 START test assert_run_context_one_row ................................. [RUN]
17:04:39 | 20 | prefect.task_runs - 9 of 108 PASS assert_run_context_one_row ....................................... [[32mPASS[0m in 0.18s]
17:04:39 | 20 | prefect.task_runs - 10 of 108 START test not_null_run_context_run_month ............................ [RUN]
17:04:39 | 20 | prefect.task_runs - 10 of 108 PASS not_null_run_context_run_month .................................. [[32mPASS[0m in 0.16s]
17:04:39 | 20 | prefect.task_runs - 11 of 108 START test not_null_run_context_run_time ............................. [RUN]
17:04:39 | 20 | prefect.task_runs - 11 of 108 PASS not_null_run_context_run_time ................................... [[32mPASS[0m in 0.18s]
17:04:39 | 20 | prefect.task_runs - 12 of 108 START test not_null_run_context_window_start ......................... [RUN]
17:04:39 | 20 | prefect.task_runs - 12 of 108 PASS not_null_run_context_window_start ............................... [[32mPASS[0m in 0.18s]
17:04:39 | 20 | prefect.task_runs - 13 of 108 START test not_null_stg_commits_committed_at ......................... [RUN]
17:04:40 | 20 | prefect.task_runs - 13 of 108 PASS not_null_stg_commits_committed_at ............................... [[32mPASS[0m in 0.39s]
17:04:40 | 20 | prefect.task_runs - 14 of 108 START test not_null_stg_commits_committer_at ......................... [RUN]
17:04:40 | 20 | prefect.task_runs - 14 of 108 PASS not_null_stg_commits_committer_at ............................... [[32mPASS[0m in 0.30s]
17:04:40 | 20 | prefect.task_runs - 15 of 108 START test not_null_stg_commits_is_bot ............................... [RUN]
17:04:40 | 20 | prefect.task_runs - Finished in state Completed()
17:04:41 | 20 | prefect.task_runs - 15 of 108 PASS not_null_stg_commits_is_bot ..................................... [[32mPASS[0m in 0.42s]
17:04:41 | 20 | prefect.task_runs - 16 of 108 START test not_null_stg_commits_repo ................................. [RUN]
17:04:41 | 20 | prefect.task_runs - Finished in state Completed()
17:04:41 | 20 | prefect.task_runs - Finished in state Completed()
17:04:41 | 20 | prefect.task_runs - Finished in state Completed()
17:04:41 | 20 | prefect.task_runs - Finished in state Completed()
17:04:41 | 20 | prefect.task_runs - 16 of 108 PASS not_null_stg_commits_repo ....................................... [[32mPASS[0m in 0.31s]
17:04:41 | 20 | prefect.task_runs - 17 of 108 START test not_null_stg_commits_sha .................................. [RUN]
17:04:41 | 20 | prefect.task_runs - Finished in state Completed()
17:04:41 | 20 | prefect.task_runs - 17 of 108 PASS not_null_stg_commits_sha ........................................ [[32mPASS[0m in 0.24s]
17:04:41 | 20 | prefect.task_runs - 18 of 108 START test unique_together_stg_commits_repo__sha ..................... [RUN]
17:04:41 | 20 | prefect.task_runs - Finished in state Completed()
17:04:41 | 20 | prefect.task_runs - 18 of 108 PASS unique_together_stg_commits_repo__sha ........................... [[32mPASS[0m in 0.25s]
17:04:41 | 20 | prefect.task_runs - 19 of 108 START test accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED  [RUN]
17:04:41 | 20 | prefect.task_runs - Finished in state Completed()
17:04:42 | 20 | prefect.task_runs - Finished in state Completed()
17:04:42 | 20 | prefect.task_runs - 19 of 108 PASS accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED  [[32mPASS[0m in 0.28s]
17:04:42 | 20 | prefect.task_runs - 20 of 108 START test not_null_stg_pr_reviews_is_bot ............................ [RUN]
17:04:42 | 20 | prefect.task_runs - Finished in state Completed()
17:04:42 | 20 | prefect.task_runs - 20 of 108 PASS not_null_stg_pr_reviews_is_bot .................................. [[32mPASS[0m in 0.24s]
17:04:42 | 20 | prefect.task_runs - 21 of 108 START test not_null_stg_pr_reviews_pr_key ............................ [RUN]
17:04:42 | 20 | prefect.task_runs - Finished in state Completed()
17:04:42 | 20 | prefect.task_runs - 21 of 108 PASS not_null_stg_pr_reviews_pr_key .................................. [[32mPASS[0m in 0.20s]
17:04:42 | 20 | prefect.task_runs - 22 of 108 START test not_null_stg_pr_reviews_pr_number ......................... [RUN]
17:04:42 | 20 | prefect.task_runs - Finished in state Completed()
17:04:42 | 20 | prefect.task_runs - 22 of 108 PASS not_null_stg_pr_reviews_pr_number ............................... [[32mPASS[0m in 0.14s]
17:04:42 | 20 | prefect.task_runs - 23 of 108 START test not_null_stg_pr_reviews_repo .............................. [RUN]
17:04:42 | 20 | prefect.task_runs - Finished in state Completed()
17:04:43 | 20 | prefect.task_runs - 23 of 108 PASS not_null_stg_pr_reviews_repo .................................... [[32mPASS[0m in 0.28s]
17:04:43 | 20 | prefect.task_runs - 24 of 108 START test not_null_stg_pr_reviews_review_id ......................... [RUN]
17:04:43 | 20 | prefect.task_runs - 24 of 108 PASS not_null_stg_pr_reviews_review_id ............................... [[32mPASS[0m in 0.14s]
17:04:43 | 20 | prefect.task_runs - 25 of 108 START test not_null_stg_pr_reviews_state ............................. [RUN]
17:04:43 | 20 | prefect.task_runs - 25 of 108 PASS not_null_stg_pr_reviews_state ................................... [[32mPASS[0m in 0.18s]
17:04:43 | 20 | prefect.task_runs - Finished in state Completed()
17:04:43 | 20 | prefect.task_runs - 26 of 108 START test not_null_stg_pr_reviews_submitted_at ...................... [RUN]
17:04:43 | 20 | prefect.task_runs - 26 of 108 PASS not_null_stg_pr_reviews_submitted_at ............................ [[32mPASS[0m in 0.27s]
17:04:43 | 20 | prefect.task_runs - 27 of 108 START test unique_stg_pr_reviews_review_id ........................... [RUN]
17:04:43 | 20 | prefect.task_runs - Finished in state Completed()
17:04:43 | 20 | prefect.task_runs - Finished in state Completed()
17:04:43 | 20 | prefect.task_runs - 27 of 108 PASS unique_stg_pr_reviews_review_id ................................. [[32mPASS[0m in 0.17s]
17:04:43 | 20 | prefect.task_runs - 28 of 108 START test accepted_values_stg_pull_requests_state__open__closed ..... [RUN]
17:04:43 | 20 | prefect.task_runs - Finished in state Completed()
17:04:44 | 20 | prefect.task_runs - 28 of 108 PASS accepted_values_stg_pull_requests_state__open__closed ........... [[32mPASS[0m in 0.24s]
17:04:44 | 20 | prefect.task_runs - 29 of 108 START test assert_pr_closed_after_created ............................ [RUN]
17:04:44 | 20 | prefect.task_runs - 29 of 108 PASS assert_pr_closed_after_created .................................. [[32mPASS[0m in 0.16s]
17:04:44 | 20 | prefect.task_runs - 30 of 108 START test assert_review_after_pr_created ............................ [RUN]
17:04:44 | 20 | prefect.task_runs - Finished in state Completed()
17:04:44 | 20 | prefect.task_runs - 30 of 108 PASS assert_review_after_pr_created .................................. [[32mPASS[0m in 0.32s]
17:04:44 | 20 | prefect.task_runs - 31 of 108 START test not_null_stg_pull_requests_created_at ..................... [RUN]
17:04:44 | 20 | prefect.task_runs - 31 of 108 PASS not_null_stg_pull_requests_created_at ........................... [[32mPASS[0m in 0.22s]
17:04:44 | 20 | prefect.task_runs - 32 of 108 START test not_null_stg_pull_requests_is_bot ......................... [RUN]
17:04:45 | 20 | prefect.task_runs - 32 of 108 PASS not_null_stg_pull_requests_is_bot ............................... [[32mPASS[0m in 0.17s]
17:04:45 | 20 | prefect.task_runs - 33 of 108 START test not_null_stg_pull_requests_number ......................... [RUN]
17:04:45 | 20 | prefect.task_runs - Finished in state Completed()
17:04:45 | 20 | prefect.task_runs - Finished in state Completed()
17:04:45 | 20 | prefect.task_runs - Finished in state Completed()
17:04:45 | 20 | prefect.task_runs - 33 of 108 PASS not_null_stg_pull_requests_number ............................... [[32mPASS[0m in 0.25s]
17:04:45 | 20 | prefect.task_runs - 34 of 108 START test not_null_stg_pull_requests_pr_key ......................... [RUN]
17:04:45 | 20 | prefect.task_runs - Finished in state Completed()
17:04:45 | 20 | prefect.task_runs - 34 of 108 PASS not_null_stg_pull_requests_pr_key ............................... [[32mPASS[0m in 0.32s]
17:04:45 | 20 | prefect.task_runs - 35 of 108 START test not_null_stg_pull_requests_repo ........................... [RUN]
17:04:45 | 20 | prefect.task_runs - Finished in state Completed()
17:04:45 | 20 | prefect.task_runs - 35 of 108 PASS not_null_stg_pull_requests_repo ................................. [[32mPASS[0m in 0.25s]
17:04:45 | 20 | prefect.task_runs - 36 of 108 START test not_null_stg_pull_requests_state .......................... [RUN]
17:04:46 | 20 | prefect.task_runs - Finished in state Completed()
17:04:46 | 20 | prefect.task_runs - 36 of 108 PASS not_null_stg_pull_requests_state ................................ [[32mPASS[0m in 0.17s]
17:04:46 | 20 | prefect.task_runs - 37 of 108 START test not_null_stg_pull_requests_updated_at ..................... [RUN]
17:04:46 | 20 | prefect.task_runs - 37 of 108 PASS not_null_stg_pull_requests_updated_at ........................... [[32mPASS[0m in 0.25s]
17:04:46 | 20 | prefect.task_runs - 38 of 108 START test relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_  [RUN]
17:04:46 | 20 | prefect.task_runs - 38 of 108 PASS relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_  [[32mPASS[0m in 0.19s]
17:04:46 | 20 | prefect.task_runs - 39 of 108 START test unique_stg_pull_requests_pr_key ........................... [RUN]
17:04:46 | 20 | prefect.task_runs - Finished in state Completed()
17:04:46 | 20 | prefect.task_runs - 39 of 108 PASS unique_stg_pull_requests_pr_key ................................. [[32mPASS[0m in 0.18s]
17:04:46 | 20 | prefect.task_runs - 40 of 108 START test not_null_stg_releases_prerelease .......................... [RUN]
17:04:46 | 20 | prefect.task_runs - Finished in state Completed()
17:04:46 | 20 | prefect.task_runs - Finished in state Completed()
17:04:46 | 20 | prefect.task_runs - 40 of 108 PASS not_null_stg_releases_prerelease ................................ [[32mPASS[0m in 0.26s]
17:04:46 | 20 | prefect.task_runs - 41 of 108 START test not_null_stg_releases_published_at ........................ [RUN]
17:04:47 | 20 | prefect.task_runs - Finished in state Completed()
17:04:47 | 20 | prefect.task_runs - 41 of 108 PASS not_null_stg_releases_published_at .............................. [[32mPASS[0m in 0.19s]
17:04:47 | 20 | prefect.task_runs - 42 of 108 START test not_null_stg_releases_release_id .......................... [RUN]
17:04:47 | 20 | prefect.task_runs - 42 of 108 PASS not_null_stg_releases_release_id ................................ [[32mPASS[0m in 0.21s]
17:04:47 | 20 | prefect.task_runs - 43 of 108 START test not_null_stg_releases_repo ................................ [RUN]
17:04:47 | 20 | prefect.task_runs - Finished in state Completed()
17:04:47 | 20 | prefect.task_runs - 43 of 108 PASS not_null_stg_releases_repo ...................................... [[32mPASS[0m in 0.24s]
17:04:47 | 20 | prefect.task_runs - 44 of 108 START test unique_stg_releases_release_id ............................ [RUN]
17:04:47 | 20 | prefect.task_runs - Finished in state Completed()
17:04:47 | 20 | prefect.task_runs - 44 of 108 PASS unique_stg_releases_release_id .................................. [[32mPASS[0m in 0.33s]
17:04:47 | 20 | prefect.task_runs - 45 of 108 START sql table model main.fct_pull_requests ......................... [RUN]
17:04:48 | 20 | prefect.task_runs - Finished in state Completed()
17:04:48 | 20 | prefect.task_runs - Finished in state Completed()
17:04:48 | 20 | prefect.task_runs - Finished in state Completed()
17:04:48 | 20 | prefect.task_runs - Finished in state Completed()
17:04:48 | 20 | prefect.task_runs - 45 of 108 OK created sql table model main.fct_pull_requests .................... [[32mOK[0m in 0.69s]
17:04:48 | 20 | prefect.task_runs - 46 of 108 START sql table model main.fct_releases .............................. [RUN]
17:04:49 | 20 | prefect.task_runs - 46 of 108 OK created sql table model main.fct_releases ......................... [[32mOK[0m in 0.33s]
17:04:49 | 20 | prefect.task_runs - 47 of 108 START test accepted_values_fct_pull_requests_state__open__closed ..... [RUN]
17:04:49 | 20 | prefect.task_runs - Finished in state Completed()
17:04:49 | 20 | prefect.task_runs - 47 of 108 PASS accepted_values_fct_pull_requests_state__open__closed ........... [[32mPASS[0m in 0.26s]
17:04:49 | 20 | prefect.task_runs - 48 of 108 START test non_negative_fct_pull_requests_hours_to_close ............. [RUN]
17:04:49 | 20 | prefect.task_runs - Finished in state Completed()
17:04:49 | 20 | prefect.task_runs - 48 of 108 PASS non_negative_fct_pull_requests_hours_to_close ................... [[32mPASS[0m in 0.18s]
17:04:49 | 20 | prefect.task_runs - 49 of 108 START test non_negative_fct_pull_requests_hours_to_first_review ...... [RUN]
17:04:49 | 20 | prefect.task_runs - Finished in state Completed()
17:04:49 | 20 | prefect.task_runs - Finished in state Completed()
17:04:50 | 20 | prefect.task_runs - 49 of 108 PASS non_negative_fct_pull_requests_hours_to_first_review ............ [[32mPASS[0m in 0.50s]
17:04:50 | 20 | prefect.task_runs - 50 of 108 START test non_negative_fct_pull_requests_hours_to_merge ............. [RUN]
17:04:50 | 20 | prefect.task_runs - 50 of 108 PASS non_negative_fct_pull_requests_hours_to_merge ................... [[32mPASS[0m in 0.21s]
17:04:50 | 20 | prefect.task_runs - 51 of 108 START test not_null_fct_pull_requests_created_at ..................... [RUN]
17:04:50 | 20 | prefect.task_runs - Finished in state Completed()
17:04:50 | 20 | prefect.task_runs - 51 of 108 PASS not_null_fct_pull_requests_created_at ........................... [[32mPASS[0m in 0.13s]
17:04:50 | 20 | prefect.task_runs - 52 of 108 START test not_null_fct_pull_requests_has_approving_review ........... [RUN]
17:04:50 | 20 | prefect.task_runs - Finished in state Completed()
17:04:50 | 20 | prefect.task_runs - Finished in state Completed()
17:04:50 | 20 | prefect.task_runs - 52 of 108 PASS not_null_fct_pull_requests_has_approving_review ................. [[32mPASS[0m in 0.30s]
17:04:50 | 20 | prefect.task_runs - 53 of 108 START test not_null_fct_pull_requests_is_bot ......................... [RUN]
17:04:50 | 20 | prefect.task_runs - Finished in state Completed()
17:04:50 | 20 | prefect.task_runs - Finished in state Completed()
17:04:50 | 20 | prefect.task_runs - Finished in state Completed()
17:04:50 | 20 | prefect.task_runs - Finished in state Completed()
17:04:50 | 20 | prefect.task_runs - 53 of 108 PASS not_null_fct_pull_requests_is_bot ............................... [[32mPASS[0m in 0.26s]
17:04:50 | 20 | prefect.task_runs - 54 of 108 START test not_null_fct_pull_requests_is_closed_unmerged ............. [RUN]
17:04:51 | 20 | prefect.task_runs - 54 of 108 PASS not_null_fct_pull_requests_is_closed_unmerged ................... [[32mPASS[0m in 0.19s]
17:04:51 | 20 | prefect.task_runs - 55 of 108 START test not_null_fct_pull_requests_is_merged ...................... [RUN]
17:04:51 | 20 | prefect.task_runs - 55 of 108 PASS not_null_fct_pull_requests_is_merged ............................ [[32mPASS[0m in 0.21s]
17:04:51 | 20 | prefect.task_runs - 56 of 108 START test not_null_fct_pull_requests_is_open ........................ [RUN]
17:04:51 | 20 | prefect.task_runs - 56 of 108 PASS not_null_fct_pull_requests_is_open .............................. [[32mPASS[0m in 0.11s]
17:04:51 | 20 | prefect.task_runs - 57 of 108 START test not_null_fct_pull_requests_is_stale ....................... [RUN]
17:04:51 | 20 | prefect.task_runs - 57 of 108 PASS not_null_fct_pull_requests_is_stale ............................. [[32mPASS[0m in 0.14s]
17:04:51 | 20 | prefect.task_runs - 58 of 108 START test not_null_fct_pull_requests_number ......................... [RUN]
17:04:51 | 20 | prefect.task_runs - 58 of 108 PASS not_null_fct_pull_requests_number ............................... [[32mPASS[0m in 0.16s]
17:04:51 | 20 | prefect.task_runs - 59 of 108 START test not_null_fct_pull_requests_pr_key ......................... [RUN]
17:04:51 | 20 | prefect.task_runs - Finished in state Completed()
17:04:51 | 20 | prefect.task_runs - 59 of 108 PASS not_null_fct_pull_requests_pr_key ............................... [[32mPASS[0m in 0.24s]
17:04:52 | 20 | prefect.task_runs - 60 of 108 START test not_null_fct_pull_requests_repo ........................... [RUN]
17:04:52 | 20 | prefect.task_runs - Finished in state Completed()
17:04:52 | 20 | prefect.task_runs - 60 of 108 PASS not_null_fct_pull_requests_repo ................................. [[32mPASS[0m in 0.32s]
17:04:52 | 20 | prefect.task_runs - 61 of 108 START test not_null_fct_pull_requests_reviewed_within_7d ............. [RUN]
17:04:52 | 20 | prefect.task_runs - 61 of 108 PASS not_null_fct_pull_requests_reviewed_within_7d ................... [[32mPASS[0m in 0.20s]
17:04:52 | 20 | prefect.task_runs - 62 of 108 START test not_null_fct_pull_requests_state .......................... [RUN]
17:04:52 | 20 | prefect.task_runs - Finished in state Completed()
17:04:52 | 20 | prefect.task_runs - Finished in state Completed()
17:04:52 | 20 | prefect.task_runs - 62 of 108 PASS not_null_fct_pull_requests_state ................................ [[32mPASS[0m in 0.26s]
17:04:52 | 20 | prefect.task_runs - 63 of 108 START test not_null_fct_pull_requests_updated_at ..................... [RUN]
17:04:53 | 20 | prefect.task_runs - 63 of 108 PASS not_null_fct_pull_requests_updated_at ........................... [[32mPASS[0m in 0.22s]
17:04:53 | 20 | prefect.task_runs - 64 of 108 START test unique_fct_pull_requests_pr_key ........................... [RUN]
17:04:53 | 20 | prefect.task_runs - Finished in state Completed()
17:04:53 | 20 | prefect.task_runs - Finished in state Completed()
17:04:53 | 20 | prefect.task_runs - 64 of 108 PASS unique_fct_pull_requests_pr_key ................................. [[32mPASS[0m in 0.16s]
17:04:53 | 20 | prefect.task_runs - 65 of 108 START test non_negative_fct_releases_days_since_previous ............. [RUN]
17:04:53 | 20 | prefect.task_runs - 65 of 108 PASS non_negative_fct_releases_days_since_previous ................... [[32mPASS[0m in 0.19s]
17:04:53 | 20 | prefect.task_runs - 66 of 108 START test not_null_fct_releases_is_prerelease ....................... [RUN]
17:04:53 | 20 | prefect.task_runs - Finished in state Completed()
17:04:53 | 20 | prefect.task_runs - 66 of 108 PASS not_null_fct_releases_is_prerelease ............................. [[32mPASS[0m in 0.24s]
17:04:53 | 20 | prefect.task_runs - 67 of 108 START test not_null_fct_releases_published_at ........................ [RUN]
17:04:53 | 20 | prefect.task_runs - Finished in state Completed()
17:04:54 | 20 | prefect.task_runs - 67 of 108 PASS not_null_fct_releases_published_at .............................. [[32mPASS[0m in 0.32s]
17:04:54 | 20 | prefect.task_runs - 68 of 108 START test not_null_fct_releases_release_id .......................... [RUN]
17:04:54 | 20 | prefect.task_runs - 68 of 108 PASS not_null_fct_releases_release_id ................................ [[32mPASS[0m in 0.13s]
17:04:54 | 20 | prefect.task_runs - 69 of 108 START test not_null_fct_releases_repo ................................ [RUN]
17:04:54 | 20 | prefect.task_runs - 69 of 108 PASS not_null_fct_releases_repo ...................................... [[32mPASS[0m in 0.18s]
17:04:54 | 20 | prefect.task_runs - 70 of 108 START test unique_fct_releases_release_id ............................ [RUN]
17:04:54 | 20 | prefect.task_runs - Finished in state Completed()
17:04:54 | 20 | prefect.task_runs - 70 of 108 PASS unique_fct_releases_release_id .................................. [[32mPASS[0m in 0.23s]
17:04:54 | 20 | prefect.task_runs - 71 of 108 START sql table model main.mart_repo_monthly ......................... [RUN]
17:04:54 | 20 | prefect.task_runs - Finished in state Completed()
17:04:54 | 20 | prefect.task_runs - Finished in state Completed()
17:04:54 | 20 | prefect.task_runs - 71 of 108 OK created sql table model main.mart_repo_monthly .................... [[32mOK[0m in 0.46s]
17:04:55 | 20 | prefect.task_runs - Finished in state Completed()
17:04:55 | 20 | prefect.task_runs - 72 of 108 START test between_mart_repo_monthly_merge_ratio__1__0 ............... [RUN]
17:04:55 | 20 | prefect.task_runs - Finished in state Completed()
17:04:55 | 20 | prefect.task_runs - 72 of 108 PASS between_mart_repo_monthly_merge_ratio__1__0 ..................... [[32mPASS[0m in 0.20s]
17:04:55 | 20 | prefect.task_runs - 73 of 108 START test non_negative_mart_repo_monthly_active_contributors ........ [RUN]
17:04:55 | 20 | prefect.task_runs - 73 of 108 PASS non_negative_mart_repo_monthly_active_contributors .............. [[32mPASS[0m in 0.24s]
17:04:55 | 20 | prefect.task_runs - 74 of 108 START test non_negative_mart_repo_monthly_commits .................... [RUN]
17:04:55 | 20 | prefect.task_runs - Finished in state Completed()
17:04:55 | 20 | prefect.task_runs - 74 of 108 PASS non_negative_mart_repo_monthly_commits .......................... [[32mPASS[0m in 0.22s]
17:04:55 | 20 | prefect.task_runs - 75 of 108 START test non_negative_mart_repo_monthly_median_first_review_hours .. [RUN]
17:04:55 | 20 | prefect.task_runs - Finished in state Completed()
17:04:56 | 20 | prefect.task_runs - 75 of 108 PASS non_negative_mart_repo_monthly_median_first_review_hours ........ [[32mPASS[0m in 0.30s]
17:04:56 | 20 | prefect.task_runs - Finished in state Completed()
17:04:56 | 20 | prefect.task_runs - 76 of 108 START test non_negative_mart_repo_monthly_median_merge_hours ......... [RUN]
17:04:56 | 20 | prefect.task_runs - Finished in state Completed()
17:04:56 | 20 | prefect.task_runs - 76 of 108 PASS non_negative_mart_repo_monthly_median_merge_hours ............... [[32mPASS[0m in 0.29s]
17:04:56 | 20 | prefect.task_runs - 77 of 108 START test non_negative_mart_repo_monthly_month_index ................ [RUN]
17:04:56 | 20 | prefect.task_runs - 77 of 108 PASS non_negative_mart_repo_monthly_month_index ...................... [[32mPASS[0m in 0.23s]
17:04:56 | 20 | prefect.task_runs - 78 of 108 START test non_negative_mart_repo_monthly_open_backlog_end ........... [RUN]
17:04:56 | 20 | prefect.task_runs - Finished in state Completed()
17:04:56 | 20 | prefect.task_runs - Finished in state Completed()
17:04:57 | 20 | prefect.task_runs - 78 of 108 PASS non_negative_mart_repo_monthly_open_backlog_end ................. [[32mPASS[0m in 0.29s]
17:04:57 | 20 | prefect.task_runs - 79 of 108 START test non_negative_mart_repo_monthly_prs_closed_unmerged ........ [RUN]
17:04:57 | 20 | prefect.task_runs - Finished in state Completed()
17:04:57 | 20 | prefect.task_runs - 79 of 108 PASS non_negative_mart_repo_monthly_prs_closed_unmerged .............. [[32mPASS[0m in 0.26s]
17:04:57 | 20 | prefect.task_runs - 80 of 108 START test non_negative_mart_repo_monthly_prs_merged ................. [RUN]
17:04:57 | 20 | prefect.task_runs - 80 of 108 PASS non_negative_mart_repo_monthly_prs_merged ....................... [[32mPASS[0m in 0.22s]
17:04:57 | 20 | prefect.task_runs - 81 of 108 START test non_negative_mart_repo_monthly_prs_opened ................. [RUN]
17:04:57 | 20 | prefect.task_runs - Finished in state Completed()
17:04:57 | 20 | prefect.task_runs - Finished in state Completed()
17:04:57 | 20 | prefect.task_runs - 81 of 108 PASS non_negative_mart_repo_monthly_prs_opened ....................... [[32mPASS[0m in 0.28s]
17:04:57 | 20 | prefect.task_runs - 82 of 108 START test not_null_mart_repo_monthly_active_contributors ............ [RUN]
17:04:57 | 20 | prefect.task_runs - Finished in state Completed()
17:04:57 | 20 | prefect.task_runs - 82 of 108 PASS not_null_mart_repo_monthly_active_contributors .................. [[32mPASS[0m in 0.13s]
17:04:57 | 20 | prefect.task_runs - 83 of 108 START test not_null_mart_repo_monthly_commits ........................ [RUN]
17:04:58 | 20 | prefect.task_runs - 83 of 108 PASS not_null_mart_repo_monthly_commits .............................. [[32mPASS[0m in 0.17s]
17:04:58 | 20 | prefect.task_runs - 84 of 108 START test not_null_mart_repo_monthly_is_partial_month ............... [RUN]
17:04:58 | 20 | prefect.task_runs - Finished in state Completed()
17:04:58 | 20 | prefect.task_runs - 84 of 108 PASS not_null_mart_repo_monthly_is_partial_month ..................... [[32mPASS[0m in 0.21s]
17:04:58 | 20 | prefect.task_runs - 85 of 108 START test not_null_mart_repo_monthly_month .......................... [RUN]
17:04:58 | 20 | prefect.task_runs - Finished in state Completed()
17:04:58 | 20 | prefect.task_runs - Finished in state Completed()
17:04:58 | 20 | prefect.task_runs - 85 of 108 PASS not_null_mart_repo_monthly_month ................................ [[32mPASS[0m in 0.36s]
17:04:58 | 20 | prefect.task_runs - 86 of 108 START test not_null_mart_repo_monthly_month_index .................... [RUN]
17:04:58 | 20 | prefect.task_runs - 86 of 108 PASS not_null_mart_repo_monthly_month_index .......................... [[32mPASS[0m in 0.13s]
17:04:58 | 20 | prefect.task_runs - 87 of 108 START test not_null_mart_repo_monthly_open_backlog_end ............... [RUN]
17:04:59 | 20 | prefect.task_runs - 87 of 108 PASS not_null_mart_repo_monthly_open_backlog_end ..................... [[32mPASS[0m in 0.19s]
17:04:59 | 20 | prefect.task_runs - 88 of 108 START test not_null_mart_repo_monthly_prs_closed_unmerged ............ [RUN]
17:04:59 | 20 | prefect.task_runs - 88 of 108 PASS not_null_mart_repo_monthly_prs_closed_unmerged .................. [[32mPASS[0m in 0.26s]
17:04:59 | 20 | prefect.task_runs - 89 of 108 START test not_null_mart_repo_monthly_prs_merged ..................... [RUN]
17:04:59 | 20 | prefect.task_runs - Finished in state Completed()
17:04:59 | 20 | prefect.task_runs - 89 of 108 PASS not_null_mart_repo_monthly_prs_merged ........................... [[32mPASS[0m in 0.14s]
17:04:59 | 20 | prefect.task_runs - 90 of 108 START test not_null_mart_repo_monthly_prs_opened ..................... [RUN]
17:04:59 | 20 | prefect.task_runs - Finished in state Completed()
17:04:59 | 20 | prefect.task_runs - 90 of 108 PASS not_null_mart_repo_monthly_prs_opened ........................... [[32mPASS[0m in 0.29s]
17:04:59 | 20 | prefect.task_runs - 91 of 108 START test not_null_mart_repo_monthly_repo ........................... [RUN]
17:04:59 | 20 | prefect.task_runs - Finished in state Completed()
17:04:59 | 20 | prefect.task_runs - Finished in state Completed()
17:05:00 | 20 | prefect.task_runs - Finished in state Completed()
17:05:00 | 20 | prefect.task_runs - 91 of 108 PASS not_null_mart_repo_monthly_repo ................................. [[32mPASS[0m in 0.28s]
17:05:00 | 20 | prefect.task_runs - 92 of 108 START test unique_together_mart_repo_monthly_repo__month ............. [RUN]
17:05:00 | 20 | prefect.task_runs - Finished in state Completed()
17:05:00 | 20 | prefect.task_runs - 92 of 108 PASS unique_together_mart_repo_monthly_repo__month ................... [[32mPASS[0m in 0.30s]
17:05:00 | 20 | prefect.task_runs - 93 of 108 START sql table model main.mart_health_inputs ........................ [RUN]
17:05:00 | 20 | prefect.task_runs - Finished in state Completed()
17:05:00 | 20 | prefect.task_runs - Finished in state Completed()
17:05:00 | 20 | prefect.task_runs - 93 of 108 OK created sql table model main.mart_health_inputs ................... [[32mOK[0m in 0.45s]
17:05:00 | 20 | prefect.task_runs - 94 of 108 START test between_mart_health_inputs_merge_rate__1__0 ............... [RUN]
17:05:01 | 20 | prefect.task_runs - Finished in state Completed()
17:05:01 | 20 | prefect.task_runs - Finished in state Completed()
17:05:01 | 20 | prefect.task_runs - 94 of 108 PASS between_mart_health_inputs_merge_rate__1__0 ..................... [[32mPASS[0m in 0.23s]
17:05:01 | 20 | prefect.task_runs - 95 of 108 START test between_mart_health_inputs_merged_per_opened__1__0 ........ [RUN]
17:05:01 | 20 | prefect.task_runs - Finished in state Completed()
17:05:01 | 20 | prefect.task_runs - 95 of 108 PASS between_mart_health_inputs_merged_per_opened__1__0 .............. [[32mPASS[0m in 0.18s]
17:05:01 | 20 | prefect.task_runs - 96 of 108 START test between_mart_health_inputs_pct_merged_with_approval__1__0 . [RUN]
17:05:01 | 20 | prefect.task_runs - 96 of 108 PASS between_mart_health_inputs_pct_merged_with_approval__1__0 ....... [[32mPASS[0m in 0.12s]
17:05:01 | 20 | prefect.task_runs - 97 of 108 START test between_mart_health_inputs_pct_open_stale__1__0 ........... [RUN]
17:05:01 | 20 | prefect.task_runs - Finished in state Completed()
17:05:01 | 20 | prefect.task_runs - 97 of 108 PASS between_mart_health_inputs_pct_open_stale__1__0 ................. [[32mPASS[0m in 0.16s]
17:05:01 | 20 | prefect.task_runs - 98 of 108 START test between_mart_health_inputs_pct_reviewed_within_7d__1__0 ... [RUN]
17:05:01 | 20 | prefect.task_runs - Finished in state Completed()
17:05:01 | 20 | prefect.task_runs - 98 of 108 PASS between_mart_health_inputs_pct_reviewed_within_7d__1__0 ......... [[32mPASS[0m in 0.24s]
17:05:01 | 20 | prefect.task_runs - 99 of 108 START test between_mart_health_inputs_top_author_share__1__0 ......... [RUN]
17:05:01 | 20 | prefect.task_runs - Finished in state Completed()
17:05:02 | 20 | prefect.task_runs - 99 of 108 PASS between_mart_health_inputs_top_author_share__1__0 ............... [[32mPASS[0m in 0.17s]
17:05:02 | 20 | prefect.task_runs - 100 of 108 START test non_negative_mart_health_inputs_active_authors_per_month . [RUN]
17:05:02 | 20 | prefect.task_runs - 100 of 108 PASS non_negative_mart_health_inputs_active_authors_per_month ....... [[32mPASS[0m in 0.29s]
17:05:02 | 20 | prefect.task_runs - 101 of 108 START test non_negative_mart_health_inputs_bus_factor ............... [RUN]
17:05:02 | 20 | prefect.task_runs - Finished in state Completed()
17:05:02 | 20 | prefect.task_runs - 101 of 108 PASS non_negative_mart_health_inputs_bus_factor ..................... [[32mPASS[0m in 0.29s]
17:05:02 | 20 | prefect.task_runs - 102 of 108 START test non_negative_mart_health_inputs_commits_per_week ......... [RUN]
17:05:02 | 20 | prefect.task_runs - Finished in state Completed()
17:05:02 | 20 | prefect.task_runs - Finished in state Completed()
17:05:02 | 20 | prefect.task_runs - 102 of 108 PASS non_negative_mart_health_inputs_commits_per_week ............... [[32mPASS[0m in 0.20s]
17:05:02 | 20 | prefect.task_runs - 103 of 108 START test non_negative_mart_health_inputs_median_hours_to_first_review  [RUN]
17:05:03 | 20 | prefect.task_runs - Finished in state Completed()
17:05:03 | 20 | prefect.task_runs - Finished in state Completed()
17:05:03 | 20 | prefect.task_runs - 103 of 108 PASS non_negative_mart_health_inputs_median_hours_to_first_review ... [[32mPASS[0m in 0.26s]
17:05:03 | 20 | prefect.task_runs - 104 of 108 START test non_negative_mart_health_inputs_median_hours_to_merge .... [RUN]
17:05:03 | 20 | prefect.task_runs - 104 of 108 PASS non_negative_mart_health_inputs_median_hours_to_merge .......... [[32mPASS[0m in 0.23s]
17:05:03 | 20 | prefect.task_runs - 105 of 108 START test non_negative_mart_health_inputs_release_interval_cv ...... [RUN]
17:05:03 | 20 | prefect.task_runs - Finished in state Completed()
17:05:03 | 20 | prefect.task_runs - 105 of 108 PASS non_negative_mart_health_inputs_release_interval_cv ............ [[32mPASS[0m in 0.13s]
17:05:03 | 20 | prefect.task_runs - 106 of 108 START test non_negative_mart_health_inputs_releases_per_year ........ [RUN]
17:05:03 | 20 | prefect.task_runs - 106 of 108 PASS non_negative_mart_health_inputs_releases_per_year .............. [[32mPASS[0m in 0.27s]
17:05:03 | 20 | prefect.task_runs - 107 of 108 START test not_null_mart_health_inputs_repo ......................... [RUN]
17:05:03 | 20 | prefect.task_runs - Finished in state Completed()
17:05:03 | 20 | prefect.task_runs - Finished in state Completed()
17:05:04 | 20 | prefect.task_runs - 107 of 108 PASS not_null_mart_health_inputs_repo ............................... [[32mPASS[0m in 0.29s]
17:05:04 | 20 | prefect.task_runs - 108 of 108 START test unique_mart_health_inputs_repo ........................... [RUN]
17:05:04 | 20 | prefect.task_runs - 108 of 108 PASS unique_mart_health_inputs_repo ................................. [[32mPASS[0m in 0.22s]
17:05:04 | 20 | prefect.task_runs - Finished in state Completed()
17:05:05 | 20 | prefect.task_runs - Finished in state Completed()
17:05:05 | 20 | prefect.task_runs - Finished in state Completed()
17:05:05 | 20 | prefect.task_runs - Finished in state Completed()
17:05:05 | 20 | prefect.task_runs - Finished in state Completed()
17:05:05 | 20 | prefect.task_runs - Finished in state Completed()
17:05:05 | 20 | prefect.task_runs - Finished running 4 table models, 98 data tests, 6 view models in 0 hours 0 minutes and 26.60 seconds (26.60s).
17:05:05 | 20 | prefect.task_runs - Finished in state Completed()
17:05:05 | 20 | prefect.task_runs - Finished in state Completed()
17:05:05 | 20 | prefect.task_runs - Finished in state Completed()
17:05:06 | 20 | prefect.task_runs - Finished in state Completed()
17:05:06 | 20 | prefect.task_runs - Finished in state Completed()
17:05:06 | 20 | prefect.task_runs - Finished in state Completed()
17:05:06 | 20 | prefect.task_runs - Finished in state Completed()
17:05:06 | 20 | prefect.task_runs - Finished in state Completed()
17:05:06 | 20 | prefect.task_runs - [32mCompleted successfully[0m
17:05:06 | 20 | prefect.task_runs - Finished in state Completed()
17:05:06 | 20 | prefect.task_runs - Done. PASS=108 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=108
17:05:06 | 20 | prefect.task_runs - model repos: SUCCESS (0.12s)
17:05:06 | 20 | prefect.task_runs - model run_context: SUCCESS (0.12s)
17:05:06 | 20 | prefect.task_runs - model stg_commits: SUCCESS (0.09s)
17:05:06 | 20 | prefect.task_runs - model stg_pr_reviews: SUCCESS (0.08s)
17:05:06 | 20 | prefect.task_runs - model stg_pull_requests: SUCCESS (0.11s)
17:05:06 | 20 | prefect.task_runs - model stg_releases: SUCCESS (0.15s)
17:05:06 | 20 | prefect.task_runs - test not_null_repos_repo: PASS (0.16s)
17:05:06 | 20 | prefect.task_runs - test unique_repos_repo: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test assert_run_context_one_row: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test not_null_run_context_run_month: PASS (0.16s)
17:05:06 | 20 | prefect.task_runs - test not_null_run_context_run_time: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test not_null_run_context_window_start: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_commits_committed_at: PASS (0.39s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_commits_committer_at: PASS (0.30s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_commits_is_bot: PASS (0.42s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_commits_repo: PASS (0.31s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_commits_sha: PASS (0.24s)
17:05:06 | 20 | prefect.task_runs - test unique_together_stg_commits_repo__sha: PASS (0.25s)
17:05:06 | 20 | prefect.task_runs - test accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED: PASS (0.28s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pr_reviews_is_bot: PASS (0.24s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pr_reviews_pr_key: PASS (0.20s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pr_reviews_pr_number: PASS (0.14s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pr_reviews_repo: PASS (0.28s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pr_reviews_review_id: PASS (0.14s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pr_reviews_state: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pr_reviews_submitted_at: PASS (0.27s)
17:05:06 | 20 | prefect.task_runs - test unique_stg_pr_reviews_review_id: PASS (0.17s)
17:05:06 | 20 | prefect.task_runs - test accepted_values_stg_pull_requests_state__open__closed: PASS (0.24s)
17:05:06 | 20 | prefect.task_runs - test assert_pr_closed_after_created: PASS (0.16s)
17:05:06 | 20 | prefect.task_runs - test assert_review_after_pr_created: PASS (0.32s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pull_requests_created_at: PASS (0.22s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pull_requests_is_bot: PASS (0.17s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pull_requests_number: PASS (0.25s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pull_requests_pr_key: PASS (0.32s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pull_requests_repo: PASS (0.25s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pull_requests_state: PASS (0.17s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_pull_requests_updated_at: PASS (0.25s)
17:05:06 | 20 | prefect.task_runs - test relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_: PASS (0.19s)
17:05:06 | 20 | prefect.task_runs - test unique_stg_pull_requests_pr_key: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_releases_prerelease: PASS (0.26s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_releases_published_at: PASS (0.19s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_releases_release_id: PASS (0.21s)
17:05:06 | 20 | prefect.task_runs - test not_null_stg_releases_repo: PASS (0.24s)
17:05:06 | 20 | prefect.task_runs - test unique_stg_releases_release_id: PASS (0.33s)
17:05:06 | 20 | prefect.task_runs - model fct_pull_requests: SUCCESS (0.69s)
17:05:06 | 20 | prefect.task_runs - model fct_releases: SUCCESS (0.33s)
17:05:06 | 20 | prefect.task_runs - test accepted_values_fct_pull_requests_state__open__closed: PASS (0.26s)
17:05:06 | 20 | prefect.task_runs - test non_negative_fct_pull_requests_hours_to_close: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test non_negative_fct_pull_requests_hours_to_first_review: PASS (0.50s)
17:05:06 | 20 | prefect.task_runs - test non_negative_fct_pull_requests_hours_to_merge: PASS (0.21s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_created_at: PASS (0.13s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_has_approving_review: PASS (0.30s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_is_bot: PASS (0.26s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_is_closed_unmerged: PASS (0.19s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_is_merged: PASS (0.21s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_is_open: PASS (0.11s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_is_stale: PASS (0.14s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_number: PASS (0.16s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_pr_key: PASS (0.24s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_repo: PASS (0.32s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_reviewed_within_7d: PASS (0.20s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_state: PASS (0.26s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_pull_requests_updated_at: PASS (0.22s)
17:05:06 | 20 | prefect.task_runs - test unique_fct_pull_requests_pr_key: PASS (0.16s)
17:05:06 | 20 | prefect.task_runs - test non_negative_fct_releases_days_since_previous: PASS (0.19s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_releases_is_prerelease: PASS (0.24s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_releases_published_at: PASS (0.32s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_releases_release_id: PASS (0.13s)
17:05:06 | 20 | prefect.task_runs - test not_null_fct_releases_repo: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test unique_fct_releases_release_id: PASS (0.23s)
17:05:06 | 20 | prefect.task_runs - model mart_repo_monthly: SUCCESS (0.46s)
17:05:06 | 20 | prefect.task_runs - test between_mart_repo_monthly_merge_ratio__1__0: PASS (0.20s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_active_contributors: PASS (0.24s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_commits: PASS (0.22s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_median_first_review_hours: PASS (0.30s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_median_merge_hours: PASS (0.29s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_month_index: PASS (0.23s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_open_backlog_end: PASS (0.29s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_prs_closed_unmerged: PASS (0.26s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_prs_merged: PASS (0.22s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_repo_monthly_prs_opened: PASS (0.28s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_active_contributors: PASS (0.13s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_commits: PASS (0.17s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_is_partial_month: PASS (0.21s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_month: PASS (0.36s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_month_index: PASS (0.13s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_open_backlog_end: PASS (0.19s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_prs_closed_unmerged: PASS (0.26s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_prs_merged: PASS (0.14s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_prs_opened: PASS (0.29s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_repo_monthly_repo: PASS (0.28s)
17:05:06 | 20 | prefect.task_runs - test unique_together_mart_repo_monthly_repo__month: PASS (0.30s)
17:05:06 | 20 | prefect.task_runs - model mart_health_inputs: SUCCESS (0.45s)
17:05:06 | 20 | prefect.task_runs - test between_mart_health_inputs_merge_rate__1__0: PASS (0.23s)
17:05:06 | 20 | prefect.task_runs - test between_mart_health_inputs_merged_per_opened__1__0: PASS (0.18s)
17:05:06 | 20 | prefect.task_runs - test between_mart_health_inputs_pct_merged_with_approval__1__0: PASS (0.12s)
17:05:06 | 20 | prefect.task_runs - test between_mart_health_inputs_pct_open_stale__1__0: PASS (0.16s)
17:05:06 | 20 | prefect.task_runs - test between_mart_health_inputs_pct_reviewed_within_7d__1__0: PASS (0.24s)
17:05:06 | 20 | prefect.task_runs - test between_mart_health_inputs_top_author_share__1__0: PASS (0.17s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_health_inputs_active_authors_per_month: PASS (0.29s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_health_inputs_bus_factor: PASS (0.29s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_health_inputs_commits_per_week: PASS (0.20s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_health_inputs_median_hours_to_first_review: PASS (0.26s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_health_inputs_median_hours_to_merge: PASS (0.23s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_health_inputs_release_interval_cv: PASS (0.13s)
17:05:06 | 20 | prefect.task_runs - test non_negative_mart_health_inputs_releases_per_year: PASS (0.27s)
17:05:06 | 20 | prefect.task_runs - test not_null_mart_health_inputs_repo: PASS (0.29s)
17:05:06 | 20 | prefect.task_runs - test unique_mart_health_inputs_repo: PASS (0.22s)
17:05:06 | 20 | prefect.task_runs - dbt build summary: {'success': 10, 'pass': 98}
17:05:06 | 20 | prefect.task_runs - Finished in state Completed()
17:05:11 | 20 | prefect.task_runs - 1a. Landed files -> raw tables
                     repo endpoint  landed_keys  missing_in_raw
scikit-learn/scikit-learn  commits         3345               0
                dask/dask  commits         1371               0
         plotly/plotly.py  commits         2827               0
scikit-learn/scikit-learn    pulls         6400               0
                dask/dask    pulls         1500               0
         plotly/plotly.py    pulls          900               0
scikit-learn/scikit-learn  reviews        18258               0
                dask/dask  reviews         1985               0
         plotly/plotly.py  reviews         1592               0
scikit-learn/scikit-learn releases           51               0
                dask/dask releases           44               0
         plotly/plotly.py releases          109               0
17:05:11 | 20 | prefect.task_runs - 1b. Raw tables -> staging models
              raw  raw_rows  excluded  expected           staging  staged_rows  missing  unexpected
      raw_commits      7543         0      7543       stg_commits         7543        0           0
raw_pull_requests      8800         0      8800 stg_pull_requests         8800        0           0
   raw_pr_reviews     21835         0     21835    stg_pr_reviews        21835        0           0
     raw_releases       204         0       204      stg_releases          204        0           0
17:05:11 | 20 | prefect.task_runs - 1c. Commits in every complete month: 34 complete months per repo, 1 repo-months with no commits
17:05:11 | 30 | prefect.task_runs - Months with no commits (reported, not a failure): dask/dask 2026-10
17:05:11 | 20 | prefect.task_runs - Completeness checks passed
17:05:11 | 20 | prefect.task_runs - 2. Outliers: value > Q3 + 1.5 x IQR, per repo (hours for merges, commits for weeks)
       measure                      repo    n   q1    q3  fence
hours_to_merge                 dask/dask  830  1.3  75.0  185.6
hours_to_merge          plotly/plotly.py  466  1.2 239.6  597.3
hours_to_merge scikit-learn/scikit-learn 3324  4.8 196.2  483.4
weekly_commits                 dask/dask  147  2.0  12.0   27.0
weekly_commits          plotly/plotly.py  147  5.0  26.0   57.5
weekly_commits scikit-learn/scikit-learn  147 17.0  26.5   40.8
17:05:11 | 20 | prefect.task_runs - 715 flagged records, all KEPT (treatment = keep):
       measure                      repo                          record   value  fence treatment
hours_to_merge                 dask/dask                 dask/dask#10812  5166.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11118  3851.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12010  2839.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12002  2779.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11965  2764.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12024  2319.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11128  2251.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12041  2251.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11875  2229.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11421  2194.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12040  2083.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11446  1960.4  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11952  1606.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11182  1432.4  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11998  1405.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11178  1321.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11347  1247.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11378  1244.4  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11818  1129.9  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11558  1129.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11822  1123.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12027  1077.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11248  1059.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11736   978.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12205   905.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11575   888.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11683   862.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12341   791.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11377   744.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11456   707.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11687   695.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12223   693.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12224   692.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12153   667.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12274   665.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11271   650.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11692   648.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12516   639.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11388   635.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12279   621.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12342   598.9  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12103   565.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12320   543.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12221   528.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12380   527.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11689   503.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12399   488.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12300   484.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11477   454.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12333   453.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12292   452.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#10929   429.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11060   428.9  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11797   428.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12284   411.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12327   408.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12354   379.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11245   378.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11623   372.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11611   363.9  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12079   332.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12301   317.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11025   312.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#10884   307.4  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11801   291.4  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11697   288.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11014   288.4  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11553   288.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#10883   286.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11029   286.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11899   283.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12087   283.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11555   278.4  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11045   267.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#10828   263.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12144   262.4  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#10838   261.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11560   257.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11326   256.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11586   254.9  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12206   241.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12344   239.9  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12345   239.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11393   221.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12423   220.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12140   217.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12104   215.9  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12304   209.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11007   204.0  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11328   199.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11624   197.7  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11450   197.5  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11844   193.6  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11970   192.3  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#10896   192.1  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#11135   191.2  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12389   190.8  185.6      keep
hours_to_merge                 dask/dask                 dask/dask#12430   188.4  185.6      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5232 11797.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5311 10237.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5262 10234.3  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5310 10220.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5285  9821.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5374  7060.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5333  6310.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4949  4486.3  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5393  4225.1  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4958  4032.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4622  3793.7  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4644  3742.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4982  3651.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5180  3407.9  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5439  3228.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4606  3095.8  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4995  3015.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4873  2654.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5445  2596.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5258  2231.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5473  2164.1  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4658  2103.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4978  2031.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5508  2004.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5274  1963.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4647  1908.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5149  1886.8  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4994  1882.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5447  1559.8  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4661  1531.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5710  1442.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5698  1408.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5699  1371.7  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5700  1371.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5697  1370.8  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5377  1363.9  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5696  1349.8  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5062  1346.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5312  1300.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5454  1283.7  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5315  1225.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5521  1223.7  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5522  1196.8  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5322  1189.1  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5719  1156.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4769  1156.3  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4612  1130.7  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4627  1130.1  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5477  1084.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4561  1079.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4510  1066.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5691  1062.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5111  1037.7  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5483   961.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4763   959.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5175   959.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5165   954.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4476   908.9  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5518   904.8  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5692   884.3  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5269   868.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4546   850.7  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4525   846.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4790   844.8  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5701   821.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5716   810.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5052   792.3  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4526   764.3  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4840   715.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5096   715.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4557   713.5  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5302   693.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5303   693.2  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#4886   687.0  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5784   684.6  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5772   680.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5634   659.4  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5019   650.1  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5727   616.7  597.3      keep
hours_to_merge          plotly/plotly.py           plotly/plotly.py#5633   614.7  597.3      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28464 18651.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29026 16646.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28760 16489.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29532 15041.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29499 15034.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29641 13715.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28043 12980.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29661 12292.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30869 12027.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29155 11800.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31413 11714.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30859 10898.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28971 10799.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29097 10686.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28755 10606.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29307 10552.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30508 10361.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29022  9909.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32042  9759.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31127  9611.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29310  9607.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28559  9385.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30134  9088.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30751  8846.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28981  8391.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29651  8374.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31177  8020.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31289  7915.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32789  7729.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29151  7684.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30041  7590.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32587  7555.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29716  7456.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30564  7284.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32761  7203.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31172  7028.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29034  6993.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29822  6774.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29431  6480.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31543  6306.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32106  6208.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32111  6147.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28509  6011.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28519  5955.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31937  5889.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31454  5306.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31529  5237.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30562  5207.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29288  5114.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28637  5032.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31230  5004.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29865  4980.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32205  4900.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28901  4864.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32460  4848.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29791  4838.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30861  4826.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30864  4776.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30974  4568.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33066  4532.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31096  4515.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30942  4508.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31036  4490.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30965  4467.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28840  4459.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29797  4366.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32119  4324.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32911  4248.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28064  4218.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30179  4195.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30399  4187.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28179  4170.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30112  4152.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32050  4102.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28096  4056.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28678  4014.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31671  4013.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28268  3998.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33908  3959.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32558  3892.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32793  3856.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33600  3574.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28322  3480.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31104  3476.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29473  3445.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30090  3377.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29519  3345.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29727  3333.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28494  3329.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29907  3327.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33354  3321.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29319  3313.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29213  3284.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33953  3264.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32193  3210.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31027  3206.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30777  3187.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33150  3184.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31068  3111.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30553  3085.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33626  3049.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33633  3000.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31322  2996.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30787  2974.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32191  2927.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33048  2922.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30340  2916.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30057  2907.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30127  2823.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28106  2811.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32163  2794.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31342  2788.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31829  2787.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30878  2782.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31062  2759.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29845  2748.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32212  2711.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30520  2685.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34326  2682.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28622  2678.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29451  2666.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32732  2620.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28721  2612.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30182  2600.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31079  2572.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30763  2561.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33086  2551.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33972  2551.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32755  2523.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33872  2518.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30616  2491.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30825  2439.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29426  2437.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30514  2421.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34313  2403.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32644  2397.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34547  2302.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31617  2284.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28189  2277.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30886  2258.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31151  2225.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31888  2213.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33252  2203.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33260  2173.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28351  2163.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33322  2128.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32565  2115.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30519  2102.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33778  2088.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33037  2075.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29060  2065.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31534  2036.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33906  1990.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29584  1990.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29080  1988.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28934  1985.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34266  1976.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30155  1966.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32828  1960.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30828  1949.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30730  1928.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33067  1923.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33959  1913.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32073  1885.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31362  1875.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30319  1869.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28422  1867.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31564  1859.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31656  1831.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32699  1814.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33573  1793.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32114  1775.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34084  1767.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28592  1730.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33262  1710.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32964  1698.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33076  1684.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31775  1681.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32100  1674.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29012  1662.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30152  1661.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29653  1639.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30990  1628.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31418  1605.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30712  1591.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30916  1590.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29705  1584.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29882  1567.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29867  1563.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33176  1560.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34582  1558.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31856  1552.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34513  1552.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28623  1547.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29978  1527.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30819  1525.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28401  1518.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29063  1516.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29419  1512.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30187  1507.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33441  1501.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30196  1482.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33759  1481.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29634  1480.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29442  1480.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31867  1476.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32117  1475.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29476  1469.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34096  1465.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32274  1433.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30959  1428.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33533  1427.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31201  1412.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31753  1388.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32708  1387.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34341  1381.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33810  1366.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31924  1364.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32108  1353.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33229  1348.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28967  1346.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33423  1344.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29237  1343.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30695  1339.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28428  1338.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30694  1329.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28773  1322.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30944  1317.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31939  1296.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29312  1292.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33269  1287.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33268  1282.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30844  1281.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31050  1271.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34681  1270.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34789  1264.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28448  1257.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33723  1254.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34786  1249.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28612  1240.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34064  1231.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32309  1215.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32853  1214.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30857  1209.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30865  1208.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34190  1197.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30985  1192.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34832  1190.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29158  1175.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30903  1168.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28101  1166.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30351  1161.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29744  1150.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34242  1148.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28701  1146.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33856  1144.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31040  1139.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28070  1138.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29475  1130.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29260  1129.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32229  1128.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32313  1128.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33020  1128.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29215  1126.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28630  1124.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32713  1123.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29760  1116.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29759  1115.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29216  1109.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34663  1107.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34246  1105.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29363  1103.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31613  1094.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28517  1087.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31643  1084.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32759  1082.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29793  1082.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29501  1082.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33491  1078.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30380  1078.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28205  1060.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33525  1054.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34289  1050.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34718  1043.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30311  1038.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33466  1032.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34490  1028.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29227  1027.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31405  1024.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30415  1023.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31294  1022.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30469  1022.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33440  1021.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29773  1004.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31563  1004.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30128   994.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31435   978.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33628   972.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29691   965.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34659   963.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34239   962.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30949   959.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34399   957.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30406   951.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28160   950.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34680   939.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28347   933.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32024   931.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31310   930.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33388   919.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34727   916.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33471   915.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32768   914.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32813   912.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28703   905.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28092   905.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31107   904.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32566   902.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32909   901.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30666   898.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29018   895.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30483   889.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29779   886.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31063   885.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28804   882.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28171   862.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31431   862.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29920   860.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31575   858.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28288   856.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28285   856.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28286   855.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32504   843.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28407   839.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34475   838.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29709   832.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32839   832.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29230   831.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29095   831.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34142   826.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31585   825.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32351   819.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33827   818.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33041   814.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28300   814.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31977   813.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28799   803.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34334   802.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30708   802.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34824   796.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31559   795.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33521   791.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28290   782.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34493   781.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33722   780.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30517   780.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32549   776.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30833   771.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29935   770.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28984   769.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28506   769.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29818   769.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29950   768.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32846   767.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28261   766.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32345   766.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31500   763.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34347   761.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34273   754.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32582   752.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30743   750.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32246   748.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33443   748.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30496   746.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31150   744.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30595   741.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29874   738.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30575   738.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34360   737.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28352   732.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29468   731.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32592   730.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29160   729.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30838   728.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30395   728.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33465   728.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34363   727.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30137   726.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28601   722.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32576   722.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28306   722.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29868   721.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28343   718.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28342   717.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28344   716.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30683   715.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31933   715.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31820   712.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28614   707.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33148   706.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31553   703.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34997   700.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32778   697.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34966   697.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33565   695.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28231   695.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30847   692.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28664   688.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31117   683.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34409   680.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34378   679.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31193   679.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31430   677.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31452   675.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34221   673.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29838   671.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31660   671.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29265   670.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31969   670.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28503   670.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33704   670.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31015   667.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29505   667.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28223   665.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31135   664.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29890   663.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31026   662.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29896   661.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29232   661.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30858   660.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34452   651.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29100   651.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31486   650.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29266   646.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28712   644.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28756   643.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33015   642.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31494   641.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30418   639.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34377   636.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33889   633.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34876   628.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34826   626.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32845   623.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31250   622.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28167   622.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32985   616.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31584   612.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30429   610.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31167   606.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34838   597.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31414   596.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34692   596.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28187   595.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34983   594.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29263   594.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30473   594.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30518   591.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33333   589.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28922   588.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29124   584.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32902   580.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29921   579.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31204   578.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33348   575.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33063   575.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28399   574.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29258   574.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34406   562.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30521   561.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34506   555.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30198   554.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31898   554.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30451   553.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29300   552.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31959   551.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30455   549.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31379   549.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29861   548.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31701   547.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30642   546.6  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29966   542.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31445   532.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29404   531.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28338   530.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34405   529.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32188   529.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34957   526.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29581   520.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28331   520.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34079   519.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28102   518.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#30895   512.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28040   509.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32583   509.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28652   508.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#28813   502.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31987   500.8  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34527   500.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34194   499.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32190   498.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32197   497.4  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32347   497.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31973   496.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#32826   496.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29389   495.7  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33563   495.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#34442   493.9  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29143   492.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#31419   489.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#33787   488.2  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29677   487.5  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#35099   487.3  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#35095   487.1  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#29700   486.0  483.4      keep
hours_to_merge scikit-learn/scikit-learn scikit-learn/scikit-learn#35120   483.9  483.4      keep
weekly_commits                 dask/dask              week of 2024-01-08    77.0   27.0      keep
weekly_commits                 dask/dask              week of 2024-01-15    75.0   27.0      keep
weekly_commits                 dask/dask              week of 2024-01-29    45.0   27.0      keep
weekly_commits                 dask/dask              week of 2024-03-04    43.0   27.0      keep
weekly_commits                 dask/dask              week of 2024-02-05    41.0   27.0      keep
weekly_commits                 dask/dask              week of 2024-01-01    36.0   27.0      keep
weekly_commits                 dask/dask              week of 2024-11-04    29.0   27.0      keep
weekly_commits                 dask/dask              week of 2024-08-12    28.0   27.0      keep
weekly_commits          plotly/plotly.py              week of 2024-10-21    98.0   57.5      keep
weekly_commits          plotly/plotly.py              week of 2024-11-11    76.0   57.5      keep
weekly_commits          plotly/plotly.py              week of 2024-11-04    73.0   57.5      keep
weekly_commits          plotly/plotly.py              week of 2024-08-26    59.0   57.5      keep
weekly_commits          plotly/plotly.py              week of 2026-07-27    59.0   57.5      keep
weekly_commits          plotly/plotly.py              week of 2024-07-29    58.0   57.5      keep
weekly_commits          plotly/plotly.py              week of 2025-02-03    58.0   57.5      keep
weekly_commits scikit-learn/scikit-learn              week of 2025-10-06    83.0   40.8      keep
weekly_commits scikit-learn/scikit-learn              week of 2024-01-22    51.0   40.8      keep
weekly_commits scikit-learn/scikit-learn              week of 2024-01-15    51.0   40.8      keep
weekly_commits scikit-learn/scikit-learn              week of 2024-10-14    51.0   40.8      keep
weekly_commits scikit-learn/scikit-learn              week of 2025-09-29    50.0   40.8      keep
weekly_commits scikit-learn/scikit-learn              week of 2024-02-19    49.0   40.8      keep
17:05:11 | 20 | prefect.task_runs - 3. Descriptive statistics per repo (hours; human PRs)
                     repo  median_first_review_h  p90_first_review_h  median_merge_h  p90_merge_h  commits_per_week  releases_per_year
                dask/dask                  11.53              223.09           12.30       223.17              9.27              15.53
         plotly/plotly.py                  16.21              701.29           41.46      1130.42             19.07              10.23
scikit-learn/scikit-learn                  12.91              277.04           35.54       842.42             22.62               5.29
17:05:11 | 20 | prefect.task_runs - Finished in state Completed()
17:05:11 | 20 | prefect.task_runs - Metric values and 0-1 scores (1 = healthiest of the three)
                     repo                       metric       dimension  direction  value null_rule  score
                dask/dask             commit_trend_pct        activity          1 -0.075      None  0.000
         plotly/plotly.py             commit_trend_pct        activity          1 -0.007      None  1.000
scikit-learn/scikit-learn             commit_trend_pct        activity          1 -0.009      None  0.957
                dask/dask median_hours_to_first_review  responsiveness         -1 11.531      None  1.000
         plotly/plotly.py median_hours_to_first_review  responsiveness         -1 16.212      None  0.000
scikit-learn/scikit-learn median_hours_to_first_review  responsiveness         -1 12.908      None  0.706
                dask/dask       pct_reviewed_within_7d  responsiveness          1  0.563      None  0.000
         plotly/plotly.py       pct_reviewed_within_7d  responsiveness          1  0.578      None  0.200
scikit-learn/scikit-learn       pct_reviewed_within_7d  responsiveness          1  0.640      None  1.000
                dask/dask                   merge_rate      throughput          1  0.876      None  1.000
         plotly/plotly.py                   merge_rate      throughput          1  0.815      None  0.664
scikit-learn/scikit-learn                   merge_rate      throughput          1  0.695      None  0.000
                dask/dask               pct_open_stale         backlog         -1  0.221      None  0.713
         plotly/plotly.py               pct_open_stale         backlog         -1  0.179      None  1.000
scikit-learn/scikit-learn               pct_open_stale         backlog         -1  0.326      None  0.000
                dask/dask      open_backlog_growth_pct         backlog         -1  0.107      None  0.000
         plotly/plotly.py      open_backlog_growth_pct         backlog         -1  0.074      None  0.660
scikit-learn/scikit-learn      open_backlog_growth_pct         backlog         -1  0.057      None  1.000
                dask/dask          release_interval_cv release_cadence         -1  0.943      None  0.000
         plotly/plotly.py          release_interval_cv release_cadence         -1  0.686      None  0.901
scikit-learn/scikit-learn          release_interval_cv release_cadence         -1  0.658      None  1.000
                dask/dask                   bus_factor    contributors          1  3.000      None  0.167
         plotly/plotly.py                   bus_factor    contributors          1  2.000      None  0.000
scikit-learn/scikit-learn                   bus_factor    contributors          1  8.000      None  1.000
17:05:11 | 20 | prefect.task_runs - Weights: responsiveness 25%, contributors 20%, backlog 20%, throughput 15%, release_cadence 10%, activity 10%
17:05:11 | 20 | prefect.task_runs - Health ranking (score and subscores 0-1)
 rank                      repo  score  responsiveness  contributors  backlog  throughput  release_cadence  activity
    1 scikit-learn/scikit-learn  0.709           0.853         1.000    0.500       0.000            1.000     0.957
    2          plotly/plotly.py  0.481           0.100         0.000    0.830       0.664            0.901     1.000
    3                 dask/dask  0.380           0.500         0.167    0.357       1.000            0.000     0.000
17:05:11 | 20 | prefect.task_runs - Sensitivity, equal weights: 1. scikit-learn/scikit-learn (0.718), 2. plotly/plotly.py (0.582), 3. dask/dask (0.337) - same order
17:05:11 | 20 | prefect.task_runs - Finished in state Completed()
17:05:13 | 20 | prefect.task_runs - Saved /tmp/prefect-flow-run-7ugcktee/DS3022-Project2-main/plots/dimension_profile.png
17:05:14 | 20 | prefect.task_runs - Saved /tmp/prefect-flow-run-7ugcktee/DS3022-Project2-main/plots/monthly_trends.png
17:05:15 | 20 | prefect.task_runs - Saved /tmp/prefect-flow-run-7ugcktee/DS3022-Project2-main/plots/review_vs_merge.png
17:05:15 | 20 | prefect.task_runs - Finished in state Completed()
17:05:15 | 20 | prefect.flow_runs - API odometer: 175 requests (94 GraphQL points) this flow run
17:05:15 | 20 | prefect.flow_runs - Finished in state Completed()
```

</details>
