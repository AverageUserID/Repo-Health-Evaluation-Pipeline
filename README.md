# Open Source Package Health Pipeline (vra7dy)

**Package set B:** scikit-learn/scikit-learn, dask/dask, plotly/plotly.py, from 2024-01-01. One Prefect flow, `vra7dy-package-health-flow` in [`main.py`](main.py), runs six tasks in order: extract, load, dbt build, validate, model, plot. The code is in [`pipeline/`](pipeline/), and the dbt models and tests are in [`models/`](models/) and [`tests/`](tests/).

**Result:** 1. scikit-learn (0.714), 2. plotly.py (0.474), 3. dask (0.390).

## Setup and run

```bash
pipenv install                  # Python 3.12; versions pinned in Pipfile.lock and requirements.txt
export GH_PAT=<your token>      # any token that can read public repos
pipenv run python main.py
```

- **Token:** read from `GH_PAT`. If that is unset, it is loaded from the Prefect Secret block `vra7dy-pat` ([`pipeline/github.py`](pipeline/github.py)).
- **Prefect:** the flow logs to whichever Prefect server the active profile points at. We ran it against the class server (`uvasds` profile).
- **Logs:** each task writes its own file in `logs/`: `extract.log`, `load.log`, `dbt_build.log`, `validate.log`, `model.log`, `plot.log` and `flow.log`. dbt's own log is `logs/dbt.log`.
- **Optional environment variables:**
  - `PIPELINE_API=rest`: fetch PRs and reviews over REST instead of GraphQL.
  - `PIPELINE_CONCURRENCY=1`: fetch the repos one at a time.
  - `PIPELINE_START`, `PIPELINE_REPOS`: a smaller window or other repos, for development.

**Expected runtime:**

| Run | Runtime |
| --- | --- |
| Full run from an empty `data/raw/` (GraphQL) | about 4 min (3 min 50 s on the class server) |
| Incremental run | about 45 s |
| `PIPELINE_API=rest` full run | about 78 min, because it hits GitHub's hourly REST limit and sleeps until the reset |

## Task 1: interrupted and resumed runs

**Run 1 (`vague-dog`):** we pressed Ctrl-C while scikit-learn's PRs were still being fetched. dask and plotly.py had finished, and scikit-learn had 7 PR pages on disk. Ctrl-C only reaches the main thread, so the extract task sets a stop flag that the worker threads check before each request. They stop at the next page boundary.

```
23:12:34 INFO No data/raw/: full extract from 2024-01-01T00:00:00Z
23:13:01 INFO plotly/plotly.py finished in 26.7s on ThreadPoolExecutor-1_2, 40 requests
23:13:09 INFO dask/dask finished in 35.0s on ThreadPoolExecutor-1_1, 29 requests
23:13:10 WARNING Interrupted: stopping repo threads after their current page
23:13:11 ERROR Task run 'extract_repo-c5e' - ExtractInterrupted('Interrupted before POST /graphql')
23:13:12 INFO Flow run 'vague-dog' - Finished in state Crashed('Execution was aborted by an interrupt signal.')
```

**Run 2 (`pumpkin-gazelle`):** there was no `_LOADED` marker, so the run kept `data/raw/` and skipped every page already on disk. Each landing file stores the cursor (GraphQL) or `next_url` (REST) for the following page, so pagination resumes exactly where it stopped.

```
23:13:20 INFO No _LOADED marker: resuming, pages already in data/raw/ will be skipped
23:13:20 INFO dask/dask commits: 1371 records, 14 pages (0 fetched, 14 already on disk), 0 requests
23:13:20 INFO dask/dask finished in 0.4s on ThreadPoolExecutor-1_1, 0 requests
23:13:20 INFO plotly/plotly.py finished in 0.4s on ThreadPoolExecutor-1_2, 0 requests
23:15:46 INFO scikit-learn/scikit-learn pulls+reviews (graphql): 6300 PRs, 63 pages (56 fetched, 7 already on disk), ...
23:15:46 INFO Extract finished: 62 API requests (61 GraphQL points); concurrency 3, wall 146.1s, ...
23:17:22 INFO Flow run 'pumpkin-gazelle' - Finished in state Completed()
```

## Task 2: loading the same files twice

Task 2 upserts on `(repo, id)` and loads the same files twice in one run. The counts are logged after each pass ([`pipeline/load.py`](pipeline/load.py)). These are the counts from run 2:

| Table | Pass 1 (rows before → after) | Pass 2 (rows before → after) |
| --- | --- | --- |
| `raw_commits` | 0 → 7,531 | 7,531 → 7,531 |
| `raw_pull_requests` | 0 → 8,500 | 8,500 → 8,500 |
| `raw_pr_reviews` | 0 → 21,789 | 21,789 → 21,789 |
| `raw_releases` | 0 → 204 | 204 → 204 |

## Requests: full run vs incremental run

| Run | API requests | GraphQL points | Extract time |
| --- | --- | --- | --- |
| Full, from an empty `data/raw/` (deployment run `arboreal-kestrel`) | **175** | 94 | 176.6 s |
| Incremental (run 3, `industrious-kelpie`) | **10** | 3 | 3.3 s |

The incremental run fetches only records newer than each endpoint's `_load_state` watermark:
- **Commits:** fetched since the commit watermark minus 7 days, because a commit's author date can be older than the date it reached the default branch.
- **PRs:** fetched newest-updated first, stopping at the first page older than the watermark.
- **Releases:** re-fetched in full every run, because there are only 1–2 pages per repo.

**API odometer:** every flow run logs its total, e.g. `API odometer: 175 requests (94 GraphQL points) this flow run` ([`main.py`](main.py)).

## dbt: bot rule and open PRs

**Bot rule** ([`macros/is_bot.sql`](macros/is_bot.sql)):
- A user is a bot if `user.type = 'Bot' OR user.login LIKE '%[bot]'`.
- It applies to PR authors, reviewers and commit authors.
- A commit with no linked GitHub account is not a bot.
- PR and review metrics exclude bots. Commit counts include them, because bot commits still land in the repo.
- GraphQL drops the `[bot]` suffix from bot logins, so [`pipeline/graphql.py`](pipeline/graphql.py) adds it back for `Bot` users.

**Open PRs and medians:**
- PRs still open at run time are kept with `is_open = true` and `closed_at` / `merged_at` NULL. They never get a fake close date.
- **Median time to merge:** taken over merged PRs only, so open PRs don't count toward it.
- **Median time to first review:** taken over every PR that has a non-author, non-bot review, open or not. A PR that was never reviewed has no value and drops out of the median.
- So that unreviewed PRs aren't simply ignored, `pct_reviewed_within_7d` counts them as "not reviewed within 7 days". It only includes PRs at least 7 days old.
- Open PRs drive the backlog metrics: `pct_open_stale` (open, with no activity for 180+ days) and `open_backlog_growth_pct`.

## Task 4: validation

**Completeness checks** ([`pipeline/validate.py`](pipeline/validate.py)) fail the flow on any mismatch:
- every distinct `(repo, id)` in `data/raw/` is in its raw table;
- every raw table's row count matches its staging model.

**One deviation from the rubric:** a complete month with no commits is logged as a warning, not a failure. dask had no commits in October 2026; the GitHub API confirms its last commit was 2026-09-29. That month is quiet, not missing: the commits endpoint returns every commit in the window, and missing data would already fail the two checks above. Failing on it would make every run after Oct 31 fail, including a run from a clean clone.

**Outliers:**
- **Cutoff:** Tukey's fence, a value above Q3 + 1.5 × IQR, computed per repo.
- **Scanned:** time to merge (human PRs) and commits per week. Weeks with no commits count as 0, and the partial first and last weeks are skipped.
- **Result:** 714 records flagged, all **kept**, and written to the `validation_outliers` table. The per-repo fences and the largest flagged values:

| Measure | Repo | Fence | Flagged | Largest |
| --- | --- | --- | --- | --- |
| hours to merge | scikit-learn | 482.7 h | 515 | #28464, 18,651 h (2.1 years) |
| hours to merge | plotly.py | 602.4 h | 80 | #5232, 11,797 h |
| hours to merge | dask | 185.6 h | 98 | #10812, 5,166 h |
| commits per week | scikit-learn | 40.8 | 5 | week of 2025-10-06, 83 |
| commits per week | plotly.py | 57.5 | 7 | week of 2024-10-21, 98 |
| commits per week | dask | 27.0 | 8 | week of 2024-01-08, 77 |

Why we keep them:
- A PR that stays open for years, or a burst of commits around a release, is real behavior, not bad data.
- The health metrics use medians, shares and ratios, which a long tail barely moves.
- Capping or dropping would hide exactly the slow merges that responsiveness is meant to reflect.

Every flagged record is listed in `logs/validate.log`.

## Task 5: health model

Each metric is defined in [`models/marts/schema.yml`](models/marts/schema.yml). Each is a share, median, ratio or size-relative trend, so the largest repo can't win on volume alone.

| Dimension (weight) | Metric | Definition | Healthier |
| --- | --- | --- | --- |
| Responsiveness (25%) | `median_hours_to_first_review` | Median hours from PR creation to the first non-author, non-bot review | lower |
| | `pct_reviewed_within_7d` | Share of PRs at least 7 days old that were reviewed within 7 days | higher |
| Contributor base (20%) | `bus_factor` | Fewest authors who together wrote at least 50% of merged PRs | higher |
| Backlog (20%) | `pct_open_stale` | Share of open PRs with no activity in 180+ days | lower |
| | `open_backlog_growth_pct` | Monthly slope of the open-PR backlog ÷ average PRs opened per month | lower |
| Throughput (15%) | `merge_rate` | Merged ÷ (merged + closed unmerged) | higher |
| Release cadence (10%) | `release_interval_cv` | Standard deviation ÷ mean of the days between releases (how regular releases are) | lower |
| Activity (10%) | `commit_trend_pct` | Monthly slope of commits ÷ average monthly commits | higher |

**Scoring** ([`pipeline/model.py`](pipeline/model.py)):
1. Each metric is min-max scaled across the three repos to 0–1, with lower-is-better metrics flipped so that 1 is always the healthiest of the three. If all three repos tie, each scores 0.5.
2. A dimension's subscore is the mean of its metrics.
3. The overall score is the weighted mean of the subscores.
4. NULLs follow what they mean:
   - "none at all" (such as no stale PRs) is filled with 0;
   - "nothing to measure" (no reviews or releases) scores worst;
   - "the window is too short to tell" is skipped, and the remaining weights are renormalized.

   No metric was NULL in our data.

**Weights:** [`ASSESSING-HEALTH.md`](ASSESSING-HEALTH.md) lists ideas rather than a fixed ranking. We weighted each dimension by how directly our data shows whether maintainers are keeping up:
- **Responsiveness is highest (25%):** whether contributors hear back is the strongest sign that a project is maintained, and the guide lists it first.
- **Contributor base and backlog next (20% each):** a project carried by a few people is fragile, and a pile of stale, growing open PRs shows maintainers falling behind. Both come straight from the PR records: who wrote the merged PRs, and which PRs are still open.
- **Throughput (15%):** acceptance matters, but a low merge rate can also mean careful review, so it gets less weight than the measures above.
- **Release cadence and activity trend last (10% each):** each comes from a single ratio or slope over 33 months, so it is the noisiest signal. Commit counts also depend on commit habits and automation, which the guide warns about.

**Guide items not scored:** CI and testing, documentation, governance files, dependency and security hygiene, adoption, and funding (items 7–12). None of these appear in the four endpoints the pipeline fetches. Contributor organizations, semantic versioning and review-comment quality aren't in our data either.

**Sensitivity check:** with equal weights, the order is the same (scikit-learn 0.726, plotly.py 0.576, dask 0.346).

```
 rank                      repo  score  responsiveness  contributors  backlog  throughput  release_cadence  activity
    1 scikit-learn/scikit-learn  0.714           0.856         1.000    0.500       0.000            1.000     1.000
    2          plotly/plotly.py  0.474           0.099         0.000    0.793       0.683            0.901     0.978
    3                 dask/dask  0.390           0.500         0.167    0.407       1.000            0.000     0.000
```

## Task 6: plots

Each repo keeps the same color in every plot: scikit-learn blue, plotly.py green, dask orange.

**[Dimension profile](plots/dimension_profile.png).** This plot shows that the ranking is driven by different strengths, not one repo winning everywhere:
- **scikit-learn ranks first** because it leads the most heavily weighted dimensions: responsiveness (0.86), contributor base (1.00, with a bus factor of 8 against 2–3 for the others), release cadence and activity.
- **scikit-learn's weak spot** is throughput: it has the lowest merge rate (0.70), with many PRs closed without merging.
- **dask** has the best throughput (merge rate 0.88), but scores 0 on release cadence (the most irregular release gaps) and on activity (the steepest commit decline). That puts it last.
- **plotly.py** is second on the strength of its backlog and release cadence. It scores 0.10 on responsiveness and 0 on contributor base.

**[Monthly trends](plots/monthly_trends.png).**
- **scikit-learn** dominates every volume measure: 100–260 PRs opened per month and 40–95 active PR authors, rising over the window.
- **scikit-learn's open backlog** climbs steadily from about 60 to about 375. That's why its backlog subscore is only 0.50 despite its size.
- **dask** declines from 2024 to zero commits in October 2026 (just outside the plotted complete months). Its open backlog jumps to about 150 in mid-2026, and its merge ratio falls below 0.2 at the end.
- **Median review and merge times** are mostly under 50 hours for all three repos. The spikes are mostly dask and plotly.py months with only a handful of PRs, where one slow PR moves the median.

**[Review speed vs merge ratio](plots/review_vs_merge.png).** This plot encodes four variables: x = median hours to first review, y = merge ratio, bubble size = active PR authors, color = repo. Each small bubble is one month; the outlined bubbles are whole-window values.
- **scikit-learn's months** cluster tightly at fast review (under about 30 h) with many authors, but at a lower merge ratio (0.5–0.9). It is a large project that reviews quickly and turns down more PRs.
- **dask and plotly.py months** are small and scattered. Most sit at high merge ratios, but a few months show slow review (100–375 h) or a low merge ratio.
- **Their whole-window values** sit near scikit-learn's review speed, with higher merge ratios.
- **Reading:** fast first review doesn't imply a high merge rate. Here, contributor volume and review consistency are what set scikit-learn apart.

## Extended items

**GraphQL** ([`pipeline/graphql.py`](pipeline/graphql.py), `extract_pulls_graphql` in [`pipeline/extract.py`](pipeline/extract.py)):
- **Query:** one query returns 100 PRs (newest updated first), each with its first 100 reviews. A follow-up query fetches the rest for the rare PR with more than 100 reviews.
- **Pagination:** cursor pagination, with the next cursor stored in each landing file.
- **Rate limits:** `rateLimit { cost remaining resetAt }` feeds the shared rate limiter.
- **Errors:** timeouts retry at page size 50, then 25.
- **Output shape:** responses are reshaped to the REST JSON shape, so load, dbt and validation are the same in both modes.
- **Comparison:** full window from 2024, the same three repos, both runs starting from an empty `data/raw/`. Commits and releases use REST in both.

| PRs + reviews | REST requests | REST time | GraphQL requests | GraphQL time |
| --- | --- | --- | --- | --- |
| scikit-learn | 5,275 | 4,558 s | 69 | 159 s |
| dask | 1,238 | 436 s | 15 | 30 s |
| plotly.py | 692 | 247 s | 10 | 18 s |
| **Whole extract** | **7,286** | **4,573 s (76 min)** | **175** | **177 s** |

REST needs one request per PR to fetch its reviews. It used up the 5,000-requests-per-hour limit and slept 39 minutes for the reset (`Rate limit (core): 20 left, sleeping 2320s until reset`).

One caveat: GitHub's Copilot coding agent appears as `Copilot` over REST and as `copilot-swe-agent[bot]` over GraphQL. It counts as a bot either way.

**Concurrent fetching:**
- **Where it happens:** in the `extract` task ([`pipeline/extract.py`](pipeline/extract.py), around line 376), which maps `extract_repo` over the repos on a Prefect `ThreadPoolTaskRunner`, one thread per repo (`PIPELINE_CONCURRENCY`, default 3).
- **Shared rate limiter:** all threads share one `GitHubClient` ([`pipeline/github.py`](pipeline/github.py)).
  - `_reserve` takes one unit of the REST or GraphQL budget under a lock before each request, and sleeps until the reset when only the buffer is left.
  - A semaphore caps requests in flight at `MAX_IN_FLIGHT = 4`, to avoid GitHub's secondary limits on concurrent requests.
  - Per-thread counters give each repo its own request count in the logs.
- **Measured overlap:** the repos' summed extract time was 1.39× the wall time on the full GraphQL run (245.9 s of repo time in 176.6 s), and 1.15× on the REST run. The gain is limited because scikit-learn alone takes most of the time (146 of the 246 s of repo time).

**Scheduled deployment:** `vra7dy-deployment` runs monthly at midnight Eastern on the 28th on `uvasds-k8s-pool`. [`DEPLOYMENTS.md`](DEPLOYMENTS.md) has the setup, the deploy command and the log of the manually triggered run.
