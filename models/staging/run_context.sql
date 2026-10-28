-- One row of run-wide constants. "Run time" is the newest fetch in the raw tables, so
-- rebuilding without new data gives the same results (not the wall clock).
with fetches as (
    select fetched_at from {{ source('raw', 'raw_commits') }}
    union all
    select fetched_at from {{ source('raw', 'raw_pull_requests') }}
    union all
    select fetched_at from {{ source('raw', 'raw_pr_reviews') }}
    union all
    select fetched_at from {{ source('raw', 'raw_releases') }}
)

select
    max(fetched_at)                           as run_time,
    date_trunc('month', max(fetched_at))      as run_month,
    '{{ var("window_start") }}'::TIMESTAMPTZ  as window_start
from fetches
