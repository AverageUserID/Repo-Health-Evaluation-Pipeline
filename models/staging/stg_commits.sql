-- One row per commit: typed fields from the raw payload, latest fetch per (repo, sha).
with latest as (
    select *
    from {{ source('raw', 'raw_commits') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id                                                       as sha,
    -- NULL when the commit email isn't linked to a GitHub account.
    payload->'author'->>'login'                              as author_login,
    {{ is_bot("payload->'author'") }}                        as is_bot,
    -- When the change was written (the README's spec); buckets commits into months.
    (payload->'commit'->'author'->>'date')::TIMESTAMPTZ      as committed_at,
    -- When it landed on the branch; what Task 1's `since` filter and watermark use.
    (payload->'commit'->'committer'->>'date')::TIMESTAMPTZ   as committer_at,
    fetched_at
from latest
