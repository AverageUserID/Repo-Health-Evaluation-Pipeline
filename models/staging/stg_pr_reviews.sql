-- One row per submitted review, latest fetch per (repo, id). PENDING reviews are drafts
-- with no submitted_at, so they are dropped.
with latest as (
    select *
    from {{ source('raw', 'raw_pr_reviews') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
),

parsed as (
    select
        repo,
        id                                                                    as review_id,
        -- pull_request_url ends in /pulls/<number>
        regexp_extract(payload->>'pull_request_url', '/pulls/(\d+)$', 1)::BIGINT as pr_number,
        payload->'user'->>'login'                                             as reviewer_login,
        {{ is_bot("payload->'user'") }}                                       as is_bot,
        payload->>'state'                                                     as state,
        (payload->>'submitted_at')::TIMESTAMPTZ                               as submitted_at,
        fetched_at
    from latest
    where (payload->>'state') <> 'PENDING'
)

select
    *,
    repo || '#' || pr_number as pr_key
from parsed
