-- One row per published release, latest fetch per (repo, id). Drafts are unpublished
-- (no published_at), so they are dropped.
with latest as (
    select *
    from {{ source('raw', 'raw_releases') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id                                          as release_id,
    payload->>'tag_name'                        as tag_name,
    (payload->>'published_at')::TIMESTAMPTZ     as published_at,
    (payload->>'prerelease')::BOOLEAN           as prerelease,
    fetched_at
from latest
where not coalesce((payload->>'draft')::BOOLEAN, false)
