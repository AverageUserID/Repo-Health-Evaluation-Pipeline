-- One row per published release in the window. The gap to the previous release is
-- computed over ALL releases before filtering, so the first release in the window still
-- measures its gap to the one before it.
with ordered as (
    select
        *,
        lag(published_at) over (partition by repo order by published_at) as previous_published_at
    from {{ ref('stg_releases') }}
)

select
    o.repo,
    o.release_id,
    o.tag_name,
    o.published_at,
    o.prerelease                                                    as is_prerelease,
    date_diff('day', o.previous_published_at, o.published_at)       as days_since_previous
from ordered o
cross join {{ ref('run_context') }} rc
where o.published_at >= rc.window_start
