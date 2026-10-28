-- One row per PR created in the window, with precalculated flags and durations.
-- Open PRs are kept (is_open) with NULL durations; they never get a fake close date.
with prs as (
    select p.*, rc.run_time
    from {{ ref('stg_pull_requests') }} p
    cross join {{ ref('run_context') }} rc
    where p.created_at >= rc.window_start
),

-- Reviews that count: not by a bot, not by the PR's own author.
review_summary as (
    select
        r.pr_key,
        min(r.submitted_at)               as first_review_at,
        bool_or(r.state = 'APPROVED')     as has_approving_review
    from {{ ref('stg_pr_reviews') }} r
    join prs p on p.pr_key = r.pr_key
    where not r.is_bot
      and r.reviewer_login is distinct from p.author_login
    group by r.pr_key
),

flagged as (
    select
        p.repo,
        p.number,
        p.pr_key,
        p.author_login,
        p.is_bot,
        p.state,
        p.created_at,
        p.updated_at,
        p.closed_at,
        p.merged_at,
        p.merged_at is not null                           as is_merged,
        p.state = 'closed' and p.merged_at is null        as is_closed_unmerged,
        p.state = 'open'                                  as is_open,
        rs.first_review_at,
        date_diff('second', p.created_at, rs.first_review_at) / 3600.0
                                                          as hours_to_first_review,
        date_diff('second', p.created_at, p.merged_at) / 3600.0
                                                          as hours_to_merge,
        case when p.state <> 'open'
             then date_diff('second', p.created_at, p.closed_at) / 3600.0
        end                                               as hours_to_close,
        coalesce(rs.has_approving_review, false)          as has_approving_review,
        p.run_time
    from prs p
    left join review_summary rs on rs.pr_key = p.pr_key
)

select
    * exclude (run_time),
    coalesce(hours_to_first_review <= 168, false)          as reviewed_within_7d,
    is_open and updated_at < run_time - interval 180 day   as is_stale
from flagged
