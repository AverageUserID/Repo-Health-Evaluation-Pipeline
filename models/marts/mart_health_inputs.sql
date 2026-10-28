-- One row per repo: every candidate health metric for Task 5. Each is a rate, share,
-- median, or size-relative slope, never a raw count. PR metrics exclude bot-authored PRs.
-- Trends use complete months only and are divided by the repo's own monthly average,
-- so they read as "% change per month" and don't scale with project size.
with rc as (
    select
        *,
        date_diff('day', window_start, run_time)            as window_days
    from {{ ref('run_context') }}
),

human_prs as (
    select * from {{ ref('fct_pull_requests') }} where not is_bot
),

activity as (
    select
        c.repo,
        count(*) / (any_value(rc.window_days) / 7.0)        as commits_per_week
    from {{ ref('stg_commits') }} c
    cross join rc
    where c.committed_at >= rc.window_start
    group by c.repo
),

pr_metrics as (
    select
        p.repo,
        -- Responsiveness. Medians cover reviewed/merged PRs only (NULLs drop out); the
        -- 7-day share counts never-reviewed PRs as "no", so waiting PRs still count.
        median(p.hours_to_first_review)                     as median_hours_to_first_review,
        avg(p.reviewed_within_7d::INT)
            filter (where p.created_at <= rc.run_time - interval 7 day)
                                                            as pct_reviewed_within_7d,
        avg(p.has_approving_review::INT) filter (where p.is_merged)
                                                            as pct_merged_with_approval,
        -- Throughput
        count(*) filter (where p.is_merged)
            / nullif(count(*) filter (where p.is_merged or p.is_closed_unmerged), 0)
                                                            as merge_rate,
        count(*) filter (where p.is_merged) / count(*)      as merged_per_opened,
        median(p.hours_to_merge)                            as median_hours_to_merge,
        -- Backlog
        avg(p.is_stale::INT) filter (where p.is_open)       as pct_open_stale
    from human_prs p
    cross join rc
    group by p.repo
),

trends as (
    select
        repo,
        regr_slope(commits, month_index) / nullif(avg(commits), 0)
                                                            as commit_trend_pct,
        regr_slope(open_backlog_end, month_index) / nullif(avg(prs_opened), 0)
                                                            as open_backlog_growth_pct,
        median(active_contributors)                         as active_authors_per_month,
        regr_slope(active_contributors, month_index) / nullif(avg(active_contributors), 0)
                                                            as author_trend_pct
    from {{ ref('mart_repo_monthly') }}
    where not is_partial_month
    group by repo
),

releases as (
    select
        r.repo,
        count(*) / (any_value(rc.window_days) / 365.25)     as releases_per_year,
        stddev_samp(r.days_since_previous) / nullif(avg(r.days_since_previous), 0)
                                                            as release_interval_cv
    from {{ ref('fct_releases') }} r
    cross join rc
    group by r.repo
),

-- Merged human PRs per author, ranked, to measure how concentrated the work is.
author_merges as (
    select
        repo,
        author_login,
        count(*)                                            as merged,
        sum(count(*)) over (partition by repo)              as repo_merged,
        sum(count(*)) over (
            partition by repo order by count(*) desc, author_login
            rows unbounded preceding
        )                                                   as cumulative_merged,
        row_number() over (
            partition by repo order by count(*) desc, author_login
        )                                                   as author_rank
    from human_prs
    where is_merged
    group by repo, author_login
),

concentration as (
    select
        repo,
        max(merged) / any_value(repo_merged)                as top_author_share,
        -- Fewest authors who together account for at least half of merged PRs.
        min(author_rank) filter (where cumulative_merged >= 0.5 * repo_merged)
                                                            as bus_factor
    from author_merges
    group by repo
)

select
    r.repo,
    a.commits_per_week,
    t.commit_trend_pct,
    p.median_hours_to_first_review,
    p.pct_reviewed_within_7d,
    p.pct_merged_with_approval,
    p.merge_rate,
    p.merged_per_opened,
    p.median_hours_to_merge,
    p.pct_open_stale,
    t.open_backlog_growth_pct,
    rel.releases_per_year,
    rel.release_interval_cv,
    t.active_authors_per_month,
    c.top_author_share,
    c.bus_factor,
    t.author_trend_pct
from {{ ref('repos') }} r
left join activity a      on a.repo = r.repo
left join pr_metrics p    on p.repo = r.repo
left join trends t        on t.repo = r.repo
left join releases rel    on rel.repo = r.repo
left join concentration c on c.repo = r.repo
