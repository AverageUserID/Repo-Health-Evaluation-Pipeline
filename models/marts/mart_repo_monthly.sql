-- Repo x month, from the window's first month through the run month. The spine is built
-- from the repos var and generate_series, so months with no activity show 0.
-- PR measures exclude bot-authored PRs (the same rule as the health metrics); each event
-- is counted in the month it happened.
with months as (
    select unnest(generate_series(
        date_trunc('month', rc.window_start), rc.run_month, interval 1 month
    )) as month
    from {{ ref('run_context') }} rc
),

spine as (
    select r.repo, m.month
    from {{ ref('repos') }} r
    cross join months m
),

human_prs as (
    select * from {{ ref('fct_pull_requests') }} where not is_bot
),

opened as (
    select
        repo,
        date_trunc('month', created_at)            as month,
        count(*)                                   as prs_opened,
        median(hours_to_first_review)              as median_first_review_hours,
        count(distinct author_login)               as active_contributors
    from human_prs
    group by all
),

merged as (
    select
        repo,
        date_trunc('month', merged_at)             as month,
        count(*)                                   as prs_merged,
        median(hours_to_merge)                     as median_merge_hours
    from human_prs
    where is_merged
    group by all
),

closed_unmerged as (
    select repo, date_trunc('month', closed_at) as month, count(*) as prs_closed_unmerged
    from human_prs
    where is_closed_unmerged
    group by all
),

commits as (
    select repo, date_trunc('month', committed_at) as month, count(*) as commits
    from {{ ref('stg_commits') }}
    group by all
),

monthly as (
    select
        s.repo,
        s.month,
        date_diff('month', date_trunc('month', rc.window_start), s.month)  as month_index,
        coalesce(o.prs_opened, 0)                                         as prs_opened,
        coalesce(m.prs_merged, 0)                                         as prs_merged,
        coalesce(c.prs_closed_unmerged, 0)                                as prs_closed_unmerged,
        m.prs_merged / nullif(m.prs_merged + coalesce(c.prs_closed_unmerged, 0), 0)
                                                                          as merge_ratio,
        o.median_first_review_hours,
        m.median_merge_hours,
        coalesce(k.commits, 0)                                            as commits,
        coalesce(o.active_contributors, 0)                                as active_contributors,
        s.month = rc.run_month                                            as is_partial_month
    from spine s
    cross join {{ ref('run_context') }} rc
    left join opened o          on o.repo = s.repo and o.month = s.month
    left join merged m          on m.repo = s.repo and m.month = s.month
    left join closed_unmerged c on c.repo = s.repo and c.month = s.month
    left join commits k         on k.repo = s.repo and k.month = s.month
)

select
    *,
    -- Open human PRs (created in the window) at the end of the month.
    sum(prs_opened - prs_merged - prs_closed_unmerged)
        over (partition by repo order by month)                           as open_backlog_end
from monthly
