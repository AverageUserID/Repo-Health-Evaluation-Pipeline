"""Task 6: save the dimension profile, monthly trends, and relationship plots to plots/."""

import duckdb
import matplotlib

matplotlib.use("Agg")  # headless: the flow never opens a window

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from prefect import task

from pipeline.config import DB_PATH, PLOT_DIR
from pipeline.logs import TeeLogger
from pipeline.model import WEIGHTS

# One fixed color per repo in every plot, keyed by name so it never depends on rank or
# plotting order. The three hues pass colorblind and normal-vision separation checks for
# every pair; repos are also always named (legend and direct labels), never color alone.
COLORS = {
    "scikit-learn/scikit-learn": "#2a78d6",  # blue
    "dask/dask": "#eb6834",  # orange
    "plotly/plotly.py": "#1baf7a",  # aqua
}
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"

DIMENSION_LABELS = {
    "responsiveness": "Responsiveness",
    "contributors": "Contributor base",
    "backlog": "Backlog",
    "throughput": "Throughput",
    "release_cadence": "Release cadence",
    "activity": "Activity",
}

# mart_repo_monthly column -> (panel title, y-axis label with units)
TREND_PANELS = {
    "commits": ("Commits", "commits / month"),
    "prs_opened": ("PRs opened", "PRs / month"),
    "prs_merged": ("PRs merged", "PRs / month"),
    "merge_ratio": ("Merge ratio", "merged / closed (0-1)"),
    "median_first_review_hours": ("Median time to first review", "hours"),
    "median_merge_hours": ("Median time to merge", "hours"),
    "active_contributors": ("Active PR authors", "authors / month"),
    "open_backlog_end": ("Open PR backlog (month end)", "open PRs"),
}

plt.rcParams.update({
    "font.size": 10,
    "axes.edgecolor": MUTED,
    "axes.labelcolor": INK,
    "axes.titlecolor": INK,
    "axes.titlesize": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "axes.axisbelow": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "legend.frameon": False,
    "figure.facecolor": "white",
    "savefig.dpi": 150,
    "savefig.bbox": "tight",
})


def short(repo: str) -> str:
    return repo.split("/")[1]


def rank_label(r) -> str:
    return f"{r.rank}. {short(r.repo)} ({r.score:.2f})"


def plot_profile(ranked: pd.DataFrame) -> plt.Figure:
    """Small multiples: one panel per dimension plus Overall, bars in rank order."""
    panels = {**{d: DIMENSION_LABELS[d] for d in WEIGHTS}, "score": "Overall score"}
    fig, axes = plt.subplots(1, len(panels), figsize=(16, 4.6), sharey=True)
    for ax, (col, title) in zip(axes, panels.items()):
        weight = f"weight {WEIGHTS[col]:.0%}" if col in WEIGHTS else "weighted mean"
        ax.set_title(f"{title}\n({weight})")
        for i, r in enumerate(ranked.itertuples()):
            value = getattr(r, col)
            if pd.isna(value):
                ax.text(i, 0.02, "n/a", ha="center", va="bottom", color=MUTED)
                continue
            ax.bar(i, value, width=0.75, color=COLORS[r.repo], edgecolor="white", linewidth=2)
            ax.text(i, value + 0.02, f"{value:.2f}", ha="center", va="bottom", color=INK,
                    fontsize=9)
        ax.set_xticks(range(len(ranked)), [f"#{r}" for r in ranked["rank"]])
        ax.set_ylim(0, 1.12)
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("Subscore (0-1, 1 = healthiest of the three)")
    handles = [plt.Rectangle((0, 0), 1, 1, color=COLORS[r.repo]) for r in ranked.itertuples()]
    fig.subplots_adjust(top=0.70, wspace=0.15)
    fig.legend(handles, [rank_label(r) for r in ranked.itertuples()], loc="upper center",
               ncol=len(ranked), bbox_to_anchor=(0.5, 0.92), title="Rank. repo (overall score)")
    fig.suptitle("Health profile by dimension, repos ordered healthiest to least healthy",
                 y=0.99, fontsize=13, color=INK)
    return fig


def plot_trends(monthly: pd.DataFrame, ranked: pd.DataFrame) -> plt.Figure:
    """One panel per monthly measure, one line per repo, complete months of the window."""
    fig, axes = plt.subplots(2, 4, figsize=(17, 7.5), sharex=True)
    for ax, (col, (title, unit)) in zip(axes.flat, TREND_PANELS.items()):
        for r in ranked.itertuples():
            g = monthly[monthly["repo"] == r.repo]
            ax.plot(g["month"], g[col], color=COLORS[r.repo], linewidth=2, marker="o",
                    markersize=3)
        ax.set_title(title)
        ax.set_ylabel(unit)
        ax.set_ylim(bottom=0)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=max(1, monthly["month"].nunique() // 8)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        ax.tick_params(axis="x", rotation=45)
    # Direct labels at the end of each line in the first panel.
    first = axes.flat[0]
    for r in ranked.itertuples():
        g = monthly[monthly["repo"] == r.repo].dropna(subset=["commits"])
        if len(g):
            last = g.iloc[-1]
            first.annotate(short(r.repo), (last["month"], last["commits"]), xytext=(4, 0),
                           textcoords="offset points", va="center", color=INK, fontsize=8)
    for ax in axes[1]:
        ax.set_xlabel("month (UTC)")
    handles = [plt.Line2D([], [], color=COLORS[r.repo], linewidth=2) for r in ranked.itertuples()]
    fig.legend(handles, [rank_label(r) for r in ranked.itertuples()], loc="upper center",
               ncol=len(ranked), bbox_to_anchor=(0.5, 0.97), title="Rank. repo (overall score)")
    start, end = monthly["month"].min(), monthly["month"].max()
    fig.suptitle(f"Monthly trends, {start:%b %Y} to {end:%b %Y} (complete months; PR measures "
                 "exclude bots)", y=1.0, fontsize=13, color=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    return fig


def plot_relationship(monthly: pd.DataFrame, overall: pd.DataFrame,
                      ranked: pd.DataFrame) -> plt.Figure:
    """Bubble per repo-month: x = median first review, y = merge ratio, size = active
    authors, color = repo; a large outlined marker per repo shows its whole-window value."""
    def size(authors):
        return 15 + 6 * authors

    fig, ax = plt.subplots(figsize=(10, 6.5))
    points = monthly.dropna(subset=["median_first_review_hours", "merge_ratio"])
    for r in ranked.itertuples():
        g = points[points["repo"] == r.repo]
        ax.scatter(g["median_first_review_hours"], g["merge_ratio"],
                   s=size(g["active_contributors"]), color=COLORS[r.repo], alpha=0.55,
                   edgecolor="white", linewidth=1, label=rank_label(r))
        o = overall[overall["repo"] == r.repo].iloc[0]
        if pd.notna(o["median_hours_to_first_review"]) and pd.notna(o["merge_rate"]):
            ax.scatter(o["median_hours_to_first_review"], o["merge_rate"],
                       s=size(o["active_authors_per_month"]), color=COLORS[r.repo],
                       edgecolor=INK, linewidth=2, zorder=3)
            # Clear the marker; alternate above/below by rank so nearby labels don't collide.
            offset = size(o["active_authors_per_month"]) ** 0.5 / 2 + 4
            above = r.rank % 2 == 1
            ax.annotate(f"{short(r.repo)} (window)",
                        (o["median_hours_to_first_review"], o["merge_rate"]),
                        xytext=(offset, offset if above else -offset), textcoords="offset points",
                        va="bottom" if above else "top", color=INK, fontsize=9,
                        fontweight="bold")
    ax.set_xlabel("Median time to first review (hours)")
    ax.set_ylabel("Merge ratio (merged / closed, 0-1)")
    ax.set_xlim(left=0)
    ax.set_ylim(0, 1.05)
    repo_legend = ax.legend(loc="lower left", title="Rank. repo (score); one bubble per month")
    for h in repo_legend.legend_handles:
        h.set_sizes([60])
        h.set_alpha(1)
    ax.add_artist(repo_legend)
    # Three round reference sizes spanning the data (multiples of 5 up to the max).
    top = max(5, int(points["active_contributors"].max() // 5 * 5))
    sizes = sorted({max(1, top // 4 // 5 * 5), max(1, top // 2 // 5 * 5), top})
    ax.legend([plt.scatter([], [], s=size(n), color=MUTED, alpha=0.55) for n in sizes],
              [f"{n} authors" for n in sizes], loc="lower right",
              title="Bubble size = active PR authors", labelspacing=1.4, borderpad=1)
    ax.set_title("Review speed vs. merge ratio by month; outlined = whole-window value",
                 fontsize=12)
    return fig


@task
def plot() -> list[str]:
    """Task 6. Read the marts and health_scores (nothing recomputed), save three PNGs."""
    log = TeeLogger("plot")
    # Not read_only, for the same reason as Task 4 (dbt-duckdb holds the database open).
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute("set TimeZone = 'UTC'")
        ranked = con.execute("select * from health_scores order by rank, repo").df()
        monthly = con.execute(f"""
            select repo, month, {', '.join(TREND_PANELS)}
            from mart_repo_monthly where not is_partial_month order by repo, month
        """).df()
        overall = con.execute("""
            select repo, median_hours_to_first_review, merge_rate, active_authors_per_month
            from mart_health_inputs
        """).df()

    PLOT_DIR.mkdir(exist_ok=True)
    figures = {
        "dimension_profile.png": plot_profile(ranked),
        "monthly_trends.png": plot_trends(monthly, ranked),
        "review_vs_merge.png": plot_relationship(monthly, overall, ranked),
    }
    paths = []
    for name, fig in figures.items():
        path = PLOT_DIR / name
        fig.savefig(path)
        plt.close(fig)
        paths.append(str(path))
        log.info(f"Saved {path}")
    return paths
