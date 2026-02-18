"""Chart and table rendering utilities for the benchmark notebook."""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure

from lib.metrics import BenchmarkResult

# Color palette
COLORS = {
    "PostgreSQL": "#336791",
    "DuckDB": "#FFF000",
    "Graph (PostgreSQL)": "#008CC1",
    "Traditional Total": "#7C8A9A",
    "AXYM": "#1E1F22",
}

sns.set_theme(style="whitegrid", font_scale=1.1)


def comparison_table(results: list[BenchmarkResult]) -> pd.DataFrame:
    """Build a comparison DataFrame from benchmark results."""
    rows = []
    for r in results:
        rows.append({
            "System": r.name,
            "Wall Time (s)": round(r.wall_time_seconds, 1),
            "CPU User (s)": round(r.cpu_user_seconds, 1),
            "CPU System (s)": round(r.cpu_system_seconds, 1),
            "Peak Memory (MB)": round(r.peak_memory_mb, 0),
            "Disk (MB)": round(r.disk_mb, 1),
            "Rows": f"{r.row_count:,}",
            "Rows/sec": f"{r.rows_per_second:,.0f}",
        })
    return pd.DataFrame(rows).set_index("System")


def bar_chart_wall_time(results: list[BenchmarkResult]) -> Figure:
    """Horizontal bar chart of wall-clock ingestion time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    names = [r.name for r in results]
    times = [r.wall_time_seconds for r in results]
    colors = [COLORS.get(n, "#999999") for n in names]

    bars = ax.barh(names, times, color=colors, edgecolor="black", linewidth=0.5)
    ax.set_xlabel("Wall Time (seconds)")
    ax.set_title("Data Ingestion — Wall Clock Time")
    ax.invert_yaxis()

    for bar, t in zip(bars, times):
        ax.text(
            bar.get_width() + max(times) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{t:.1f}s",
            va="center",
            fontsize=10,
        )

    plt.tight_layout()
    return fig


def bar_chart_disk_footprint(results: list[BenchmarkResult]) -> Figure:
    """Horizontal bar chart of disk footprint."""
    fig, ax = plt.subplots(figsize=(10, 5))
    names = [r.name for r in results]
    disk = [r.disk_mb for r in results]
    colors = [COLORS.get(n, "#999999") for n in names]

    bars = ax.barh(names, disk, color=colors, edgecolor="black", linewidth=0.5)
    ax.set_xlabel("Disk Footprint (MB)")
    ax.set_title("Data Ingestion — Disk Footprint")
    ax.invert_yaxis()

    for bar, d in zip(bars, disk):
        label = f"{d:,.0f} MB" if d >= 1 else "N/A"
        ax.text(
            bar.get_width() + max(disk) * 0.01 if max(disk) > 0 else 1,
            bar.get_y() + bar.get_height() / 2,
            label,
            va="center",
            fontsize=10,
        )

    plt.tight_layout()
    return fig


def stacked_bar_traditional_vs_axym(
    traditional_results: list[BenchmarkResult],
    axym_result: BenchmarkResult | None = None,
) -> Figure:
    """Stacked bar: traditional stack (PG + DuckDB + Graph) vs AXYM."""
    fig, ax = plt.subplots(figsize=(10, 4))

    # Traditional stack — stacked components
    bottom = 0.0
    for r in traditional_results:
        color = COLORS.get(r.name, "#999999")
        ax.barh(
            "Traditional Stack",
            r.wall_time_seconds,
            left=bottom,
            color=color,
            edgecolor="black",
            linewidth=0.5,
            label=r.name,
        )
        bottom += r.wall_time_seconds

    # AXYM
    if axym_result and axym_result.error is None:
        ax.barh(
            "AXYM",
            axym_result.wall_time_seconds,
            color=COLORS["AXYM"],
            edgecolor="black",
            linewidth=0.5,
            label="AXYM",
        )
    else:
        ax.barh(
            "AXYM",
            0,
            color=COLORS["AXYM"],
            edgecolor="black",
            linewidth=0.5,
            label="AXYM (pending)",
        )
        ax.text(1, 1, "Pending — CLI not yet available", va="center", fontsize=10, style="italic")

    ax.set_xlabel("Wall Time (seconds)")
    ax.set_title("Traditional Stack vs. AXYM — Total Ingestion Time")
    ax.legend(loc="lower right")
    ax.invert_yaxis()
    plt.tight_layout()
    return fig


def complexity_summary(traditional_results: list[BenchmarkResult]) -> pd.DataFrame:
    """Summarize operational complexity of each approach."""
    rows = [
        {
            "Metric": "Ingestion scripts",
            "Traditional Stack": "3",
            "AXYM": "1",
        },
        {
            "Metric": "Query languages",
            "Traditional Stack": "1 (SQL + recursive CTEs)",
            "AXYM": "1",
        },
        {
            "Metric": "Docker containers",
            "Traditional Stack": "0 (all hosted)",
            "AXYM": "0",
        },
        {
            "Metric": "ETL pipelines",
            "Traditional Stack": "1 (tabular → graph)",
            "AXYM": "0",
        },
        {
            "Metric": "Total wall time (s)",
            "Traditional Stack": f"{sum(r.wall_time_seconds for r in traditional_results):.1f}",
            "AXYM": "Pending",
        },
        {
            "Metric": "Total disk (MB)",
            "Traditional Stack": f"{sum(r.disk_mb for r in traditional_results):,.0f}",
            "AXYM": "Pending",
        },
    ]
    return pd.DataFrame(rows).set_index("Metric")
