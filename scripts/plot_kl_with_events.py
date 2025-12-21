#!/usr/bin/env python3
"""
Visualization of KL Divergence Analysis with Major Crypto Events.

Generates plots showing how KL divergence evolves over time, with vertical
markers for major cryptocurrency events (halvings, exchange hacks, etc.)
and zoomed-in views around each event.

Usage:
    python scripts/plot_kl_with_events.py --input-dir results/kl-analysis-highres --output-dir plots/kl-analysis-highres
    python scripts/plot_kl_with_events.py --input-dir results/kl-analysis-highres --output-dir plots/kl-analysis-highres --start-year 2012
"""

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd


# ============================================================================
# Plot Configuration
# ============================================================================

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams.update(
    {
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "font.size": 11,
        "axes.labelsize": 12,
        "axes.titlesize": 14,
        "legend.fontsize": 10,
    }
)

CENTRALITY_COLORS = {
    "kl_in_degree": "#1f77b4",
    "kl_out_degree": "#ff7f0e",
    "kl_degree": "#2ca02c",
    "kl_clustering": "#d62728",
}

CENTRALITY_LABELS = {
    "kl_in_degree": "In-Degree",
    "kl_out_degree": "Out-Degree",
    "kl_degree": "Total Degree",
    "kl_clustering": "Clustering Coefficient",
}

PROPERTY_COLORS = {
    "num_vertices": "#1f77b4",
    "num_edges": "#ff7f0e",
    "density": "#2ca02c",
    "global_cc": "#d62728",
    "avg_cc": "#9467bd",
}


# ============================================================================
# Crypto Events
# ============================================================================


@dataclass
class CryptoEvent:
    """Represents a major cryptocurrency event."""

    name: str
    date: datetime
    short_name: str
    color: str
    description: str
    category: str  # 'halving', 'hack', 'regulatory'


CRYPTO_EVENTS: List[CryptoEvent] = [
    CryptoEvent(
        name="Bitcoin Halving #1",
        date=datetime(2012, 11, 28),
        short_name="Halving 1",
        color="#9467bd",
        description="Block reward reduced from 50 to 25 BTC",
        category="halving",
    ),
    CryptoEvent(
        name="Silk Road FBI Shutdown",
        date=datetime(2013, 10, 2),
        short_name="Silk Road",
        color="#e377c2",
        description="FBI seized ~144,000 BTC from dark web marketplace",
        category="regulatory",
    ),
    CryptoEvent(
        name="Mt. Gox Hack",
        date=datetime(2014, 2, 24),
        short_name="Mt. Gox",
        color="#d62728",
        description="~850,000 BTC stolen from largest exchange",
        category="hack",
    ),
    CryptoEvent(
        name="Bitfinex Hack",
        date=datetime(2016, 8, 2),
        short_name="Bitfinex",
        color="#d62728",
        description="119,756 BTC stolen (~$72M at time)",
        category="hack",
    ),
    CryptoEvent(
        name="Bitcoin Halving #2",
        date=datetime(2016, 7, 9),
        short_name="Halving 2",
        color="#9467bd",
        description="Block reward reduced from 25 to 12.5 BTC",
        category="halving",
    ),
    CryptoEvent(
        name="Bitcoin Halving #3",
        date=datetime(2020, 5, 11),
        short_name="Halving 3",
        color="#9467bd",
        description="Block reward reduced from 12.5 to 6.25 BTC",
        category="halving",
    ),
]


# ============================================================================
# Data Loading
# ============================================================================


def load_results(input_dir: str) -> tuple:
    """Load analysis results from CSV files."""
    global_df = pd.read_csv(os.path.join(input_dir, "global_properties.csv"))
    global_df["date"] = pd.to_datetime(global_df["date"])

    kl_df = pd.read_csv(os.path.join(input_dir, "kl_divergences.csv"))
    kl_df["date"] = pd.to_datetime(kl_df["date"])

    corr_df = pd.read_csv(os.path.join(input_dir, "kl_correlations.csv"))
    corr_df["date"] = pd.to_datetime(corr_df["date"])

    summary_path = os.path.join(input_dir, "summary.json")
    summary = json.load(open(summary_path)) if os.path.exists(summary_path) else {}

    return global_df, kl_df, corr_df, summary


# ============================================================================
# Plotting Functions
# ============================================================================


def add_event_markers(
    ax,
    events: List[CryptoEvent],
    y_range: tuple,
    show_labels: bool = True,
    rotation: int = 90,
):
    """Add vertical lines and labels for crypto events."""
    for event in events:
        ax.axvline(
            x=event.date,
            color=event.color,
            linestyle="--",
            linewidth=2,
            alpha=0.8,
            zorder=5,
        )
        if show_labels:
            ax.text(
                event.date,
                y_range[1] * 0.95,
                f" {event.short_name}",
                rotation=rotation,
                va="top",
                ha="left",
                fontsize=9,
                color=event.color,
                fontweight="bold",
                zorder=6,
            )


def plot_kl_evolution_with_events(kl_df: pd.DataFrame, output_dir: str):
    """Plot KL divergence evolution with event markers."""
    fig, axes = plt.subplots(2, 2, figsize=(18, 14))
    axes = axes.flatten()

    centralities = ["kl_in_degree", "kl_out_degree", "kl_degree", "kl_clustering"]

    date_min, date_max = kl_df["date"].min(), kl_df["date"].max()
    events_in_range = [e for e in CRYPTO_EVENTS if date_min <= e.date <= date_max]

    for ax, cent in zip(axes, centralities):
        color = CENTRALITY_COLORS[cent]
        label = CENTRALITY_LABELS[cent]

        valid = kl_df.dropna(subset=[cent])
        ax.plot(valid["date"], valid[cent], color=color, linewidth=1.2, alpha=0.9)
        ax.fill_between(valid["date"], valid[cent], alpha=0.3, color=color)

        y_max = valid[cent].max()
        y_range = (0, y_max * 1.1)
        ax.set_ylim(y_range)

        add_event_markers(ax, events_in_range, y_range)

        ax.set_xlabel("Date")
        ax.set_ylabel("KL Divergence D(P_t || P_{t+1})")
        ax.set_title(f"{label}: Change Between Consecutive Snapshots")
        ax.grid(True, alpha=0.3)

        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Legend for event categories
    legend_elements = []
    categories = set(e.category for e in events_in_range)
    if "halving" in categories:
        legend_elements.append(
            mpatches.Patch(color="#9467bd", label="Bitcoin Halvings", alpha=0.8)
        )
    if "hack" in categories:
        legend_elements.append(
            mpatches.Patch(color="#d62728", label="Exchange Hacks", alpha=0.8)
        )
    if "regulatory" in categories:
        legend_elements.append(
            mpatches.Patch(color="#e377c2", label="Regulatory Events", alpha=0.8)
        )

    fig.legend(handles=legend_elements, loc="upper right", bbox_to_anchor=(0.98, 0.98))
    plt.suptitle(
        "KL Divergence Evolution with Major Crypto Events", fontsize=16, y=1.02
    )
    plt.tight_layout()

    plt.savefig(
        os.path.join(output_dir, "kl_evolution_with_events.png"),
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()
    print("Saved: kl_evolution_with_events.png")


def plot_global_properties_with_events(global_df: pd.DataFrame, output_dir: str):
    """Plot global network properties with event markers."""
    fig, axes = plt.subplots(3, 1, figsize=(16, 14), sharex=True)

    date_min, date_max = global_df["date"].min(), global_df["date"].max()
    events_in_range = [e for e in CRYPTO_EVENTS if date_min <= e.date <= date_max]

    # Network size
    ax1 = axes[0]
    ax1.plot(
        global_df["date"],
        global_df["num_vertices"],
        color=PROPERTY_COLORS["num_vertices"],
        linewidth=1.5,
        label="Vertices",
    )
    ax1.plot(
        global_df["date"],
        global_df["num_edges"],
        color=PROPERTY_COLORS["num_edges"],
        linewidth=1.5,
        label="Edges",
    )
    ax1.set_ylabel("Count")
    ax1.set_title("Network Size Evolution")
    ax1.legend(loc="upper left")
    ax1.set_yscale("log")
    y_range = (global_df["num_vertices"].min(), global_df["num_edges"].max() * 1.5)
    add_event_markers(ax1, events_in_range, y_range, rotation=0)
    ax1.grid(True, alpha=0.3)

    # Density
    ax2 = axes[1]
    ax2.plot(
        global_df["date"],
        global_df["density"],
        color=PROPERTY_COLORS["density"],
        linewidth=1.5,
    )
    ax2.set_ylabel("Density")
    ax2.set_title("Network Density Evolution")
    y_range = (0, global_df["density"].max() * 1.2)
    ax2.set_ylim(y_range)
    add_event_markers(ax2, events_in_range, y_range, rotation=0)
    ax2.grid(True, alpha=0.3)

    # Clustering
    ax3 = axes[2]
    ax3.plot(
        global_df["date"],
        global_df["global_cc"],
        color=PROPERTY_COLORS["global_cc"],
        linewidth=1.5,
        label="Global CC",
    )
    ax3.plot(
        global_df["date"],
        global_df["avg_cc"],
        color=PROPERTY_COLORS["avg_cc"],
        linewidth=1.5,
        label="Avg Local CC",
    )
    ax3.set_xlabel("Date")
    ax3.set_ylabel("Clustering Coefficient")
    ax3.set_title("Clustering Coefficient Evolution")
    ax3.legend(loc="upper right")
    y_range = (0, max(global_df["global_cc"].max(), global_df["avg_cc"].max()) * 1.2)
    ax3.set_ylim(y_range)
    add_event_markers(ax3, events_in_range, y_range, rotation=0)
    ax3.grid(True, alpha=0.3)

    for ax in axes:
        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    plt.suptitle(
        "Global Network Properties with Major Crypto Events", fontsize=16, y=1.02
    )
    plt.tight_layout()

    plt.savefig(
        os.path.join(output_dir, "global_properties_with_events.png"),
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()
    print("Saved: global_properties_with_events.png")


def plot_event_zoom(
    kl_df: pd.DataFrame,
    global_df: pd.DataFrame,
    event: CryptoEvent,
    output_dir: str,
    weeks_before: int = 4,
    weeks_after: int = 4,
):
    """Generate zoomed-in plot around a specific event (±weeks)."""
    start_date = event.date - timedelta(weeks=weeks_before)
    end_date = event.date + timedelta(weeks=weeks_after)

    kl_zoom = kl_df[(kl_df["date"] >= start_date) & (kl_df["date"] <= end_date)].copy()
    global_zoom = global_df[
        (global_df["date"] >= start_date) & (global_df["date"] <= end_date)
    ].copy()

    if len(kl_zoom) < 3:
        print(f"  Skipping {event.short_name}: insufficient data in range")
        return

    fig = plt.figure(figsize=(18, 16))
    centralities = ["kl_in_degree", "kl_out_degree", "kl_degree", "kl_clustering"]

    # KL divergence plots (2x2 grid)
    for i, cent in enumerate(centralities):
        ax = fig.add_subplot(3, 2, i + 1)
        color = CENTRALITY_COLORS[cent]
        label = CENTRALITY_LABELS[cent]

        valid = kl_zoom.dropna(subset=[cent])
        if len(valid) > 0:
            ax.plot(
                valid["date"],
                valid[cent],
                color=color,
                linewidth=2,
                marker="o",
                markersize=4,
                alpha=0.9,
            )
            ax.fill_between(valid["date"], valid[cent], alpha=0.3, color=color)

            y_max = valid[cent].max()
            ax.set_ylim(0, y_max * 1.15)
            ax.axvline(
                x=event.date,
                color=event.color,
                linestyle="--",
                linewidth=3,
                alpha=0.9,
                label=event.short_name,
            )
            ax.axvspan(
                event.date - timedelta(days=3),
                event.date + timedelta(days=3),
                alpha=0.2,
                color=event.color,
            )

        ax.set_xlabel("Date")
        ax.set_ylabel("KL Divergence")
        ax.set_title(label)
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Network size
    ax5 = fig.add_subplot(3, 2, 5)
    ax5.plot(
        global_zoom["date"],
        global_zoom["num_vertices"],
        color=PROPERTY_COLORS["num_vertices"],
        linewidth=2,
        marker="o",
        markersize=4,
        label="Vertices",
    )
    ax5.plot(
        global_zoom["date"],
        global_zoom["num_edges"],
        color=PROPERTY_COLORS["num_edges"],
        linewidth=2,
        marker="s",
        markersize=4,
        label="Edges",
    )
    ax5.axvline(x=event.date, color=event.color, linestyle="--", linewidth=3, alpha=0.9)
    ax5.axvspan(
        event.date - timedelta(days=3),
        event.date + timedelta(days=3),
        alpha=0.2,
        color=event.color,
    )
    ax5.set_xlabel("Date")
    ax5.set_ylabel("Count")
    ax5.set_title("Network Size")
    ax5.legend(loc="upper left")
    ax5.grid(True, alpha=0.3)
    ax5.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    ax5.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Clustering
    ax6 = fig.add_subplot(3, 2, 6)
    ax6.plot(
        global_zoom["date"],
        global_zoom["global_cc"],
        color=PROPERTY_COLORS["global_cc"],
        linewidth=2,
        marker="o",
        markersize=4,
        label="Global CC",
    )
    ax6.plot(
        global_zoom["date"],
        global_zoom["avg_cc"],
        color=PROPERTY_COLORS["avg_cc"],
        linewidth=2,
        marker="s",
        markersize=4,
        label="Avg Local CC",
    )
    ax6.axvline(x=event.date, color=event.color, linestyle="--", linewidth=3, alpha=0.9)
    ax6.axvspan(
        event.date - timedelta(days=3),
        event.date + timedelta(days=3),
        alpha=0.2,
        color=event.color,
    )
    ax6.set_xlabel("Date")
    ax6.set_ylabel("Clustering Coefficient")
    ax6.set_title("Clustering Coefficients")
    ax6.legend(loc="upper right")
    ax6.grid(True, alpha=0.3)
    ax6.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    ax6.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.setp(ax6.xaxis.get_majorticklabels(), rotation=45, ha="right")

    plt.suptitle(
        f"{event.name}\n{event.date.strftime('%B %d, %Y')} - {event.description}",
        fontsize=16,
        y=1.02,
    )
    plt.tight_layout()

    safe_name = event.short_name.lower().replace(" ", "_").replace(".", "")
    plt.savefig(
        os.path.join(output_dir, f"event_zoom_{safe_name}.png"),
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()
    print(f"Saved: event_zoom_{safe_name}.png")


def plot_events_impact_summary(kl_df: pd.DataFrame, output_dir: str):
    """Create summary bar chart comparing event impacts."""
    date_min, date_max = kl_df["date"].min(), kl_df["date"].max()
    events_in_range = [e for e in CRYPTO_EVENTS if date_min <= e.date <= date_max]

    if not events_in_range:
        print("No events in data range, skipping summary")
        return

    # Calculate before/after statistics
    stats = []
    for event in events_in_range:
        before = kl_df[
            (kl_df["date"] >= event.date - timedelta(weeks=4))
            & (kl_df["date"] < event.date)
        ]
        after = kl_df[
            (kl_df["date"] > event.date)
            & (kl_df["date"] <= event.date + timedelta(weeks=4))
        ]

        if len(before) > 0 and len(after) > 0:
            stat = {
                "event": event.short_name,
                "date": event.date,
                "category": event.category,
                "color": event.color,
            }
            for cent in ["kl_in_degree", "kl_out_degree", "kl_degree", "kl_clustering"]:
                before_mean = before[cent].mean()
                after_mean = after[cent].mean()
                change = (
                    ((after_mean - before_mean) / before_mean * 100)
                    if before_mean > 0
                    else 0
                )
                stat[f"{cent}_before"] = before_mean
                stat[f"{cent}_after"] = after_mean
                stat[f"{cent}_change"] = change
            stats.append(stat)

    if not stats:
        print("No events with sufficient data, skipping summary")
        return

    stats_df = pd.DataFrame(stats)

    # Bar chart
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    axes = axes.flatten()

    for ax, cent in zip(
        axes, ["kl_in_degree", "kl_out_degree", "kl_degree", "kl_clustering"]
    ):
        label = CENTRALITY_LABELS[cent]
        x = range(len(stats_df))
        bars = ax.bar(
            x,
            stats_df[f"{cent}_change"],
            color=stats_df["color"].tolist(),
            alpha=0.8,
            edgecolor="black",
        )

        ax.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
        ax.set_xticks(x)
        ax.set_xticklabels(stats_df["event"], rotation=45, ha="right")
        ax.set_ylabel("Change in KL Divergence (%)")
        ax.set_title(f"{label}: Before vs After Event")
        ax.grid(True, alpha=0.3, axis="y")

        for bar, val in zip(bars, stats_df[f"{cent}_change"]):
            height = bar.get_height()
            ax.annotate(
                f"{val:.1f}%",
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3 if height >= 0 else -10),
                textcoords="offset points",
                ha="center",
                va="bottom" if height >= 0 else "top",
                fontsize=9,
                fontweight="bold",
            )

    plt.suptitle(
        "Impact of Major Crypto Events on KL Divergence\n(% change: 4 weeks before vs 4 weeks after)",
        fontsize=16,
        y=1.02,
    )
    plt.tight_layout()

    plt.savefig(
        os.path.join(output_dir, "events_impact_summary.png"),
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()
    print("Saved: events_impact_summary.png")

    # Save statistics CSV
    stats_df.to_csv(os.path.join(output_dir, "events_statistics.csv"), index=False)
    print("Saved: events_statistics.csv")


def plot_correlations(corr_df: pd.DataFrame, output_dir: str):
    """Plot rolling correlations between KL divergences over time."""
    # Define correlation pairs and their display names
    corr_pairs = [
        ("corr_kl_in_degree_kl_out_degree", "In-Degree vs Out-Degree"),
        ("corr_kl_in_degree_kl_degree", "In-Degree vs Total Degree"),
        ("corr_kl_out_degree_kl_degree", "Out-Degree vs Total Degree"),
        ("corr_kl_in_degree_kl_clustering", "In-Degree vs Clustering"),
        ("corr_kl_out_degree_kl_clustering", "Out-Degree vs Clustering"),
        ("corr_kl_degree_kl_clustering", "Total Degree vs Clustering"),
    ]

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]

    fig, axes = plt.subplots(2, 1, figsize=(16, 10))

    # Plot 1: Degree correlations
    ax1 = axes[0]
    degree_pairs = corr_pairs[:3]
    for (col, label), color in zip(degree_pairs, colors[:3]):
        if col in corr_df.columns:
            valid = corr_df[col].notna()
            ax1.plot(
                corr_df.loc[valid, "date"],
                corr_df.loc[valid, col],
                label=label,
                color=color,
                linewidth=1.5,
                alpha=0.8,
            )

    ax1.set_ylabel("Rolling Correlation", fontsize=12)
    ax1.set_title(
        "Rolling Correlations Between Degree-Based KL Divergences", fontsize=14
    )
    ax1.legend(loc="upper left", fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax1.xaxis.set_major_locator(mdates.YearLocator())

    # Add event markers
    for event in CRYPTO_EVENTS:
        if corr_df["date"].min() <= event.date <= corr_df["date"].max():
            ax1.axvline(
                x=event.date,
                color=event.color,
                linestyle="--",
                linewidth=1.5,
                alpha=0.6,
            )

    # Plot 2: Clustering correlations
    ax2 = axes[1]
    clustering_pairs = corr_pairs[3:]
    for (col, label), color in zip(clustering_pairs, colors[3:]):
        if col in corr_df.columns:
            valid = corr_df[col].notna()
            ax2.plot(
                corr_df.loc[valid, "date"],
                corr_df.loc[valid, col],
                label=label,
                color=color,
                linewidth=1.5,
                alpha=0.8,
            )

    ax2.set_xlabel("Date", fontsize=12)
    ax2.set_ylabel("Rolling Correlation", fontsize=12)
    ax2.set_title(
        "Rolling Correlations Between Degree and Clustering KL Divergences", fontsize=14
    )
    ax2.legend(loc="upper left", fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax2.xaxis.set_major_locator(mdates.YearLocator())

    for event in CRYPTO_EVENTS:
        if corr_df["date"].min() <= event.date <= corr_df["date"].max():
            ax2.axvline(
                x=event.date,
                color=event.color,
                linestyle="--",
                linewidth=1.5,
                alpha=0.6,
            )

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "kl_correlations.png"), dpi=150, bbox_inches="tight"
    )
    plt.close()
    print("Saved: kl_correlations.png")

    # Also create a correlation heatmap for the final snapshot
    plot_correlation_heatmap(corr_df, output_dir)


def plot_correlation_heatmap(corr_df: pd.DataFrame, output_dir: str):
    """Plot heatmap of final correlations between KL divergences."""
    # Get the last valid row with correlation data
    last_valid = corr_df.dropna(subset=["corr_kl_in_degree_kl_out_degree"]).iloc[-1]

    # Build correlation matrix
    labels = ["In-Deg", "Out-Deg", "Degree", "Clustering"]
    n = len(labels)
    corr_matrix = np.ones((n, n))

    # Fill in the correlation values
    mapping = {
        (0, 1): "corr_kl_in_degree_kl_out_degree",
        (0, 2): "corr_kl_in_degree_kl_degree",
        (0, 3): "corr_kl_in_degree_kl_clustering",
        (1, 2): "corr_kl_out_degree_kl_degree",
        (1, 3): "corr_kl_out_degree_kl_clustering",
        (2, 3): "corr_kl_degree_kl_clustering",
    }

    for (i, j), col in mapping.items():
        if col in last_valid.index:
            val = last_valid[col]
            corr_matrix[i, j] = val
            corr_matrix[j, i] = val

    fig, ax = plt.subplots(figsize=(8, 7))

    im = ax.imshow(corr_matrix, cmap="RdYlBu_r", vmin=-1, vmax=1)

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_yticklabels(labels, fontsize=11)

    # Add correlation values as text
    for i in range(n):
        for j in range(n):
            color = "white" if abs(corr_matrix[i, j]) > 0.5 else "black"
            ax.text(
                j,
                i,
                f"{corr_matrix[i, j]:.3f}",
                ha="center",
                va="center",
                color=color,
                fontsize=12,
                fontweight="bold",
            )

    plt.colorbar(im, ax=ax, label="Correlation", shrink=0.8)
    ax.set_title(
        f"KL Divergence Correlations\n(as of {last_valid['date'].strftime('%Y-%m-%d')})",
        fontsize=14,
    )

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "kl_correlation_heatmap.png"),
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()
    print("Saved: kl_correlation_heatmap.png")


def create_events_reference(output_dir: str):
    """Create markdown reference table of events."""
    table = "# Major Cryptocurrency Events Analyzed\n\n"
    table += "| Event | Date | Category | Description |\n"
    table += "|-------|------|----------|-------------|\n"
    for event in CRYPTO_EVENTS:
        table += f"| {event.name} | {event.date.strftime('%Y-%m-%d')} | {event.category.title()} | {event.description} |\n"

    with open(os.path.join(output_dir, "events_reference.md"), "w") as f:
        f.write(table)
    print("Saved: events_reference.md")


# ============================================================================
# Main
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Plot KL Divergence with Crypto Events"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing analysis results",
    )
    parser.add_argument(
        "--output-dir", type=str, required=True, help="Directory for output plots"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Filter data from this year onwards",
    )
    parser.add_argument(
        "--weeks-around-event",
        type=int,
        default=4,
        help="Weeks to show around each event (default: 4)",
    )

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Loading results from {args.input_dir}...")
    global_df, kl_df, corr_df, summary = load_results(args.input_dir)
    print(f"Loaded {len(global_df)} snapshots")
    print(f"Date range: {global_df['date'].min()} to {global_df['date'].max()}")

    if args.start_year:
        start_date = pd.Timestamp(f"{args.start_year}-01-01")
        print(f"\nFiltering from {args.start_year} onwards...")
        global_df = global_df[global_df["date"] >= start_date].copy()
        kl_df = kl_df[kl_df["date"] >= start_date].copy()
        corr_df = corr_df[corr_df["date"] >= start_date].copy()
        print(f"After filtering: {len(global_df)} snapshots")

    print("\nGenerating plots...")

    print("\n1. KL divergence evolution with events...")
    plot_kl_evolution_with_events(kl_df, args.output_dir)

    print("\n2. Global properties with events...")
    plot_global_properties_with_events(global_df, args.output_dir)

    print("\n3. Zoomed plots for each event...")
    date_min, date_max = kl_df["date"].min(), kl_df["date"].max()
    for event in CRYPTO_EVENTS:
        if date_min <= event.date <= date_max:
            print(f"  Processing: {event.name}")
            plot_event_zoom(
                kl_df,
                global_df,
                event,
                args.output_dir,
                args.weeks_around_event,
                args.weeks_around_event,
            )
        else:
            print(f"  Skipping: {event.name} (outside data range)")

    print("\n4. Events impact summary...")
    plot_events_impact_summary(kl_df, args.output_dir)

    print("\n5. KL divergence correlations...")
    plot_correlations(corr_df, args.output_dir)

    print("\n6. Creating events reference...")
    create_events_reference(args.output_dir)

    print(f"\nAll plots saved to: {args.output_dir}")


if __name__ == "__main__":
    main()
