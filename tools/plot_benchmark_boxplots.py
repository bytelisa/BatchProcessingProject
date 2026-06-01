"""
plot_benchmark_boxplots.py

Generate publication-quality matplotlib box plots from benchmark raw
end-to-end iteration data.

Input:
  output/benchmarks/benchmark_raw_iterations_summary.csv

Output:
  plots/boxplots/boxplot_q1_end_to_end.png
  plots/boxplots/boxplot_q1_end_to_end.pdf
  plots/boxplots/boxplot_q2_end_to_end.png
  plots/boxplots/boxplot_q2_end_to_end.pdf
  plots/boxplots/boxplot_q3_end_to_end.png
  plots/boxplots/boxplot_q3_end_to_end.pdf
"""

import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = PROJECT_ROOT / "output" / "benchmarks" / "benchmark_raw_iterations_summary.csv"
OUTPUT_DIR = PROJECT_ROOT / "plots" / "boxplots"

QUERIES = ["Q1", "Q2", "Q3"]
IMPLEMENTATIONS = ["df", "rdd"]

QUERY_TITLES = {
    "Q1": "Q1 - End-to-end execution time distribution",
    "Q2": "Q2 - End-to-end execution time distribution",
    "Q3": "Q3 - End-to-end execution time distribution",
}

OUTPUT_BASENAMES = {
    "Q1": "boxplot_q1_end_to_end",
    "Q2": "boxplot_q2_end_to_end",
    "Q3": "boxplot_q3_end_to_end",
}


def read_raw_iterations(path):
    if not path.exists():
        raise FileNotFoundError("Missing benchmark raw iterations CSV: " + str(path))

    df = pd.read_csv(path)

    required_columns = {
        "worker_count",
        "query",
        "impl",
        "phase",
        "time_s",
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(
            "Missing required columns in "
            + str(path)
            + ": "
            + str(sorted(missing_columns))
        )

    df = df.copy()
    df["query"] = df["query"].astype(str).str.strip().str.upper()
    df["impl"] = df["impl"].astype(str).str.strip().str.lower()
    df["phase"] = df["phase"].astype(str).str.strip()
    df["worker_count"] = pd.to_numeric(df["worker_count"], errors="coerce")
    df["time_s"] = pd.to_numeric(df["time_s"], errors="coerce")

    df = df.dropna(subset=["worker_count", "time_s"])
    df["worker_count"] = df["worker_count"].astype(int)

    return df[df["phase"] == "end_to_end_s"]


def build_worker_impl_data(query_df):
    worker_counts = sorted(query_df["worker_count"].unique().tolist())
    data_by_impl = {
        "df": [],
        "rdd": [],
    }

    for worker_count in worker_counts:
        for impl in IMPLEMENTATIONS:
            group_df = query_df[
                (query_df["worker_count"] == worker_count)
                & (query_df["impl"] == impl)
            ]

            data_by_impl[impl].append(group_df["time_s"].dropna().tolist())

    return worker_counts, data_by_impl


def warn_small_groups(query, worker_counts, data_by_impl):
    for worker_count, df_values, rdd_values in zip(
        worker_counts,
        data_by_impl["df"],
        data_by_impl["rdd"],
    ):
        for impl, values in [("df", df_values), ("rdd", rdd_values)]:
            if values and len(values) < 5:
                print(
                    "[WARN] "
                    + query
                    + " group "
                    + str(worker_count)
                    + "W "
                    + impl
                    + " has fewer than 5 observations (n="
                    + str(len(values))
                    + "); box plot may be weak."
                )


def draw_impl_boxplot(ax, data, positions, color, label):
    if not data:
        return

    boxplot = ax.boxplot(
        data,
        positions=positions,
        widths=0.30,
        patch_artist=True,
        showfliers=True,
        medianprops={"color": "#d62728", "linewidth": 1.8},
        boxprops={"linewidth": 1.2},
        whiskerprops={"linewidth": 1.1},
        capprops={"linewidth": 1.1},
        flierprops={
            "marker": "o",
            "markersize": 4,
            "markerfacecolor": "white",
            "markeredgecolor": "#555555",
            "alpha": 0.8,
        },
    )

    for box in boxplot["boxes"]:
        box.set_facecolor(color)
        box.set_alpha(0.75)
        box.set_edgecolor("#333333")

    for median in boxplot["medians"]:
        median.set_label(label)


def plot_query(query, query_df):
    if query_df.empty:
        print("[WARN] No end_to_end_s data for " + query + "; skipping.")
        return []

    worker_counts, data_by_impl = build_worker_impl_data(query_df)
    if not worker_counts:
        print("[WARN] No plottable groups for " + query + "; skipping.")
        return []

    warn_small_groups(query, worker_counts, data_by_impl)

    fig_width = max(7.5, len(worker_counts) * 1.6)
    fig, ax = plt.subplots(figsize=(fig_width, 5.2))

    worker_positions = list(range(1, len(worker_counts) + 1))
    offset = 0.18

    df_data = []
    df_positions = []
    rdd_data = []
    rdd_positions = []

    for position, df_values, rdd_values in zip(
        worker_positions,
        data_by_impl["df"],
        data_by_impl["rdd"],
    ):
        if df_values:
            df_data.append(df_values)
            df_positions.append(position - offset)

        if rdd_values:
            rdd_data.append(rdd_values)
            rdd_positions.append(position + offset)

    draw_impl_boxplot(
        ax=ax,
        data=df_data,
        positions=df_positions,
        color="#2ca02c",
        label="DataFrame",
    )
    draw_impl_boxplot(
        ax=ax,
        data=rdd_data,
        positions=rdd_positions,
        color="#1f77b4",
        label="RDD",
    )

    ax.set_title(QUERY_TITLES[query], fontsize=14, pad=12)
    ax.set_ylabel("Execution time (s)", fontsize=11)
    ax.set_xlabel("")
    ax.set_xticks(worker_positions)
    ax.set_xticklabels([str(worker_count) + "W" for worker_count in worker_counts])
    ax.grid(axis="y", color="#d9d9d9", linestyle="-", linewidth=0.8, alpha=0.8)
    ax.set_axisbelow(True)
    ax.legend(
        handles=[
            Patch(facecolor="#2ca02c", edgecolor="#333333", alpha=0.75, label="DataFrame"),
            Patch(facecolor="#1f77b4", edgecolor="#333333", alpha=0.75, label="RDD"),
        ],
        loc="best",
        frameon=True,
    )

    fig.tight_layout()

    basename = OUTPUT_BASENAMES[query]
    png_path = OUTPUT_DIR / (basename + ".png")
    pdf_path = OUTPUT_DIR / (basename + ".pdf")

    fig.savefig(png_path, dpi=300)
    fig.savefig(pdf_path)
    plt.close(fig)

    return [png_path, pdf_path]


def main():
    print("=" * 72)
    print("Benchmark end-to-end box plot generation")
    print("=" * 72)
    print("[INFO] Project root: " + str(PROJECT_ROOT))
    print("[INFO] Input CSV:  " + str(INPUT_CSV))
    print("[INFO] Output dir: " + str(OUTPUT_DIR))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = read_raw_iterations(INPUT_CSV)

    generated_files = []

    for query in QUERIES:
        query_df = df[df["query"] == query]
        generated_files.extend(plot_query(query, query_df))

    print("\nGenerated files:")
    if generated_files:
        for path in generated_files:
            print("  - " + os.fspath(path))
    else:
        print("  No files generated.")


if __name__ == "__main__":
    main()
