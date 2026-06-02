"""
plot_benchmark_boxplots.py

Generate a 2x3 grid of end-to-end benchmark box plots from raw iteration data.

Input:
  output/benchmarks/benchmark_raw_iterations_summary.csv

Output:
  plots/benchmark_plots/boxplot_end_to_end_distribution_grid.png
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = PROJECT_ROOT / "output" / "benchmarks" / "benchmark_raw_iterations_summary.csv"
OUTPUT_DIR = PROJECT_ROOT / "plots" / "benchmark_plots"
OUTPUT_PATH = OUTPUT_DIR / "boxplot_end_to_end_distribution_grid.png"
DISPLAY_OUTPUT_PATH = "output/benchmark_plots/boxplot_end_to_end_distribution_grid.png"

QUERIES = ["Q1", "Q2", "Q3"]
IMPLEMENTATIONS = ["df", "rdd"]

IMPL_LABELS = {
    "df": "DataFrame",
    "rdd": "RDD",
}

IMPL_COLORS = {
    "df": "#2ca02c",
    "rdd": "#1f77b4",
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

    return df


def filter_end_to_end_rows(raw_df):
    return raw_df[
        (raw_df["phase"] == "end_to_end_s")
        & (raw_df["query"].isin(QUERIES))
        & (raw_df["impl"].isin(IMPLEMENTATIONS))
    ].copy()


def get_worker_box_data(filtered_df, query, impl, workers):
    data = []
    positions = []

    for position, worker_count in enumerate(workers, start=1):
        group_df = filtered_df[
            (filtered_df["query"] == query)
            & (filtered_df["impl"] == impl)
            & (filtered_df["worker_count"] == worker_count)
        ]
        values = group_df["time_s"].dropna().tolist()

        if values:
            data.append(values)
            positions.append(position)

            if len(values) < 5:
                print(
                    "[WARN] "
                    + query
                    + " "
                    + impl
                    + " "
                    + str(worker_count)
                    + "W has fewer than 5 observations (n="
                    + str(len(values))
                    + "); box plot may be weak."
                )

    return data, positions


def draw_boxplot(ax, data, positions, color):
    boxplot = ax.boxplot(
        data,
        positions=positions,
        widths=0.45,
        patch_artist=True,
        showfliers=False,
        medianprops={"color": "#d62728", "linewidth": 1.6},
        boxprops={"linewidth": 1.1},
        whiskerprops={"linewidth": 1.0},
        capprops={"linewidth": 1.0},
        flierprops={
            "marker": "o",
            "markersize": 3.5,
            "markerfacecolor": "white",
            "markeredgecolor": "#555555",
            "alpha": 0.8,
        },
    )

    for box in boxplot["boxes"]:
        box.set_facecolor(color)
        box.set_alpha(0.75)
        box.set_edgecolor("#333333")


def setup_subplot(ax, query, impl, workers):
    worker_positions = list(range(1, len(workers) + 1))

    ax.set_title(query + " - " + IMPL_LABELS[impl], fontsize=12, pad=8)
    ax.set_ylabel("Execution time (s)", fontsize=10)
    ax.set_xlabel("Number of workers", fontsize=10)
    ax.set_xticks(worker_positions)
    ax.set_xticklabels([str(worker_count) + "W" for worker_count in workers])
    ax.grid(axis="y", color="#d9d9d9", linestyle="-", linewidth=0.8, alpha=0.85)
    ax.set_axisbelow(True)


def plot_end_to_end_boxplot_grid(raw_df, output_path):
    filtered_df = filter_end_to_end_rows(raw_df)

    if filtered_df.empty:
        print("[WARN] No end_to_end_s benchmark data available; skipping plot.")
        return False

    workers = sorted(filtered_df["worker_count"].unique().tolist())
    if not workers:
        print("[WARN] No worker_count values available; skipping plot.")
        return False

    fig, axes = plt.subplots(nrows=2, ncols=3, figsize=(14.5, 8.0), sharey=False)
    fig.suptitle("End-to-end execution time distribution", fontsize=16, y=0.98)

    for row_index, impl in enumerate(IMPLEMENTATIONS):
        for col_index, query in enumerate(QUERIES):
            ax = axes[row_index, col_index]
            setup_subplot(ax, query, impl, workers)

            data, positions = get_worker_box_data(
                filtered_df=filtered_df,
                query=query,
                impl=impl,
                workers=workers,
            )

            if not data:
                print("[WARN] No data for " + query + " " + impl + "; subplot left empty.")
                continue

            draw_boxplot(
                ax=ax,
                data=data,
                positions=positions,
                color=IMPL_COLORS[impl],
            )

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(output_path, dpi=300)
    plt.close(fig)
    return True


def main():
    print("=" * 72)
    print("Benchmark end-to-end box plot grid generation")
    print("=" * 72)
    print("[INFO] Project root: " + str(PROJECT_ROOT))
    print("[INFO] Input CSV:  " + str(INPUT_CSV))
    print("[INFO] Output PNG: " + str(OUTPUT_PATH))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw_df = read_raw_iterations(INPUT_CSV)
    generated = plot_end_to_end_boxplot_grid(raw_df, OUTPUT_PATH)

    if generated:
        print("[OK] Saved " + str(DISPLAY_OUTPUT_PATH))


if __name__ == "__main__":
    main()
