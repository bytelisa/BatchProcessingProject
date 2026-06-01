#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


QUERIES = ["Q1", "Q2", "Q3"]

QUERY_LABELS = {
    "Q1": "Query 1",
    "Q2": "Query 2",
    "Q3": "Query 3",
}

PROCESSING_PHASE_MAP = {
    ("Q1", "df"): "computation_s",
    ("Q1", "rdd"): "computation_s",

    ("Q2", "df"): "all_airlines_computation_s",
    ("Q2", "rdd"): "computation_s",

    ("Q3", "df"): "computation_percentiles_s",
    ("Q3", "rdd"): "computation_percentiles_s",
}


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize basic column types expected from benchmark_scaling_summary.csv.
    """
    required_columns = {
        "worker_count",
        "query",
        "impl",
        "phase",
        "mean_s",
    }

    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in benchmark CSV: {sorted(missing)}")

    df = df.copy()

    df["worker_count"] = pd.to_numeric(df["worker_count"], errors="coerce").astype("Int64")
    df["mean_s"] = pd.to_numeric(df["mean_s"], errors="coerce")

    if "std_s" in df.columns:
        df["std_s"] = pd.to_numeric(df["std_s"], errors="coerce")

    df["query"] = df["query"].astype(str).str.upper()
    df["impl"] = df["impl"].astype(str).str.lower()
    df["phase"] = df["phase"].astype(str)

    df = df.dropna(subset=["worker_count", "mean_s"])

    return df


def extract_e2e_data(df: pd.DataFrame, impl: str) -> pd.DataFrame:
    """
    Extract end-to-end rows for a given implementation: df or rdd.
    """
    result = df[
        (df["impl"] == impl)
        & (df["phase"] == "end_to_end_s")
        & (df["query"].isin(QUERIES))
    ].copy()

    return result


def extract_processing_data(df: pd.DataFrame, impl: str) -> pd.DataFrame:
    """
    Extract normalized processing rows for a given implementation.

    Since the internal phase name differs across queries/implementations,
    this function maps each query to its correct source phase.
    """
    pieces = []

    for query in QUERIES:
        source_phase = PROCESSING_PHASE_MAP.get((query, impl))

        if source_phase is None:
            print(f"[WARN] No processing phase mapping for query={query}, impl={impl}")
            continue

        subset = df[
            (df["query"] == query)
            & (df["impl"] == impl)
            & (df["phase"] == source_phase)
        ].copy()

        if subset.empty:
            print(
                f"[WARN] Missing processing rows for "
                f"query={query}, impl={impl}, phase={source_phase}"
            )
            continue

        subset["normalized_phase"] = "processing_s"
        subset["source_phase"] = source_phase
        pieces.append(subset)

    if not pieces:
        return pd.DataFrame(columns=df.columns)

    return pd.concat(pieces, ignore_index=True)


def plot_line_chart(
    data: pd.DataFrame,
    title: str,
    subtitle: str,
    output_path: Path,
    y_label: str = "Execution Time (sec)",
) -> None:
    """
    Plot one line chart with one line per query.
    Style is intentionally simple and similar to the provided reference image.
    """
    fig, ax = plt.subplots(figsize=(9, 5.5))

    for query in QUERIES:
        qdf = data[data["query"] == query].sort_values("worker_count")

        if qdf.empty:
            print(f"[WARN] No data for {query} in plot {output_path.name}")
            continue

        ax.plot(
            qdf["worker_count"].astype(int),
            qdf["mean_s"],
            marker="o",
            linewidth=1.8,
            label=QUERY_LABELS.get(query, query),
        )

    ax.set_title(f"{title}\n{subtitle}", fontsize=13)
    ax.set_xlabel("Number of Workers")
    ax.set_ylabel(y_label)

    ax.grid(True, which="major", linestyle="-", linewidth=0.8, alpha=0.7)
    ax.legend(title="Query Name", loc="best")

    workers = sorted(data["worker_count"].dropna().astype(int).unique())
    if workers:
        ax.set_xticks(workers)

    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300)
    plt.close(fig)

    print(f"[OK] Saved {output_path}")


def build_all_plots(input_csv: Path, output_dir: Path) -> None:
    raw_df = pd.read_csv(input_csv)
    df = normalize_dataframe(raw_df)

    plots = [
        {
            "data": extract_e2e_data(df, "df"),
            "title": "End-to-End Time vs Workers",
            "subtitle": "Implementation: DataFrame",
            "filename": "benchmark_e2e_time_df.png",
        },
        {
            "data": extract_processing_data(df, "df"),
            "title": "Processing Time vs Workers",
            "subtitle": "Implementation: DataFrame",
            "filename": "benchmark_processing_time_df.png",
        },
        {
            "data": extract_e2e_data(df, "rdd"),
            "title": "End-to-End Time vs Workers",
            "subtitle": "Implementation: RDD",
            "filename": "benchmark_e2e_time_rdd.png",
        },
        {
            "data": extract_processing_data(df, "rdd"),
            "title": "Processing Time vs Workers",
            "subtitle": "Implementation: RDD",
            "filename": "benchmark_processing_time_rdd.png",
        },
    ]

    for plot in plots:
        if plot["data"].empty:
            print(f"[WARN] Skipping {plot['filename']}: no data available")
            continue

        plot_line_chart(
            data=plot["data"],
            title=plot["title"],
            subtitle=plot["subtitle"],
            output_path=output_dir / plot["filename"],
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate benchmark scaling line charts from benchmark_scaling_summary.csv"
    )

    parser.add_argument(
        "--input",
        default="output/benchmarks/benchmark_scaling_summary.csv",
        help="Path to benchmark_scaling_summary.csv",
    )

    parser.add_argument(
        "--output-dir",
        default="plots/scaling_plots",
        help="Directory where PNG charts will be saved",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_csv = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    build_all_plots(input_csv=input_csv, output_dir=output_dir)


if __name__ == "__main__":
    main()