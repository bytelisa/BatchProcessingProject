"""
export_benchmark_to_redis.py

Exports Spark benchmark results to Redis using Grafana-friendly hashes.

Each Grafana panel reads one Redis Hash with HGETALL. Every hash value is a
JSON row, so Grafana can use the "Extract fields" transformation on values.

Input:
- output/benchmarks/benchmark_scaling_summary.csv
- output/benchmarks/benchmark_raw_iterations_summary.csv

Output Redis keys:
- bench:grafana:line:e2e:<query>
- bench:grafana:line:processing:<query>
- bench:grafana:table:aggregate
- bench:grafana:box:e2e:<query>
- bench:grafana:boxstats:e2e:<query>
- bench:grafana:meta
"""

import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import redis


# ---------------------------------------------------------------------------
# 1. Config
# ---------------------------------------------------------------------------

BENCHMARK_DIR = os.getenv("BENCHMARK_DIR", "output/benchmarks")

BENCHMARK_AGGREGATE_CSV = os.getenv(
    "BENCHMARK_AGGREGATE_CSV",
    os.path.join(BENCHMARK_DIR, "benchmark_scaling_summary.csv"),
)

BENCHMARK_RAW_CSV = os.getenv(
    "BENCHMARK_RAW_CSV",
    os.path.join(BENCHMARK_DIR, "benchmark_raw_iterations_summary.csv"),
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

QUERIES = ["Q1", "Q2", "Q3"]
IMPLEMENTATIONS = ["df", "rdd"]

LINE_EXPORTS = {
    "e2e": {
        "display_phase": "end_to_end_s",
        "source_phase_by_query": {
            "Q1": "end_to_end_s",
            "Q2": "end_to_end_s",
            "Q3": "end_to_end_s",
        },
    },
    "processing": {
        "display_phase": "processing_s",
        "source_phase_by_query": {
            "Q1": "computation_s",
            "Q2": "computation_s",
            "Q3": "computation_percentiles_s",
        },
    },
}


# ---------------------------------------------------------------------------
# 2. Redis helpers
# ---------------------------------------------------------------------------

def connect_redis():
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )

    try:
        r.ping()
    except redis.RedisError as exc:
        print("[ERROR] Unable to connect to Redis")
        print(exc)
        sys.exit(1)

    print("[INFO] Connected to Redis: " + REDIS_HOST + ":" + str(REDIS_PORT))
    return r


def delete_old_benchmark_keys(r):
    deleted = 0

    for key in r.scan_iter("bench:grafana:*"):
        r.delete(key)
        deleted += 1

    print("[INFO] Deleted bench:grafana:* keys: " + str(deleted))


def hset_json(r, key, field, value):
    payload = json.dumps(value, ensure_ascii=False)
    r.hset(key, field, payload)


# ---------------------------------------------------------------------------
# 3. CSV helpers
# ---------------------------------------------------------------------------

def read_csv_rows(path):
    if not os.path.exists(path):
        raise FileNotFoundError("Missing benchmark CSV: " + path)

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        raise ValueError("Empty benchmark CSV: " + path)

    return rows


def clean(value):
    if value is None:
        return ""
    return str(value).strip()


def to_int(value):
    value = clean(value)
    if value == "":
        return 0
    return int(float(value))


def to_float_or_none(value):
    value = clean(value)
    if value == "":
        return None
    return float(value)


# ---------------------------------------------------------------------------
# 4. Normalizzazione righe aggregate
# ---------------------------------------------------------------------------

def infer_algorithm(query, impl):
    query = clean(query).upper()
    impl = clean(impl).lower()

    if impl == "df":
        return "Spark DataFrame"

    if query == "Q3" and impl == "rdd":
        return "RDD + t-digest"

    if impl == "rdd":
        return "Spark RDD"

    return ""


def infer_note(row):
    phase = clean(row.get("phase"))

    if phase == "end_to_end_s":
        return "includes output"

    if phase == "computation_percentiles_s":
        return "Q3 percentile computation"

    if phase == "computation_s":
        return "main computation phase"

    return ""


def normalize_aggregate_rows(rows):
    normalized = []

    for row in rows:
        query = clean(row.get("query")).upper()
        impl = clean(row.get("impl")).lower()

        item = {
            "worker_count": to_int(row.get("worker_count")),
            "query": query,
            "impl": impl,
            "phase": clean(row.get("phase")),
            "n": to_int(row.get("n")),
            "mean_s": to_float_or_none(row.get("mean_s")),
            "std_s": to_float_or_none(row.get("std_s")),
            "median_s": to_float_or_none(row.get("median_s")),
            "min_s": to_float_or_none(row.get("min_s")),
            "max_s": to_float_or_none(row.get("max_s")),
            "algorithm": infer_algorithm(query, impl),
            "note": infer_note(row),
        }

        normalized.append(item)

    return normalized


# ---------------------------------------------------------------------------
# 5. Normalizzazione righe raw
# ---------------------------------------------------------------------------

def normalize_raw_rows(rows):
    normalized = []

    for row in rows:
        iteration = (
            row.get("iteration")
            or row.get("iteration_idx")
            or row.get("run")
        )

        item = {
            "worker_count": to_int(row.get("worker_count")),
            "query": clean(row.get("query")).upper(),
            "impl": clean(row.get("impl")).lower(),
            "phase": clean(row.get("phase")),
            "iteration": to_int(iteration),
            "time_s": to_float_or_none(row.get("time_s")),
        }

        normalized.append(item)

    return normalized


# ---------------------------------------------------------------------------
# 6. Builder line chart
# ---------------------------------------------------------------------------

def build_wide_line_rows(aggregate_rows, query, source_phase, display_phase):
    by_worker = {}

    for row in aggregate_rows:
        if row["query"] != query:
            continue

        if row["phase"] != source_phase:
            continue

        impl = row["impl"]
        if impl not in IMPLEMENTATIONS:
            continue

        worker = row["worker_count"]

        if worker not in by_worker:
            by_worker[worker] = {
                "worker_count": worker,
                "df": None,
                "rdd": None,
                "display_phase": display_phase,
                "source_phase_df": source_phase,
                "source_phase_rdd": source_phase,
            }

        by_worker[worker][impl] = row["mean_s"]

    result = [by_worker[w] for w in sorted(by_worker)]

    for row in result:
        if row["df"] is None or row["rdd"] is None:
            print(
                "[WARN] Incomplete line row: "
                + query + " " + display_phase + " "
                + json.dumps(row, ensure_ascii=False)
            )

    return result


# ---------------------------------------------------------------------------
# 7. Builder tabella aggregate
# ---------------------------------------------------------------------------

def build_aggregate_table_rows(aggregate_rows):
    return list(aggregate_rows)


# ---------------------------------------------------------------------------
# 8. Builder box plot
# ---------------------------------------------------------------------------

def build_box_rows(raw_rows, query):
    result = []
    impl_order = {"df": 0, "rdd": 1}

    for row in raw_rows:
        if row["query"] != query:
            continue

        if row["phase"] != "end_to_end_s":
            continue

        impl = row["impl"]
        worker = row["worker_count"]

        item = {
            "query": query,
            "worker_count": worker,
            "impl": impl,
            "iteration": row["iteration"],
            "time_s": row["time_s"],
            "group": str(worker) + "W " + impl,
            "group_order": worker * 10 + impl_order.get(impl, 9),
        }

        result.append(item)

    result.sort(key=lambda r: (r["group_order"], r["iteration"]))
    return result


def percentile(values, q):
    if not values:
        return None

    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower

    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def build_boxstats_rows(raw_rows, query):
    grouped = {}
    impl_order = {"df": 0, "rdd": 1}
    base_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)

    for row in raw_rows:
        if row["query"] != query:
            continue

        if row["phase"] != "end_to_end_s":
            continue

        if row["time_s"] is None:
            continue

        key = (row["worker_count"], row["impl"])
        if key not in grouped:
            grouped[key] = []

        grouped[key].append(row["time_s"])

    result = []

    for (worker, impl), values in grouped.items():
        values_sorted = sorted(values)
        group_order = worker * 10 + impl_order.get(impl, 9)
        fake_time = base_time + timedelta(minutes=group_order)

        result.append({
            "time": fake_time.isoformat().replace("+00:00", "Z"),
            "query": query,
            "worker_count": worker,
            "impl": impl,
            "group": str(worker) + "W " + impl,
            "group_order": group_order,
            "n": len(values_sorted),
            "min_s": values_sorted[0],
            "q1_s": percentile(values_sorted, 0.25),
            "median_s": percentile(values_sorted, 0.50),
            "q3_s": percentile(values_sorted, 0.75),
            "max_s": values_sorted[-1],
        })

    result.sort(key=lambda r: r["group_order"])
    return result


# ---------------------------------------------------------------------------
# 9. Exporter Redis
# ---------------------------------------------------------------------------

def export_line_charts(r, aggregate_rows):
    for export_name, config in LINE_EXPORTS.items():
        display_phase = config["display_phase"]

        for query in QUERIES:
            source_phase = config["source_phase_by_query"][query]
            key = "bench:grafana:line:" + export_name + ":" + query

            rows = build_wide_line_rows(
                aggregate_rows=aggregate_rows,
                query=query,
                source_phase=source_phase,
                display_phase=display_phase,
            )

            for row in rows:
                hset_json(r, key, str(row["worker_count"]), row)

            print("[INFO] Exported " + str(len(rows)) + " rows to " + key)


def export_aggregate_table(r, aggregate_rows):
    key = "bench:grafana:table:aggregate"
    rows = build_aggregate_table_rows(aggregate_rows)

    for row in rows:
        field = (
            row["query"] + ":"
            + row["impl"] + ":"
            + str(row["worker_count"]) + ":"
            + row["phase"]
        )
        hset_json(r, key, field, row)

    print("[INFO] Exported " + str(len(rows)) + " rows to " + key)


def export_box_plots(r, raw_rows):
    for query in QUERIES:
        key = "bench:grafana:box:e2e:" + query
        rows = build_box_rows(raw_rows, query)

        for index, row in enumerate(rows):
            field = (
                str(row["group_order"]) + ":"
                + str(row["iteration"]) + ":"
                + str(index)
            )
            hset_json(r, key, field, row)

        print("[INFO] Exported " + str(len(rows)) + " rows to " + key)


def export_boxstats(r, raw_rows):
    for query in QUERIES:
        key = "bench:grafana:boxstats:e2e:" + query
        rows = build_boxstats_rows(raw_rows, query)

        for row in rows:
            field = str(row["group_order"]) + ":" + row["impl"]
            hset_json(r, key, field, row)

        print("[INFO] Exported " + str(len(rows)) + " boxstats rows to " + key)


def export_meta(r):
    key = "bench:grafana:meta"
    meta = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "aggregate_csv": BENCHMARK_AGGREGATE_CSV,
        "raw_csv": BENCHMARK_RAW_CSV,
        "line_exports": LINE_EXPORTS,
        "redis_model": "Redis Hash with JSON row values",
        "note": (
            "Processing phase is mapped to computation_s for Q1/Q2 "
            "and computation_percentiles_s for Q3. Boxstats are computed "
            "from raw end_to_end_s iteration rows."
        ),
    }

    hset_json(r, key, "metadata", meta)
    print("[INFO] Exported metadata to " + key)


# ---------------------------------------------------------------------------
# 10. Validazione/logging
# ---------------------------------------------------------------------------

def validate_rows(aggregate_rows, raw_rows):
    aggregate_count = len(aggregate_rows)
    raw_count = len(raw_rows)

    print("[INFO] Aggregate rows: " + str(aggregate_count))
    print("[INFO] Raw rows:       " + str(raw_count))

    for query in QUERIES:
        for export_name, config in LINE_EXPORTS.items():
            source_phase = config["source_phase_by_query"][query]
            rows = build_wide_line_rows(
                aggregate_rows=aggregate_rows,
                query=query,
                source_phase=source_phase,
                display_phase=config["display_phase"],
            )
            if not rows:
                print(
                    "[WARN] No line data for "
                    + export_name + " " + query
                    + " source_phase=" + source_phase
                )

        box_rows = build_box_rows(raw_rows, query)
        if not box_rows:
            print("[WARN] No e2e box plot data for " + query)

        boxstats_rows = build_boxstats_rows(raw_rows, query)
        if not boxstats_rows:
            print("[WARN] No e2e boxstats data for " + query)


# ---------------------------------------------------------------------------
# 11. main()
# ---------------------------------------------------------------------------

def main():
    print("=" * 72)
    print("SABD Project 1 - Export benchmark to Redis for Grafana")
    print("=" * 72)
    print("[INFO] Aggregate CSV: " + BENCHMARK_AGGREGATE_CSV)
    print("[INFO] Raw CSV:       " + BENCHMARK_RAW_CSV)
    print("[INFO] Redis:         " + REDIS_HOST + ":" + str(REDIS_PORT))

    r = connect_redis()
    delete_old_benchmark_keys(r)

    aggregate_csv_rows = read_csv_rows(BENCHMARK_AGGREGATE_CSV)
    raw_csv_rows = read_csv_rows(BENCHMARK_RAW_CSV)

    aggregate_rows = normalize_aggregate_rows(aggregate_csv_rows)
    raw_rows = normalize_raw_rows(raw_csv_rows)

    validate_rows(aggregate_rows, raw_rows)

    export_line_charts(r, aggregate_rows)
    export_aggregate_table(r, aggregate_rows)
    export_box_plots(r, raw_rows)
    export_boxstats(r, raw_rows)
    export_meta(r)

    print("=" * 72)
    print("Benchmark export completed successfully.")
    print("=" * 72)


if __name__ == "__main__":
    main()
