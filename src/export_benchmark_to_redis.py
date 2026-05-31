"""
export_benchmark_to_redis.py
────────────────────────────
Esporta i benchmark Spark su Redis Stack in formato RedisJSON,
ottimizzato per Grafana.

Input:
- output/benchmarks/benchmark_scaling_summary.csv
- output/benchmarks/benchmark_raw_iterations_summary.csv

Output RedisJSON:
- bench:grafana:line:<query>:<phase>
- bench:grafana:table:aggregate
- bench:grafana:box:<query>:<phase>
"""

import csv
import json
import os
import sys

import redis


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

BENCHMARK_DIR = os.getenv("BENCHMARK_DIR", "output/benchmarks")

AGGREGATE_CSV_PATH = os.getenv(
    "BENCHMARK_AGGREGATE_CSV",
    os.path.join(BENCHMARK_DIR, "benchmark_scaling_summary.csv"),
)

RAW_CSV_PATH = os.getenv(
    "BENCHMARK_RAW_CSV",
    os.path.join(BENCHMARK_DIR, "benchmark_raw_iterations_summary.csv"),
)


LINE_PHASES = {
    "Q1": ["end_to_end_s", "computation_s"],
    "Q2": ["end_to_end_s", "computation_s"],
    "Q3": ["end_to_end_s", "computation_percentiles_s"],
}

BOX_PHASES = {
    "Q1": ["end_to_end_s", "computation_s"],
    "Q2": ["end_to_end_s", "computation_s"],
    "Q3": ["end_to_end_s", "computation_percentiles_s"],
}


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
        print("[ERROR] Impossibile connettersi a Redis")
        print(exc)
        sys.exit(1)

    print("[INFO] Connesso a Redis: " + REDIS_HOST + ":" + str(REDIS_PORT))
    return r


def read_csv(path):
    if not os.path.exists(path):
        print("[ERROR] File non trovato: " + path)
        sys.exit(1)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("[ERROR] CSV vuoto: " + path)
        sys.exit(1)

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


def to_float(value):
    value = clean(value)
    if value == "":
        return 0.0
    return float(value)


def delete_old_benchmark_keys(r):
    keys = list(r.scan_iter("bench:grafana:*"))
    if keys:
        r.delete(*keys)
    print("[INFO] Eliminate chiavi bench:grafana:* " + str(len(keys)))


def json_set(r, key, value):
    payload = json.dumps(value, ensure_ascii=False)
    r.execute_command("JSON.SET", key, "$", payload)


def normalize_aggregate_rows(rows):
    normalized = []

    for row in rows:
        item = {
            "worker_count": to_int(row.get("worker_count")),
            "query": clean(row.get("query")),
            "impl": clean(row.get("impl")).lower(),
            "phase": clean(row.get("phase")),
            "n": to_int(row.get("n")),
            "mean_s": to_float(row.get("mean_s")),
            "std_s": to_float(row.get("std_s")),
            "median_s": to_float(row.get("median_s")),
            "min_s": to_float(row.get("min_s")),
            "max_s": to_float(row.get("max_s")),
        }

        if "algorithm" in row:
            item["algorithm"] = clean(row.get("algorithm"))

        if "note" in row:
            item["note"] = clean(row.get("note"))

        if "total_iterations" in row:
            item["total_iterations"] = to_int(row.get("total_iterations"))

        if "warmup_iterations" in row:
            item["warmup_iterations"] = to_int(row.get("warmup_iterations"))

        if "valid_iterations" in row:
            item["valid_iterations"] = to_int(row.get("valid_iterations"))

        normalized.append(item)

    return normalized


def normalize_raw_rows(rows):
    normalized = []

    for row in rows:
        worker_count = to_int(row.get("worker_count"))
        query = clean(row.get("query"))
        impl = clean(row.get("impl")).lower()
        phase = clean(row.get("phase"))
        iteration = to_int(row.get("iteration"))
        time_s = to_float(row.get("time_s"))

        normalized.append({
            "worker_count": worker_count,
            "query": query,
            "impl": impl,
            "phase": phase,
            "iteration": iteration,
            "time_s": time_s,
            "group": str(worker_count) + "W " + impl,
        })

    return normalized


def export_aggregate_table(r, aggregate_rows):
    key = "bench:grafana:table:aggregate"
    json_set(r, key, aggregate_rows)

    print("[✓] Esportata tabella aggregata:")
    print("    " + key)
    print("    rows=" + str(len(aggregate_rows)))


def build_line_rows(aggregate_rows, query, phase):
    """
    Costruisce righe del tipo:
    [
      {"worker_count": 1, "df": 1.2, "rdd": 4.5},
      {"worker_count": 2, "df": 0.9, "rdd": 3.8}
    ]
    """
    by_worker = {}

    for row in aggregate_rows:
        if row["query"] != query:
            continue
        if row["phase"] != phase:
            continue

        worker = row["worker_count"]
        impl = row["impl"]

        if worker not in by_worker:
            by_worker[worker] = {"worker_count": worker}

        by_worker[worker][impl] = row["mean_s"]

    result = []

    for worker in sorted(by_worker.keys()):
        item = by_worker[worker]

        # Garantisce schema stabile anche se manca una implementazione
        if "df" not in item:
            item["df"] = None
        if "rdd" not in item:
            item["rdd"] = None

        result.append(item)

    return result


def export_line_charts(r, aggregate_rows):
    exported = 0

    for query, phases in LINE_PHASES.items():
        for phase in phases:
            rows = build_line_rows(aggregate_rows, query, phase)

            if not rows:
                print("[WARN] Nessun dato line chart per " + query + " " + phase)
                continue

            key = "bench:grafana:line:" + query + ":" + phase
            json_set(r, key, rows)

            print("[✓] Esportata line chart:")
            print("    " + key)
            print("    rows=" + str(len(rows)))

            exported += 1

    print("[INFO] Line chart JSON esportate: " + str(exported))


def build_speedup_rows(aggregate_rows, query, phase):
    """
    Costruisce:
    [
      {"worker_count": 1, "df": 1.0, "rdd": 1.0},
      {"worker_count": 2, "df": 1.4, "rdd": 0.8}
    ]
    """
    rows = build_line_rows(aggregate_rows, query, phase)

    if not rows:
        return []

    base = rows[0]
    base_df = base.get("df")
    base_rdd = base.get("rdd")

    result = []

    for row in rows:
        worker = row["worker_count"]

        df_value = row.get("df")
        rdd_value = row.get("rdd")

        item = {"worker_count": worker}

        if base_df and df_value:
            item["df"] = round(base_df / df_value, 4)
        else:
            item["df"] = None

        if base_rdd and rdd_value:
            item["rdd"] = round(base_rdd / rdd_value, 4)
        else:
            item["rdd"] = None

        result.append(item)

    return result


def export_speedup_charts(r, aggregate_rows):
    exported = 0

    for query, phases in LINE_PHASES.items():
        for phase in phases:
            rows = build_speedup_rows(aggregate_rows, query, phase)

            if not rows:
                continue

            key = "bench:grafana:speedup:" + query + ":" + phase
            json_set(r, key, rows)

            print("[✓] Esportata speedup chart:")
            print("    " + key)
            print("    rows=" + str(len(rows)))

            exported += 1

    print("[INFO] Speedup JSON esportate: " + str(exported))


def build_box_rows(raw_rows, query, phase):
    result = []

    for row in raw_rows:
        if row["query"] != query:
            continue
        if row["phase"] != phase:
            continue

        result.append({
            "group": row["group"],
            "worker_count": row["worker_count"],
            "impl": row["impl"],
            "iteration": row["iteration"],
            "time_s": row["time_s"],
        })

    result.sort(key=lambda x: (x["worker_count"], x["impl"], x["iteration"]))

    return result


def export_box_plots(r, raw_rows):
    exported = 0

    for query, phases in BOX_PHASES.items():
        for phase in phases:
            rows = build_box_rows(raw_rows, query, phase)

            if not rows:
                print("[WARN] Nessun dato box plot per " + query + " " + phase)
                continue

            key = "bench:grafana:box:" + query + ":" + phase
            json_set(r, key, rows)

            print("[✓] Esportato box plot:")
            print("    " + key)
            print("    rows=" + str(len(rows)))

            exported += 1

    print("[INFO] Box plot JSON esportati: " + str(exported))


def main():
    print("=" * 72)
    print("SABD Project 1 - Export benchmark to RedisJSON")
    print("=" * 72)
    print("[INFO] Aggregate CSV: " + AGGREGATE_CSV_PATH)
    print("[INFO] Raw CSV:       " + RAW_CSV_PATH)
    print("[INFO] Redis:         " + REDIS_HOST + ":" + str(REDIS_PORT))

    r = connect_redis()
    delete_old_benchmark_keys(r)

    aggregate_csv_rows = read_csv(AGGREGATE_CSV_PATH)
    raw_csv_rows = read_csv(RAW_CSV_PATH)

    aggregate_rows = normalize_aggregate_rows(aggregate_csv_rows)
    raw_rows = normalize_raw_rows(raw_csv_rows)

    export_aggregate_table(r, aggregate_rows)
    export_line_charts(r, aggregate_rows)
    export_speedup_charts(r, aggregate_rows)
    export_box_plots(r, raw_rows)

    print("\n" + "=" * 72)
    print("[✓] Export benchmark RedisJSON completato")
    print("=" * 72)


if __name__ == "__main__":
    main()