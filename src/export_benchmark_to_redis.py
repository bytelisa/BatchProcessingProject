"""
export_benchmark_to_redis.py
────────────────────────────
Esporta i risultati dei benchmark Spark su Redis Stack per Grafana.

Input attesi:
- output/benchmarks/benchmark_scaling_summary.csv
  contiene statistiche aggregate: mean, std, median, min, max

- output/benchmarks/benchmark_raw_iterations_summary.csv
  contiene tempi grezzi delle iterazioni valide:
  worker_count, query, impl, iteration, phase, time_s

Output Redis:
- bench:line:<query>:<impl>:<phase>:mean
- bench:line:<query>:<impl>:<phase>:std
- bench:line:<query>:<impl>:<phase>:median
- bench:line:<query>:<impl>:<phase>:min
- bench:line:<query>:<impl>:<phase>:max

- bench:table:<column>

- benchraw:<query>:<impl>:<phase>:<worker_count>
"""

import csv
import os
import sys

import redis


# ─────────────────────────────────────────────
# Config Redis
# ─────────────────────────────────────────────

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None


# ─────────────────────────────────────────────
# Input paths
# ─────────────────────────────────────────────

BENCHMARK_DIR = os.getenv("BENCHMARK_DIR", "output/benchmarks")

AGGREGATE_CSV_PATH = os.getenv(
    "BENCHMARK_AGGREGATE_CSV",
    os.path.join(BENCHMARK_DIR, "benchmark_scaling_summary.csv"),
)

RAW_CSV_PATH = os.getenv(
    "BENCHMARK_RAW_CSV",
    os.path.join(BENCHMARK_DIR, "benchmark_raw_iterations_summary.csv"),
)


# ─────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────

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
        print("        host:", REDIS_HOST)
        print("        port:", REDIS_PORT)
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


def delete_keys_by_prefix(r, prefix):
    keys = list(r.scan_iter(prefix + "*"))
    if keys:
        r.delete(*keys)
    return len(keys)


def clean_value(value):
    if value is None:
        return ""
    return str(value).strip()


def safe_float(value):
    value = clean_value(value)
    if value == "":
        return 0.0
    return float(value)


def safe_int(value):
    value = clean_value(value)
    if value == "":
        return 0
    return int(float(value))


def require_columns(rows, required, label):
    found = set(rows[0].keys())
    missing = set(required) - found

    if missing:
        print("[ERROR] Colonne mancanti in " + label + ": " + str(sorted(missing)))
        print("[INFO] Colonne trovate: " + str(sorted(found)))
        sys.exit(1)


# ─────────────────────────────────────────────
# Export aggregate
# ─────────────────────────────────────────────

AGG_REQUIRED_COLUMNS = [
    "worker_count",
    "query",
    "impl",
    "phase",
    "n",
    "mean_s",
    "std_s",
    "median_s",
    "min_s",
    "max_s",
]

AGG_OPTIONAL_COLUMNS = [
    "algorithm",
    "note",
    "total_iterations",
    "warmup_iterations",
    "valid_iterations",
]

AGG_NUMERIC_STATS = [
    "mean_s",
    "std_s",
    "median_s",
    "min_s",
    "max_s",
]


def export_aggregate_rows(r, rows):
    print("\n" + "=" * 72)
    print("EXPORT BENCHMARK AGGREGATE")
    print("=" * 72)

    require_columns(rows, AGG_REQUIRED_COLUMNS, AGGREGATE_CSV_PATH)

    pipe = r.pipeline(transaction=False)
    exported = 0

    table_columns = AGG_REQUIRED_COLUMNS + [
        col for col in AGG_OPTIONAL_COLUMNS if col in rows[0]
    ]

    for idx, row in enumerate(rows, start=1):
        row_id = str(idx)

        worker_count = str(safe_int(row["worker_count"]))
        query = clean_value(row["query"])
        impl = clean_value(row["impl"])
        phase = clean_value(row["phase"])

        # ─────────────────────────────────────
        # Table: column-oriented hashes
        # ─────────────────────────────────────
        for col in table_columns:
            pipe.hset(
                "bench:table:" + col,
                row_id,
                clean_value(row.get(col, "")),
            )
            exported += 1

        # ─────────────────────────────────────
        # Row detail: useful for debug/manual inspection
        # ─────────────────────────────────────
        row_key = "bench:row:" + row_id
        pipe.hset(
            row_key,
            mapping={
                col: clean_value(row.get(col, ""))
                for col in table_columns
            },
        )
        exported += 1

        # ─────────────────────────────────────
        # Line chart hashes
        # bench:line:Q2:df:end_to_end_s:mean
        #   field worker_count -> value
        # ─────────────────────────────────────
        for stat in AGG_NUMERIC_STATS:
            key = "bench:line:" + query + ":" + impl + ":" + phase + ":" + stat.replace("_s", "")
            pipe.hset(key, worker_count, safe_float(row[stat]))
            exported += 1

    pipe.execute()

    print("[✓] Aggregate benchmark esportato")
    print("[INFO] Righe aggregate lette: " + str(len(rows)))
    print("[INFO] Elementi Redis scritti: " + str(exported))


# ─────────────────────────────────────────────
# Export speedup
# ─────────────────────────────────────────────

def export_speedup(r, aggregate_rows):
    """
    Calcola speedup rispetto al worker_count minimo disponibile.

    Per ogni combinazione:
      query, impl, phase

    speedup(N) = mean_s(base_worker) / mean_s(N)
    """
    print("\n" + "=" * 72)
    print("EXPORT BENCHMARK SPEEDUP")
    print("=" * 72)

    groups = {}

    for row in aggregate_rows:
        query = clean_value(row["query"])
        impl = clean_value(row["impl"])
        phase = clean_value(row["phase"])
        worker = safe_int(row["worker_count"])
        mean_s = safe_float(row["mean_s"])

        key = (query, impl, phase)

        if key not in groups:
            groups[key] = {}

        groups[key][worker] = mean_s

    pipe = r.pipeline(transaction=False)
    exported = 0

    for (query, impl, phase), values_by_worker in groups.items():
        workers = sorted(values_by_worker.keys())

        if not workers:
            continue

        base_worker = workers[0]
        base_time = values_by_worker[base_worker]

        if base_time <= 0:
            continue

        redis_key = "bench:line:" + query + ":" + impl + ":" + phase + ":speedup"

        for worker in workers:
            current_time = values_by_worker[worker]

            if current_time <= 0:
                speedup = 0.0
            else:
                speedup = round(base_time / current_time, 4)

            pipe.hset(redis_key, str(worker), speedup)
            exported += 1

    pipe.execute()

    print("[✓] Speedup esportato")
    print("[INFO] Elementi speedup scritti: " + str(exported))


# ─────────────────────────────────────────────
# Export raw timings
# ─────────────────────────────────────────────

RAW_REQUIRED_COLUMNS = [
    "worker_count",
    "query",
    "impl",
    "iteration",
    "phase",
    "time_s",
]


def export_raw_rows(r, rows):
    print("\n" + "=" * 72)
    print("EXPORT BENCHMARK RAW ITERATIONS")
    print("=" * 72)

    require_columns(rows, RAW_REQUIRED_COLUMNS, RAW_CSV_PATH)

    pipe = r.pipeline(transaction=False)
    exported = 0

    # Metadata table per raw, utile per debug
    for idx, row in enumerate(rows, start=1):
        row_id = str(idx)

        worker_count = str(safe_int(row["worker_count"]))
        query = clean_value(row["query"])
        impl = clean_value(row["impl"])
        iteration = str(safe_int(row["iteration"]))
        phase = clean_value(row["phase"])
        time_s = safe_float(row["time_s"])

        # Per box plot:
        # benchraw:Q2:df:end_to_end_s:1
        #   iteration -> time_s
        raw_key = (
            "benchraw:"
            + query + ":"
            + impl + ":"
            + phase + ":"
            + worker_count
        )

        pipe.hset(raw_key, iteration, time_s)
        exported += 1

        # Tabella raw column-oriented, se vuoi visualizzarla/debuggarla
        pipe.hset("benchraw:table:worker_count", row_id, worker_count)
        pipe.hset("benchraw:table:query", row_id, query)
        pipe.hset("benchraw:table:impl", row_id, impl)
        pipe.hset("benchraw:table:iteration", row_id, iteration)
        pipe.hset("benchraw:table:phase", row_id, phase)
        pipe.hset("benchraw:table:time_s", row_id, time_s)
        exported += 6

    pipe.execute()

    print("[✓] Raw timings esportati")
    print("[INFO] Righe raw lette: " + str(len(rows)))
    print("[INFO] Elementi Redis scritti: " + str(exported))


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print("=" * 72)
    print("SABD Project 1 - Export benchmark to Redis")
    print("=" * 72)
    print("[INFO] Aggregate CSV: " + AGGREGATE_CSV_PATH)
    print("[INFO] Raw CSV:       " + RAW_CSV_PATH)
    print("[INFO] Redis host:    " + REDIS_HOST)
    print("[INFO] Redis port:    " + str(REDIS_PORT))

    r = connect_redis()

    deleted_bench = delete_keys_by_prefix(r, "bench:")
    deleted_raw = delete_keys_by_prefix(r, "benchraw:")

    print("[INFO] Eliminate chiavi bench:*    " + str(deleted_bench))
    print("[INFO] Eliminate chiavi benchraw:* " + str(deleted_raw))

    aggregate_rows = read_csv(AGGREGATE_CSV_PATH)
    raw_rows = read_csv(RAW_CSV_PATH)

    export_aggregate_rows(r, aggregate_rows)
    export_speedup(r, aggregate_rows)
    export_raw_rows(r, raw_rows)

    print("\n" + "=" * 72)
    print("[✓] Export benchmark completato")
    print("=" * 72)


if __name__ == "__main__":
    main()