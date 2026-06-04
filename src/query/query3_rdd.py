"""
query3_rdd.py
─────────────
Query 3 — Implementazione RDD + t-digest

Usa mapPartitions per costruire un TDigest locale per partizione,
poi raccoglie i digest sul driver e li fonde con update_centroids_from_list.
Tutto il codice t-digest è inline nelle lambda/funzioni di mapPartitions,
evitando dipendenze da moduli esterni non disponibili sul worker.

FIX ModuleNotFoundError:
    _build_digests_partition era definita a livello di modulo (top-level).
    cloudpickle la serializzava come riferimento a 'query3_rdd._build_digests_partition',
    e gli executor fallivano con "No module named 'query3_rdd'".
    Soluzione: spostata come closure locale dentro run_query3_rdd(),
    così cloudpickle serializza il corpo completo senza riferimenti al modulo.
"""

import csv
import os
import time

from utils import (
    get_spark_session,
    LOCAL_OUT_PATH,
    HDFS_OUT_PATH,
)

from config import HDFS_PROCESSED_PARQUET_PATH

PARQUET_PATH = HDFS_PROCESSED_PARQUET_PATH
# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────



TARGET_AIRLINES = {"AA", "DL", "UA", "WN"}

TDIGEST_DELTA = 0.01

OUTPUT_PERCENTILES = "query3_rdd_hourly_percentiles"
OUTPUT_MINMAX      = "query3_rdd_global_minmax"

HDFS_OUTPUT_PERC   = f"{HDFS_OUT_PATH}/{OUTPUT_PERCENTILES}"
HDFS_OUTPUT_MM     = f"{HDFS_OUT_PATH}/{OUTPUT_MINMAX}"
LOCAL_OUTPUT_PERC  = f"{LOCAL_OUT_PATH}/{OUTPUT_PERCENTILES}.csv"
LOCAL_OUTPUT_MM    = f"{LOCAL_OUT_PATH}/{OUTPUT_MINMAX}.csv"

CSV_HEADER_PERC    = "airline,hour,num_flights,p25,p50,p75,p90"
CSV_HEADER_MM      = "airline,min_delay,max_delay"


# ─────────────────────────────────────────────
# Salvataggio
# ─────────────────────────────────────────────

def save_rdd_local(rows, filepath, header):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header.split(","))
        writer.writerows(rows)
    print(f"[✓] CSV locale salvato in: {filepath}")


def delete_hdfs_if_exists(sc, hdfs_path):
    jvm  = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(hdfs_path)
    fs   = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    if fs.exists(path):
        fs.delete(path, True)
        print(f"    [!] Path HDFS esistente eliminato: {hdfs_path}")


def save_rdd_hdfs(sc, rows, hdfs_path, header, row_to_str):
    delete_hdfs_if_exists(sc, hdfs_path)
    header_rdd = sc.parallelize([header])
    data_rdd   = sc.parallelize(rows).map(row_to_str)
    (
        header_rdd
        .union(data_rdd)
        .coalesce(1)
        .saveAsTextFile(hdfs_path)
    )
    print(f"[✓] CSV HDFS salvato in: {hdfs_path}")


# ─────────────────────────────────────────────
# Core
# ─────────────────────────────────────────────

def run_query3_rdd(spark, save_output=True, print_preview=True):
    """
    Esegue la Query 3 con API RDD + t-digest.

    Struttura:

    ┌──────────────────────────────────────────────────────────────────┐
    │  Ramo A — Percentili per (airline, hour)                         │
    │  mapPartitions: ogni partizione costruisce i propri TDigest      │
    │  reduceByKey: fonde i digest tra partizioni sul driver           │
    │               tramite update_centroids_from_list                 │
    │  collect + percentile(): estrae P25/P50/P75/P90                  │
    └──────────────────────────────────────────────────────────────────┘
    ┌──────────────────────────────────────────────────────────────────┐
    │  Ramo B — Min/Max globali per airline                            │
    │  combineByKey → aggrega (min, max) per partizione                │
    └──────────────────────────────────────────────────────────────────┘
    """

    timings = {}
    sc = spark.sparkContext

    # ── FIX: closure locale — cloudpickle serializza il corpo per intero ──
    # _build_digests_partition era top-level: cloudpickle registrava un
    # riferimento a 'query3_rdd._build_digests_partition' e gli executor
    # fallivano con ModuleNotFoundError. Definita qui dentro, viene
    # serializzata per valore senza alcun riferimento al modulo.
    def _build_digests_partition(iterator):
        """
        Riceve un iteratore di (airline, hour, dep_delay) per una partizione.
        Restituisce una lista di ((airline, hour), TDigest).
        """
        from tdigest import TDigest  # import locale: incluso nella closure
        local = {}
        for airline, hour, delay in iterator:
            key = (airline, hour)
            if key not in local:
                local[key] = TDigest(delta=0.01)
            local[key].update(delay)
        return list(local.items())
    # ── FINE FIX ──────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────
    # 1. Loading + Filtering
    # ─────────────────────────────────────────────

    print("\n[1] Lettura Parquet e conversione in RDD...")
    print(f"    Input path: {PARQUET_PATH}")

    t0 = time.time()

    target = TARGET_AIRLINES

    rdd_base = (
        spark.read.parquet(PARQUET_PATH)
        .select("OP_UNIQUE_CARRIER", "CRS_DEP_TIME", "DEP_DELAY", "CANCELLED")
        .rdd
        .map(lambda row: (
            row["OP_UNIQUE_CARRIER"],
            row["CRS_DEP_TIME"],
            row["DEP_DELAY"],
            row["CANCELLED"],
        ))
        .filter(lambda r: r[0] in target)
        .filter(lambda r: r[3] == 0)
        .filter(lambda r: r[2] is not None)
        .filter(lambda r: r[1] is not None)
        .map(lambda r: (r[0], int(r[1]) // 100, r[2]))
        .filter(lambda r: 0 <= r[1] <= 23)
        .cache()
    )

    rdd_base.count()

    timings["loading_s"] = round(time.time() - t0, 3)
    print(f"    Loading completato in {timings['loading_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 2a. Ramo A — Percentili con t-digest
    # ─────────────────────────────────────────────

    print("\n[2a] Calcolo percentili P25/P50/P75/P90 con t-digest...")
    print(f"     delta = {TDIGEST_DELTA}")

    t1 = time.time()

    def merge_digests(td1, td2):
        td1.update_centroids_from_list(td2.centroids_to_list())
        return td1

    groups_with_digest = (
        rdd_base
        .mapPartitions(_build_digests_partition)
        .reduceByKey(merge_digests)
        .collect()
    )

    rows_percentiles = []
    for (airline, hour), td in groups_with_digest:
        if td is None:
            rows_percentiles.append((airline, hour, 0, None, None, None, None))
            continue
        n   = int(td.n) if hasattr(td, 'n') else 0
        p25 = round(td.percentile(25), 4)
        p50 = round(td.percentile(50), 4)
        p75 = round(td.percentile(75), 4)
        p90 = round(td.percentile(90), 4)
        rows_percentiles.append((airline, hour, n, p25, p50, p75, p90))

    rows_percentiles.sort(key=lambda r: (r[0], r[1]))

    # Azione che forza l'esecuzione
    rows_percentiles.count()

    timings["computation_percentiles_s"] = round(time.time() - t1, 3)
    print(f"    Calcolo percentili completato in {timings['computation_percentiles_s']:.2f}s")
    print(f"    Righe risultato: {len(rows_percentiles)}")

    # ─────────────────────────────────────────────
    # 2b. Ramo B — Min/Max con combineByKey
    # ─────────────────────────────────────────────

    print("\n[2b] Calcolo min/max globali per airline (combineByKey)...")

    t2 = time.time()

    rows_minmax = (
        rdd_base
        .map(lambda r: (r[0], r[2]))
        .combineByKey(
            lambda v:      (v, v),
            lambda acc, v: (min(acc[0], v), max(acc[1], v)),
            lambda a, b:   (min(a[0], b[0]), max(a[1], b[1]))
        )
        .map(lambda kv: (kv[0], round(kv[1][0], 4), round(kv[1][1], 4)))
        .sortBy(lambda r: r[0])
        .collect()
    )

    timings["computation_minmax_s"] = round(time.time() - t2, 3)
    print(f"    Calcolo min/max completato in {timings['computation_minmax_s']:.2f}s")
    print(f"    Righe risultato: {len(rows_minmax)}")

    # ─────────────────────────────────────────────
    # 3. Anteprima console (opzionale)
    # ─────────────────────────────────────────────

    if print_preview:
        print("\n[3a] Anteprima percentili (prime 12 righe):")
        print(f"  {'airline':8s} {'hour':4s} {'n_flights':10s} "
              f"{'p25':8s} {'p50':8s} {'p75':8s} {'p90':8s}")
        print("  " + "-" * 58)
        for r in rows_percentiles[:12]:
            print(f"  {r[0]:8s} {r[1]:4d} {r[2]:10d} "
                  f"{str(r[3]):8s} {str(r[4]):8s} {str(r[5]):8s} {str(r[6]):8s}")

        print("\n[3b] Min/Max globali:")
        print(f"  {'airline':8s} {'min_delay':10s} {'max_delay':10s}")
        print("  " + "-" * 32)
        for r in rows_minmax:
            print(f"  {r[0]:8s} {r[1]:10.4f} {r[2]:10.4f}")

    # ─────────────────────────────────────────────
    # 4. Output CSV (opzionale)
    # ─────────────────────────────────────────────

    if save_output:
        print("\n[4] Salvataggio CSV (RDD + t-digest)...")
        t3 = time.time()

        save_rdd_local(rows_percentiles, LOCAL_OUTPUT_PERC, CSV_HEADER_PERC)
        save_rdd_hdfs(
            sc, rows_percentiles, HDFS_OUTPUT_PERC, CSV_HEADER_PERC,
            row_to_str=lambda r: f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]},{r[5]},{r[6]}"
        )
        save_rdd_local(rows_minmax, LOCAL_OUTPUT_MM, CSV_HEADER_MM)
        save_rdd_hdfs(
            sc, rows_minmax, HDFS_OUTPUT_MM, CSV_HEADER_MM,
            row_to_str=lambda r: f"{r[0]},{r[1]},{r[2]}"
        )

        timings["output_s"] = round(time.time() - t3, 3)
        print(f"    Output completato in {timings['output_s']:.2f}s")

    rdd_base.unpersist()

    timings["total_s"] = round(
        timings["loading_s"] +
        timings["computation_percentiles_s"] +
        timings["computation_minmax_s"],
        3
    )

    return rows_percentiles, rows_minmax, timings


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 3 RDD: t-digest Percentiles")
    print("=" * 72)

    total_start = time.time()
    spark = get_spark_session("SABD-Query3-RDD-tdigest")

    try:
        _, _, timings = run_query3_rdd(spark, save_output=True, print_preview=True)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 3 (RDD + t-digest)")
        print("=" * 72)
        print(f"  Loading:               {timings['loading_s']:.3f}s")
        print(f"  Percentili (2a):       {timings['computation_percentiles_s']:.3f}s")
        print(f"  Min/Max (2b):          {timings['computation_minmax_s']:.3f}s")
        print(f"  Output:                {timings.get('output_s', 0):.3f}s")
        print(f"  Totale (no output):    {timings['total_s']:.3f}s")
        print(f"  Totale script:         {total_elapsed:.3f}s")
        print("\n[✓] Query 3 RDD (t-digest) completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()