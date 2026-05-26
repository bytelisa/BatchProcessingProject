"""
query3_rdd.py
─────────────
Query 3 - SABD Project 1 — Implementazione con RDD + approxQuantile nativa

Per le compagnie AA, DL, UA, WN:

  [CSV 1 — query3_rdd_hourly_percentiles]
  - Ricava l'ora dal campo CRS_DEP_TIME (HHMM intero: 830→8, 1245→12)
  - Per ciascuna compagnia × fascia oraria (0–23), sui soli voli non
    cancellati, calcola P25, P50, P75, P90 di DEP_DELAY
  - Tecnica: RDD groupByKey per partizionare i dati per (airline, hour),
    poi approxQuantile di Spark (Greenwald-Khanna) su mini-DataFrame

  [CSV 2 — query3_rdd_global_minmax]
  - Per ciascuna compagnia: min e max assoluto di DEP_DELAY
  - Tecnica: combineByKey su RDD

Parametri di run_query3_rdd():
  save_output   (bool, default True)  — se False salta la scrittura CSV
  print_preview (bool, default True)  — se False salta la stampa dell'anteprima

Output:
- CSV locale  → /opt/results/query3_rdd_hourly_percentiles.csv
                /opt/results/query3_rdd_global_minmax.csv
- CSV su HDFS → hdfs://.../query3_rdd_hourly_percentiles/
                hdfs://.../query3_rdd_global_minmax/
"""

import csv
import os
import time

from pyspark.sql.types import DoubleType, StructType, StructField

from utils import (
    get_spark_session,
    HDFS_BASE,
    LOCAL_OUT_PATH,
    HDFS_OUT_PATH,
)

# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

PARQUET_PATH = f"{HDFS_BASE}/data/processed/flights/parquet"

TARGET_AIRLINES = {"AA", "DL", "UA", "WN"}

RELATIVE_ERROR = 0.001   # stesso di query3.py (accuracy=1000)

OUTPUT_PERCENTILES = "query3_rdd_hourly_percentiles"
OUTPUT_MINMAX      = "query3_rdd_global_minmax"

HDFS_OUTPUT_PERC   = f"{HDFS_OUT_PATH}/{OUTPUT_PERCENTILES}"
HDFS_OUTPUT_MM     = f"{HDFS_OUT_PATH}/{OUTPUT_MINMAX}"
LOCAL_OUTPUT_PERC  = f"{LOCAL_OUT_PATH}/{OUTPUT_PERCENTILES}.csv"
LOCAL_OUTPUT_MM    = f"{LOCAL_OUT_PATH}/{OUTPUT_MINMAX}.csv"

CSV_HEADER_PERC    = "airline,hour,num_flights,p25,p50,p75,p90"
CSV_HEADER_MM      = "airline,min_delay,max_delay"

DELAY_SCHEMA = StructType([StructField("DEP_DELAY", DoubleType(), True)])


# ─────────────────────────────────────────────
# Salvataggio RDD — nessun DataFrame coinvolto
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
    Esegue la Query 3 con API RDD + approxQuantile nativa di Spark.

    Parametri:
      save_output   — se False salta la scrittura CSV (benchmark: misura
                      solo loading + computation, non I/O su HDFS)
      print_preview — se False salta la stampa dell'anteprima a console
                      (benchmark: evita di inquinare wall_total_s)

    Struttura in due rami:

    ┌─────────────────────────────────────────────────────────────────┐
    │  Ramo A — Percentili per (airline, hour)                        │
    │  groupByKey → collect() → per gruppo: mini-DF + approxQuantile  │
    └─────────────────────────────────────────────────────────────────┘
    ┌─────────────────────────────────────────────────────────────────┐
    │  Ramo B — Min/Max globali per airline                           │
    │  combineByKey → aggrega (min,max) per partizione prima shuffle  │
    └─────────────────────────────────────────────────────────────────┘

    Nota: approxQuantile e percentile_approx usano lo stesso algoritmo
    Greenwald-Khanna con lo stesso relative_error. La differenza è che
    percentile_approx (DataFrame) costruisce lo sketch distribuito sugli
    executor, mentre approxQuantile (qui) richiede collect() per gruppo
    sul driver — overhead misurabile nel confronto dei tempi.
    """

    timings = {}
    sc = spark.sparkContext

    # ─────────────────────────────────────────────
    # 1. Loading
    # ─────────────────────────────────────────────

    print("\n[1] Lettura Parquet e conversione in RDD...")
    print(f"    Input path: {PARQUET_PATH}")

    t0 = time.time()

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
        .filter(lambda r: r[0] in TARGET_AIRLINES)
        .filter(lambda r: r[3] == 0)
        .filter(lambda r: r[2] is not None)
        .filter(lambda r: r[1] is not None)
        .map(lambda r: (r[0], int(r[1]) // 100, r[2]))   # (airline, hour, dep_delay)
        .filter(lambda r: 0 <= r[1] <= 23)
        .cache()
    )

    timings["loading_s"] = round(time.time() - t0, 3)
    print(f"    Loading completato in {timings['loading_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 2a. Ramo A — Percentili con approxQuantile
    # ─────────────────────────────────────────────

    print("\n[2a] Calcolo percentili P25/P50/P75/P90 (groupByKey + approxQuantile)...")
    print(f"     relative_error = {RELATIVE_ERROR}")

    t1 = time.time()

    groups = (
        rdd_base
        .map(lambda r: ((r[0], r[1]), r[2]))
        .groupByKey()
        .collect()
    )

    rows_percentiles = []
    for (airline, hour), vals in groups:
        delay_list = list(vals)
        n = len(delay_list)
        if n == 0:
            rows_percentiles.append((airline, hour, 0, None, None, None, None))
            continue
        mini_df = spark.createDataFrame(
            [(float(v),) for v in delay_list],
            schema=DELAY_SCHEMA
        )
        quantiles = mini_df.approxQuantile(
            "DEP_DELAY", [0.25, 0.50, 0.75, 0.90], RELATIVE_ERROR
        )
        p25, p50, p75, p90 = (round(q, 4) for q in quantiles)
        rows_percentiles.append((airline, hour, n, p25, p50, p75, p90))

    rows_percentiles.sort(key=lambda r: (r[0], r[1]))

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
    #    Saltato durante il benchmark (save_output=False) per non
    #    inquinare la misurazione con latenza I/O su HDFS.
    # ─────────────────────────────────────────────

    if save_output:
        print("\n[4] Salvataggio CSV (RDD puro)...")
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
# Main — esecuzione standalone
# ─────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 3 RDD: approxQuantile Percentiles")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query3-RDD")

    try:
        # Esecuzione standalone: save_output e print_preview entrambi True
        _, _, timings = run_query3_rdd(spark, save_output=True, print_preview=True)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 3 (RDD + approxQuantile)")
        print("=" * 72)
        print(f"  Loading:               {timings['loading_s']:.3f}s")
        print(f"  Percentili (2a):       {timings['computation_percentiles_s']:.3f}s")
        print(f"  Min/Max (2b):          {timings['computation_minmax_s']:.3f}s")
        print(f"  Output:                {timings.get('output_s', 0):.3f}s")
        print(f"  Totale (no output):    {timings['total_s']:.3f}s")
        print(f"  Totale script:         {total_elapsed:.3f}s")
        print("\n[✓] Query 3 RDD completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()