"""
query1_rdd.py
─────────────
Query 1 — Implementazione con RDD

Per le compagnie AA e DL, aggregare i dati su base mensile e calcolare:
- statistiche DEP_DELAY sui soli voli non cancellati: mean, min, max
- statistiche ARR_DELAY sui soli voli non cancellati: mean, min, max
- cancellation rate: percentuale di voli cancellati sul totale

L'output viene scritto direttamente come RDD su HDFS e come CSV locale
tramite Python puro, senza alcuna conversione a DataFrame — in modo da
misurare i tempi in modo pulito e confrontarli con query1.py (DataFrame).

Parametri di run_query1_rdd():
  save_output   (bool, default True)  — se False salta la scrittura CSV;
                usato dal benchmark per non inquinare i tempi di computazione
  print_preview (bool, default True)  — se False salta la stampa dell'anteprima;
                usato dal benchmark per non inquinare wall_total_s

Output:
- CSV locale  → /opt/output/query1_rdd_monthly_stats.csv
- CSV su HDFS → hdfs://.../query1_rdd_monthly_stats/
"""

import csv
import os
import time

from utils import (
    get_spark_session,
    LOCAL_OUT_PATH,
    HDFS_OUT_PATH,
)

# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

from config import HDFS_PROCESSED_PARQUET_PATH

PARQUET_PATH = HDFS_PROCESSED_PARQUET_PATH
OUTPUT_NAME  = "query1_rdd_monthly_stats"

HDFS_OUTPUT  = f"{HDFS_OUT_PATH}/{OUTPUT_NAME}"
LOCAL_OUTPUT = f"{LOCAL_OUT_PATH}/{OUTPUT_NAME}.csv"

CSV_HEADER = "month,airline,dep_delay_mean,dep_delay_min,dep_delay_max," \
             "arr_delay_mean,arr_delay_min,arr_delay_max,cancellation_rate"

TARGET_AIRLINES = {"AA", "DL"}


# ─────────────────────────────────────────────
# Salvataggio RDD — nessun DataFrame coinvolto
# ─────────────────────────────────────────────

def save_rdd_local(rows, filepath, header):
    """
    Scrive le righe (lista di tuple) in un file CSV locale
    usando Python puro — nessuna conversione a DataFrame.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header.split(","))
        writer.writerows(rows)
    print(f"[✓] CSV locale salvato in: {filepath}")


def delete_hdfs_if_exists(sc, hdfs_path):
    """
    Elimina il path HDFS se esiste, per permettere sovrascrittura.
    saveAsTextFile non supporta overwrite nativo: bisogna cancellare prima.
    Usa l'API Java di Hadoop tramite il gateway Py4J di Spark.
    """
    jvm  = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(hdfs_path)
    fs   = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    if fs.exists(path):
        fs.delete(path, True)   # True = ricorsivo
        print(f"    [!] Path HDFS esistente eliminato: {hdfs_path}")


def save_rdd_hdfs(result_rdd, hdfs_path, header):
    """
    Scrive l'RDD su HDFS come testo CSV usando saveAsTextFile.
    Prepende l'header tramite union — nessuna conversione a DataFrame.
    Elimina il path di destinazione se già esiste (overwrite).
    """
    sc = result_rdd.context
    delete_hdfs_if_exists(sc, hdfs_path)
    header_rdd = sc.parallelize([header])
    (
        header_rdd
        .union(result_rdd.map(
            lambda r: f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]},{r[5]},{r[6]},{r[7]},{r[8]}"
        ))
        .coalesce(1)
        .saveAsTextFile(hdfs_path)
    )
    print(f"[✓] CSV HDFS salvato in: {hdfs_path}")


# ─────────────────────────────────────────────
# Core
# ─────────────────────────────────────────────

def run_query1_rdd(spark, save_output=True, print_preview=True):
    """
    Esegue la Query 1 usando l'API RDD dall'inizio alla fine.

    Parametri:
      save_output   — se False salta la scrittura CSV (benchmark: misura
                      solo loading + computation, non I/O su HDFS)
      print_preview — se False salta la stampa dell'anteprima a console
                      (benchmark: evita di inquinare wall_total_s)

    """

    timings = {}

    # ─────────────────────────────────────────────
    # 1. Loading
    # ─────────────────────────────────────────────

    print("\n[1] Lettura Parquet e conversione in RDD...")
    print(f"    Input path: {PARQUET_PATH}")

    t0 = time.time()

    rdd_base = (
        spark.read.parquet(PARQUET_PATH)
        .select("MONTH", "OP_UNIQUE_CARRIER", "DEP_DELAY", "ARR_DELAY", "CANCELLED")
        .rdd
        .map(lambda row: (
            row["OP_UNIQUE_CARRIER"],   # 0: carrier
            row["MONTH"],               # 1: month
            row["DEP_DELAY"],           # 2: dep_delay (float o None)
            row["ARR_DELAY"],           # 3: arr_delay (float o None)
            row["CANCELLED"],           # 4: cancelled (int)
        ))
        .filter(lambda r: r[0] in TARGET_AIRLINES)
        .cache()
    )

    timings["loading_s"] = round(time.time() - t0, 3)
    print(f"    Loading completato in {timings['loading_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 2. Aggregazioni
    # ─────────────────────────────────────────────

    print("\n[2] Calcolo statistiche (RDD)...")

    t1 = time.time()

    def create_combiner(v):  return (v, v, v, 1)
    def merge_value(acc, v): return (acc[0]+v, min(acc[1],v), max(acc[2],v), acc[3]+1)
    def merge_combiners(a, b): return (a[0]+b[0], min(a[1],b[1]), max(a[2],b[2]), a[3]+b[3])
    def to_stats(acc): return (round(acc[0]/acc[3],4), round(acc[1],4), round(acc[2],4))

    dep_stats = (
        rdd_base
        .filter(lambda r: r[4] == 0 and r[2] is not None)
        .map(lambda r: ((r[1], r[0]), r[2]))
        .combineByKey(create_combiner, merge_value, merge_combiners)
        .mapValues(to_stats)
    )

    arr_stats = (
        rdd_base
        .filter(lambda r: r[4] == 0 and r[3] is not None)
        .map(lambda r: ((r[1], r[0]), r[3]))
        .combineByKey(create_combiner, merge_value, merge_combiners)
        .mapValues(to_stats)
    )

    cancel_stats = (
        rdd_base
        .map(lambda r: ((r[1], r[0]), (r[4], 1)))
        .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
        .mapValues(lambda v: round((v[0]/v[1])*100.0, 4))
    )

    result_rdd = (
        dep_stats
        .join(arr_stats)
        .join(cancel_stats)
        .map(lambda kv: (
            kv[0][0],           # month
            kv[0][1],           # airline
            kv[1][0][0][0],     # dep_delay_mean
            kv[1][0][0][1],     # dep_delay_min
            kv[1][0][0][2],     # dep_delay_max
            kv[1][0][1][0],     # arr_delay_mean
            kv[1][0][1][1],     # arr_delay_min
            kv[1][0][1][2],     # arr_delay_max
            kv[1][1],           # cancellation_rate
        ))
        .sortBy(lambda r: (r[1], r[0]))
    )

    rows = result_rdd.collect()

    timings["computation_s"] = round(time.time() - t1, 3)
    print(f"    Computation completata in {timings['computation_s']:.2f}s")
    print(f"    Righe risultato: {len(rows)}")

    # ─────────────────────────────────────────────
    # 3. Anteprima console (opzionale)
    # ─────────────────────────────────────────────

    if print_preview:
        print("\n[3] Anteprima risultato:")
        print(f"  {'month':5s} {'airline':8s} {'dep_mean':10s} {'dep_min':10s} {'dep_max':10s}"
              f" {'arr_mean':10s} {'arr_min':10s} {'arr_max':10s} {'canc_rate':10s}")
        print("  " + "-" * 90)
        for r in rows:
            print(
                f"  {r[0]:5d} {r[1]:8s} {r[2]:10.4f} {r[3]:10.4f} {r[4]:10.4f}"
                f" {r[5]:10.4f} {r[6]:10.4f} {r[7]:10.4f} {r[8]:10.4f}"
            )

    # ─────────────────────────────────────────────
    # 4. Output CSV (opzionale)
    #    Saltato durante il benchmark (save_output=False) per non
    #    inquinare la misurazione con latenza I/O su HDFS.
    # ─────────────────────────────────────────────

    if save_output:
        print("\n[4] Salvataggio CSV (RDD puro)...")
        t2 = time.time()
        save_rdd_local(rows, LOCAL_OUTPUT, CSV_HEADER)
        save_rdd_hdfs(result_rdd, HDFS_OUTPUT, CSV_HEADER)
        timings["output_s"] = round(time.time() - t2, 3)
        print(f"    Output completato in {timings['output_s']:.2f}s")

    rdd_base.unpersist()

    timings["total_s"] = round(
        timings["loading_s"] + timings["computation_s"], 3
    )

    return rows, timings


# ─────────────────────────────────────────────
# Main — esecuzione standalone
# ─────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 1 RDD: Monthly Delay and Cancellation Stats")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query1-RDD")

    try:
        # Esecuzione standalone: save_output e print_preview entrambi True
        _, timings = run_query1_rdd(spark, save_output=True, print_preview=True)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 1 (RDD)")
        print("=" * 72)
        print(f"  Loading:      {timings['loading_s']:.3f}s")
        print(f"  Computation:  {timings['computation_s']:.3f}s")
        print(f"  Output:       {timings.get('output_s', 0):.3f}s")
        print(f"  Totale:       {total_elapsed:.3f}s")
        print("\n[✓] Query 1 RDD completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()