"""
query1_rdd.py
─────────────
Query 1 - SABD Project 1 — Implementazione con RDD

Per le compagnie AA e DL, aggregare i dati su base mensile e calcolare:
- statistiche DEP_DELAY sui soli voli non cancellati: mean, min, max
- statistiche ARR_DELAY sui soli voli non cancellati: mean, min, max
- cancellation rate: percentuale di voli cancellati sul totale

Questa implementazione usa l'API RDD di basso livello e può essere
confrontata con la versione DataFrame (query1.py) per valutare le
differenze di performance.

Output:
- CSV locale in /opt/results/query1_rdd_monthly_stats.csv
- CSV HDFS in /data/processed/flights/query1_rdd_monthly_stats
"""

import time
import statistics

from pyspark import SparkContext
from pyspark.sql import SparkSession

from utils import (
    get_spark_session,
    save_csv,
    HDFS_BASE,
)

# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

PARQUET_PATH = f"{HDFS_BASE}/data/processed/flights/parquet"
OUTPUT_NAME  = "query1_rdd_monthly_stats"

TARGET_AIRLINES = {"AA", "DL"}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def to_row_dict(row):
    """
    Converte una Row Spark (letta da Parquet) in un dizionario Python.
    I campi numerici possono essere None per voli cancellati.
    """
    return {
        "carrier":   row["OP_UNIQUE_CARRIER"],
        "month":     row["MONTH"],
        "dep_delay": row["DEP_DELAY"],   # float o None
        "arr_delay": row["ARR_DELAY"],   # float o None
        "cancelled": row["CANCELLED"],   # int: 0 o 1
    }


def run_query1_rdd(spark):
    """
    Esegue la Query 1 usando l'API RDD.

    Strategia a due rami + join:
    ┌─────────────────────────────┐
    │  Ramo 1: delay stats        │  solo voli NON cancellati
    │  combineByKey → (sum,min,   │  → mean/min/max DEP e ARR
    │                  max, count)│
    └────────────┬────────────────┘
                 │ join su (month, carrier)
    ┌────────────┴────────────────┐
    │  Ramo 2: cancel rate        │  TUTTI i voli
    │  reduceByKey → (n_canc,     │  → cancellation_rate %
    │                 n_tot)      │
    └─────────────────────────────┘

    Nota: combineByKey è preferibile a groupByKey perché aggrega
    parzialmente all'interno di ogni partizione PRIMA del shuffle,
    riducendo i dati trasferiti in rete tra i nodi.
    """

    timings = {}

    sc = spark.sparkContext

    # ─────────────────────────────────────────────
    # 1. Loading — legge il Parquet tramite DataFrame
    #    poi converte in RDD di dizionari
    # ─────────────────────────────────────────────

    print("\n[1] Lettura Parquet e conversione in RDD...")
    print(f"    Input path: {PARQUET_PATH}")

    t0 = time.time()

    df_raw = spark.read.parquet(PARQUET_PATH).select(
        "MONTH",
        "OP_UNIQUE_CARRIER",
        "DEP_DELAY",
        "ARR_DELAY",
        "CANCELLED",
    )

    # Conversione DataFrame → RDD di dizionari e filtraggio compagnie
    rdd_base = (
        df_raw.rdd
        .map(to_row_dict)
        .filter(lambda r: r["carrier"] in TARGET_AIRLINES)
        .cache()   # riutilizzato nei due rami
    )

    timings["loading_s"] = time.time() - t0
    print(f"    Loading completato in {timings['loading_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 2. Ramo 1 — Statistiche ritardi
    #    Solo voli NON cancellati, con DEP_DELAY/ARR_DELAY non nulli
    # ─────────────────────────────────────────────

    print("\n[2] Calcolo statistiche ritardi (RDD)...")

    t1 = time.time()

    # Accumulatore per una singola metrica: (somma, min, max, count)
    def create_combiner(v):
        return (v, v, v, 1)

    def merge_value(acc, v):
        return (acc[0] + v, min(acc[1], v), max(acc[2], v), acc[3] + 1)

    def merge_combiners(a, b):
        return (a[0] + b[0], min(a[1], b[1]), max(a[2], b[2]), a[3] + b[3])

    def acc_to_stats(acc):
        """(somma, min, max, count) → (mean, min, max) arrotondati a 4 decimali"""
        mean = round(acc[0] / acc[3], 4)
        mn   = round(acc[1], 4)
        mx   = round(acc[2], 4)
        return (mean, mn, mx)

    # DEP_DELAY stats
    dep_stats = (
        rdd_base
        .filter(lambda r: r["cancelled"] == 0 and r["dep_delay"] is not None)
        .map(lambda r: ((r["month"], r["carrier"]), r["dep_delay"]))
        .combineByKey(create_combiner, merge_value, merge_combiners)
        .mapValues(acc_to_stats)
    )

    # ARR_DELAY stats
    arr_stats = (
        rdd_base
        .filter(lambda r: r["cancelled"] == 0 and r["arr_delay"] is not None)
        .map(lambda r: ((r["month"], r["carrier"]), r["arr_delay"]))
        .combineByKey(create_combiner, merge_value, merge_combiners)
        .mapValues(acc_to_stats)
    )

    # Unisce dep e arr stats: stessa key (month, carrier)
    # delay_combined: key → ((dep_mean, dep_min, dep_max), (arr_mean, arr_min, arr_max))
    delay_combined = dep_stats.join(arr_stats)

    # ─────────────────────────────────────────────
    # 3. Ramo 2 — Cancellation rate (tutti i voli)
    # ─────────────────────────────────────────────

    cancel_stats = (
        rdd_base
        .map(lambda r: (
            (r["month"], r["carrier"]),
            (r["cancelled"], 1)          # (n_cancellati, n_totali)
        ))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda v: round((v[0] / v[1]) * 100.0, 4))
    )

    timings["computation_s"] = time.time() - t1
    print(f"    Computation completata in {timings['computation_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 4. Join dei due rami e costruzione risultato finale
    # ─────────────────────────────────────────────

    print("\n[3] Join delay stats + cancel rate...")

    result_rdd = (
        delay_combined
        .join(cancel_stats)
        # kv = ((month, carrier), (((dep_mean,dep_min,dep_max),(arr_mean,arr_min,arr_max)), cancel_rate))
        .map(lambda kv: (
            kv[0][1],                   # airline
            kv[0][0],                   # month
            kv[1][0][0][0],             # dep_delay_mean
            kv[1][0][0][1],             # dep_delay_min
            kv[1][0][0][2],             # dep_delay_max
            kv[1][0][1][0],             # arr_delay_mean
            kv[1][0][1][1],             # arr_delay_min
            kv[1][0][1][2],             # arr_delay_max
            kv[1][1],                   # cancellation_rate
        ))
        .sortBy(lambda r: (r[0], r[1]))  # ordina per airline, month
    )

    # ─────────────────────────────────────────────
    # 5. Materializzazione e conversione a DataFrame
    #    per usare save_csv() di utils.py
    # ─────────────────────────────────────────────

    print("\n[4] Materializzazione risultato...")

    rows = result_rdd.collect()
    result_count = len(rows)
    print(f"    Righe risultato: {result_count}")

    # ─────────────────────────────────────────────
    # 6. Anteprima console
    # ─────────────────────────────────────────────

    header = (
        "airline | month | dep_delay_mean | dep_delay_min | dep_delay_max |"
        " arr_delay_mean | arr_delay_min | arr_delay_max | cancellation_rate"
    )
    print("\n[5] Anteprima risultato (prime 10 righe):")
    print("-" * len(header))
    print(header)
    print("-" * len(header))
    for r in rows[:10]:
        print(
            f"  {r[0]:6s} | {r[1]:5d} | {r[2]:14.4f} | {r[3]:13.4f} | "
            f"{r[4]:13.4f} | {r[5]:14.4f} | {r[6]:13.4f} | {r[7]:13.4f} | {r[8]:17.4f}"
        )

    # ─────────────────────────────────────────────
    # 7. Salvataggio — convertiamo l'RDD in DataFrame
    #    per riutilizzare save_csv() di utils.py
    # ─────────────────────────────────────────────

    print("\n[6] Salvataggio risultati CSV...")

    t3 = time.time()

    from pyspark.sql import Row

    result_df = spark.createDataFrame(
        [Row(
            airline=r[0],
            month=r[1],
            dep_delay_mean=r[2],
            dep_delay_min=r[3],
            dep_delay_max=r[4],
            arr_delay_mean=r[5],
            arr_delay_min=r[6],
            arr_delay_max=r[7],
            cancellation_rate=r[8],
        ) for r in rows]
    ).select(
        "month", "airline",
        "dep_delay_mean", "dep_delay_min", "dep_delay_max",
        "arr_delay_mean", "arr_delay_min", "arr_delay_max",
        "cancellation_rate",
    ).orderBy("airline", "month")

    save_csv(result_df, OUTPUT_NAME, local=True)
    save_csv(result_df, OUTPUT_NAME, local=False)

    timings["output_s"] = time.time() - t3
    print(f"    Output completato in {timings['output_s']:.2f}s")

    timings["total_s"] = sum(timings.values())

    return result_df, timings


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 1 RDD: Monthly Delay and Cancellation Stats")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query1-RDD")

    try:
        _, timings = run_query1_rdd(spark)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 1 (RDD)")
        print("=" * 72)
        print(f"Loading:      {timings['loading_s']:.2f}s")
        print(f"Computation:  {timings['computation_s']:.2f}s")
        print(f"Output:       {timings['output_s']:.2f}s")
        print(f"Totale fasi:  {timings['total_s']:.2f}s")
        print(f"Totale script:{total_elapsed:.2f}s")

        print("\n[✓] Query 1 RDD completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()