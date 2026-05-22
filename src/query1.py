"""
query1.py
─────────
Query 1 - SABD Project 1

Per le compagnie AA e DL, aggregare i dati su base mensile e calcolare:

- statistiche DEP_DELAY sui soli voli non cancellati:
    mean, min, max

- statistiche ARR_DELAY sui soli voli non cancellati:
    mean, min, max

- cancellation rate:
    percentuale di voli cancellati sul totale dei voli osservati
    nel gruppo mese-compagnia

Output:
- CSV locale in /opt/results/query1_monthly_stats
- CSV HDFS in /data/processed/flights/query1_monthly_stats

"""

import time

from pyspark.sql import functions as F

from utils import (
    get_spark_session,
    save_csv,
    HDFS_BASE,
)


# Deve essere coerente con il path usato dal preprocess.py e dal DAG Airflow.
PARQUET_PATH = f"{HDFS_BASE}/data/processed/flights/parquet"

OUTPUT_NAME = "query1_monthly_stats"

TARGET_AIRLINES = ["AA", "DL"]


def run_query1(spark):
    """
    Esegue la Query 1 usando DataFrame API.

    Nota metodologica:
    - Per DEP_DELAY e ARR_DELAY si considerano solo voli non cancellati.
    - Per cancellation_rate si considerano tutti i voli osservati nel gruppo.
    """

    timings = {}

    # ─────────────────────────────────────────────
    # 1. Loading
    # ─────────────────────────────────────────────

    print("\n[1] Lettura dataset processed Parquet...")
    print(f"    Input path: {PARQUET_PATH}")

    t0 = time.time()

    df = spark.read.parquet(PARQUET_PATH)

    # Seleziono solo le colonne necessarie alla Q1.
    # Questo è utile con Parquet perché permette column pruning.
    df = df.select(
        "MONTH",
        "OP_UNIQUE_CARRIER",
        "DEP_DELAY",
        "ARR_DELAY",
        "CANCELLED",
    )

    timings["loading_s"] = time.time() - t0
    print(f"    Loading completato in {timings['loading_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 2. Filtering
    # ─────────────────────────────────────────────

    print("\n[2] Filtro compagnie AA e DL...")

    t1 = time.time()

    df_filtered = df.filter(
        F.col("OP_UNIQUE_CARRIER").isin(TARGET_AIRLINES)
    )

    timings["filtering_s"] = time.time() - t1
    print(f"    Filtering completato in {timings['filtering_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 3. Aggregation
    # ─────────────────────────────────────────────

    print("\n[3] Aggregazione mensile...")

    t2 = time.time()

    is_not_cancelled = F.col("CANCELLED") == F.lit(0)
    is_cancelled = F.col("CANCELLED") == F.lit(1)

    result = (
        df_filtered
        .groupBy(
            F.col("MONTH").alias("month"),
            F.col("OP_UNIQUE_CARRIER").alias("airline"),
        )
        .agg(
            # Conteggi di controllo
            F.count(F.lit(1)).alias("total_flights"),
            F.sum(is_cancelled.cast("long")).alias("cancelled_flights"),
            F.sum(is_not_cancelled.cast("long")).alias("non_cancelled_flights"),

            # DEP_DELAY: solo voli non cancellati
            F.avg(F.when(is_not_cancelled, F.col("DEP_DELAY"))).alias("dep_delay_mean"),
            F.min(F.when(is_not_cancelled, F.col("DEP_DELAY"))).alias("dep_delay_min"),
            F.max(F.when(is_not_cancelled, F.col("DEP_DELAY"))).alias("dep_delay_max"),

            # ARR_DELAY: solo voli non cancellati
            F.avg(F.when(is_not_cancelled, F.col("ARR_DELAY"))).alias("arr_delay_mean"),
            F.min(F.when(is_not_cancelled, F.col("ARR_DELAY"))).alias("arr_delay_min"),
            F.max(F.when(is_not_cancelled, F.col("ARR_DELAY"))).alias("arr_delay_max"),
        )
        .withColumn(
            "cancellation_rate",
            F.when(
                F.col("total_flights") > 0,
                F.col("cancelled_flights") / F.col("total_flights") * F.lit(100.0),
            ).otherwise(F.lit(None)),
        )
        .select(
            "month",
            "airline",
            "total_flights",
            "cancelled_flights",
            "non_cancelled_flights",
            F.round("dep_delay_mean", 4).alias("dep_delay_mean"),
            F.round("dep_delay_min", 4).alias("dep_delay_min"),
            F.round("dep_delay_max", 4).alias("dep_delay_max"),
            F.round("arr_delay_mean", 4).alias("arr_delay_mean"),
            F.round("arr_delay_min", 4).alias("arr_delay_min"),
            F.round("arr_delay_max", 4).alias("arr_delay_max"),
            F.round("cancellation_rate", 4).alias("cancellation_rate"),
        )
        .orderBy("airline", "month")
    )

    # Materializza la query per misurare il tempo di computazione.
    result_count = result.count()

    timings["computation_s"] = time.time() - t2
    print(f"    Aggregazione completata in {timings['computation_s']:.2f}s")
    print(f"    Righe risultato: {result_count}")

    # ─────────────────────────────────────────────
    # 4. Output
    # ─────────────────────────────────────────────

    print("\n[4] Anteprima risultato:")
    result.show(20, truncate=False)

    print("\n[5] Salvataggio risultato CSV...")

    t3 = time.time()

    # Salvataggio locale: visibile nella cartella ./results del progetto
    save_csv(result, OUTPUT_NAME, local=True)

    # Salvataggio HDFS: utile per pipeline completa e Airflow
    save_csv(result, OUTPUT_NAME, local=False)

    timings["output_s"] = time.time() - t3

    print(f"    Output completato in {timings['output_s']:.2f}s")

    timings["total_s"] = sum(timings.values())

    return result, timings


def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 1: Monthly Delay and Cancellation Stats")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query1")

    try:
        _, timings = run_query1(spark)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 1")
        print("=" * 72)
        print(f"Loading:      {timings['loading_s']:.2f}s")
        print(f"Filtering:    {timings['filtering_s']:.2f}s")
        print(f"Computation:  {timings['computation_s']:.2f}s")
        print(f"Output:       {timings['output_s']:.2f}s")
        print(f"Totale fasi:  {timings['total_s']:.2f}s")
        print(f"Totale script:{total_elapsed:.2f}s")

        print("\n[✓] Query 1 completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()