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

Parametri di run_query1():
  save_output   (bool, default True)  — se False salta la scrittura CSV;
                usato dal benchmark per non inquinare i tempi di computazione
  print_preview (bool, default True)  — se False salta result.show();
                usato dal benchmark per non inquinare wall_total_s

Output:
- CSV locale in /opt/output/query1_monthly_stats
- CSV HDFS in /data/processed/flights/query1_monthly_stats
"""

import time

from pyspark.sql import functions as F

from utils import (
    get_spark_session,
    save_csv,
)


from config import HDFS_PROCESSED_PARQUET_PATH

PARQUET_PATH = HDFS_PROCESSED_PARQUET_PATH

OUTPUT_NAME = "query1_monthly_stats"

TARGET_AIRLINES = ["AA", "DL"]


def run_query1(spark, save_output=True, print_preview=True):
    """
    Esegue la Query 1 usando DataFrame API.

    Parametri:
      save_output   — se False salta la scrittura CSV (benchmark: misura
                      solo loading + computation, non I/O su HDFS)
      print_preview — se False salta result.show() a console
                      (benchmark: evita di inquinare wall_total_s)

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

    df = df.select(
        "MONTH",
        "OP_UNIQUE_CARRIER",
        "DEP_DELAY",
        "ARR_DELAY",
        "CANCELLED",
    )

    timings["loading_s"] = round(time.time() - t0, 3)
    print(f"    Loading completato in {timings['loading_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 2. Filtering
    # ─────────────────────────────────────────────

    print("\n[2] Filtro compagnie AA e DL...")

    t1 = time.time()

    df_filtered = df.filter(
        F.col("OP_UNIQUE_CARRIER").isin(TARGET_AIRLINES)
    )

    timings["filtering_s"] = round(time.time() - t1, 3)
    print(f"    Filtering completato in {timings['filtering_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 3. Aggregation
    # ─────────────────────────────────────────────

    print("\n[3] Aggregazione mensile...")

    t2 = time.time()

    is_not_cancelled = F.col("CANCELLED") == F.lit(0)
    is_cancelled     = F.col("CANCELLED") == F.lit(1)

    result = (
        df_filtered
        .groupBy(
            F.col("MONTH").alias("month"),
            F.col("OP_UNIQUE_CARRIER").alias("airline"),
        )
        .agg(
            F.count(F.lit(1)).alias("total_flights"),
            F.sum(is_cancelled.cast("long")).alias("cancelled_flights"),
            F.sum(is_not_cancelled.cast("long")).alias("non_cancelled_flights"),

            F.avg(F.when(is_not_cancelled, F.col("DEP_DELAY"))).alias("dep_delay_mean"),
            F.min(F.when(is_not_cancelled, F.col("DEP_DELAY"))).alias("dep_delay_min"),
            F.max(F.when(is_not_cancelled, F.col("DEP_DELAY"))).alias("dep_delay_max"),

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
            F.round("dep_delay_min",  4).alias("dep_delay_min"),
            F.round("dep_delay_max",  4).alias("dep_delay_max"),
            F.round("arr_delay_mean", 4).alias("arr_delay_mean"),
            F.round("arr_delay_min",  4).alias("arr_delay_min"),
            F.round("arr_delay_max",  4).alias("arr_delay_max"),
            F.round("cancellation_rate", 4).alias("cancellation_rate"),
        )
        .orderBy("airline", "month")
    )

    # Materializza per misurare il tempo di computazione
    result_count = result.count()

    timings["computation_s"] = round(time.time() - t2, 3)
    print(f"    Aggregazione completata in {timings['computation_s']:.2f}s")
    print(f"    Righe risultato: {result_count}")

    # ─────────────────────────────────────────────
    # 4. Anteprima (opzionale)
    # ─────────────────────────────────────────────

    if print_preview:
        print("\n[4] Anteprima risultato:")
        result.show(20, truncate=False)

    # ─────────────────────────────────────────────
    # 5. Output CSV (opzionale)
    #    Saltato durante il benchmark (save_output=False) per non
    #    inquinare la misurazione con latenza I/O su HDFS.
    # ─────────────────────────────────────────────

    if save_output:
        print("\n[5] Salvataggio risultato CSV...")
        t3 = time.time()
        save_csv(result, OUTPUT_NAME, local=True)
        save_csv(result, OUTPUT_NAME, local=False)
        timings["output_s"] = round(time.time() - t3, 3)
        print(f"    Output completato in {timings['output_s']:.2f}s")

    timings["total_s"] = round(
        timings["loading_s"] + timings["filtering_s"] + timings["computation_s"], 3
    )

    return result, timings


def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 1: Monthly Delay and Cancellation Stats")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query1")

    try:
        # Esecuzione standalone: save_output e print_preview entrambi True
        _, timings = run_query1(spark, save_output=True, print_preview=True)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 1")
        print("=" * 72)
        print(f"  Loading:      {timings['loading_s']:.3f}s")
        print(f"  Filtering:    {timings['filtering_s']:.3f}s")
        print(f"  Computation:  {timings['computation_s']:.3f}s")
        print(f"  Output:       {timings.get('output_s', 0):.3f}s")
        print(f"  Totale:       {total_elapsed:.3f}s")
        print("\n[✓] Query 1 completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()