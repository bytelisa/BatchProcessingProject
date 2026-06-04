"""
query3.py
─────────
Query 3 - Implementazione con Dataframe

Per le compagnie AA (American Airlines), DL (Delta), UA (United), WN (Southwest):

  [CSV 1 — query3_hourly_percentiles]
  - Ricava l'ora dal campo CRS_DEP_TIME (formato HHMM intero: 830 → 8, 1245 → 12)
  - Per ciascuna compagnia × fascia oraria (0–23):
      calcola P25, P50, P75, P90 di DEP_DELAY sui soli voli non cancellati
  - Tecnica: percentile_approx (Spark built-in, basato su Greenwald-Khanna sketch)

  [CSV 2 — query3_global_minmax]
  - Per ciascuna compagnia:
      calcola il minimo e il massimo assoluto di DEP_DELAY sull'intero dataset

Parametri di run_query3():
  save_output   (bool, default True)  — se False salta la scrittura CSV;
                usato dal benchmark per non inquinare i tempi di computazione
  print_preview (bool, default True)  — se False salta result.show();
                usato dal benchmark per non inquinare wall_total_s

Output:
  - CSV locale  → /opt/output/query3_hourly_percentiles.csv
                  /opt/output/query3_global_minmax.csv
  - CSV su HDFS → /data/output/query3_hourly_percentiles
                  /data/output/query3_global_minmax
"""

import time

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from utils import (
    get_spark_session,
    save_csv,
)

from config import HDFS_PROCESSED_PARQUET_PATH

PARQUET_PATH = HDFS_PROCESSED_PARQUET_PATH

# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

TARGET_AIRLINES = ["AA", "DL", "UA", "WN"]

PERCENTILE_ACCURACY = 1000   # errore relativo ≤ 1/1000

OUTPUT_PERCENTILES = "query3_hourly_percentiles"
OUTPUT_MINMAX      = "query3_global_minmax"


# ─────────────────────────────────────────────
# Core
# ─────────────────────────────────────────────

def run_query3(spark, save_output=True, print_preview=True):
    """
    Esegue la Query 3 usando DataFrame API.

    Parametri:
      save_output   — se False salta la scrittura CSV (benchmark: misura
                      solo loading + computation, non I/O su HDFS)
      print_preview — se False salta result.show() a console
                      (benchmark: evita di inquinare wall_total_s)

    Nota metodologica — scelta della tecnica per i percentili:
        Si usa F.percentile_approx, funzione nativa di Spark basata
        sull'algoritmo Greenwald-Khanna (sketch per quantili approssimati).
        Lavora in un singolo passaggio sui dati, non materializza l'intero
        ordinamento, ed è progettata per dataset di grandi dimensioni.
        Il parametro accuracy=1000 garantisce un errore relativo ≤ 1/1000.
    """

    timings = {}

    # ─────────────────────────────────────────────
    # 1. Loading  (column pruning grazie a Parquet)
    # ─────────────────────────────────────────────

    print("\n[1] Lettura dataset processed Parquet...")
    print(f"    Input path: {PARQUET_PATH}")

    t0 = time.time()

    df = (spark.read.parquet(PARQUET_PATH)
        .select(
        "OP_UNIQUE_CARRIER",
        "CRS_DEP_TIME",
        "DEP_DELAY",
        "CANCELLED",
        )
        .cache()
    )

    # Azione che forza l'esecuzione (per calcolo tempi)
    df.count()

    timings["loading_s"] = round(time.time() - t0, 3)
    print(f"    Loading completato in {timings['loading_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 2. Filtering
    # ─────────────────────────────────────────────

    print("\n[2] Filtro compagnie AA, DL, UA, WN e voli non cancellati...")

    t1 = time.time()

    df_filtered = (
        df
        .filter(F.col("OP_UNIQUE_CARRIER").isin(TARGET_AIRLINES))
        .filter(F.col("CANCELLED") == 0)
        .filter(F.col("DEP_DELAY").isNotNull())
        .withColumn(
            "HOUR",
            (F.col("CRS_DEP_TIME") / 100).cast(IntegerType())
        )
        .filter((F.col("HOUR") >= 0) & (F.col("HOUR") <= 23))
        .cache() #il DataFrame viene riusato per due aggregazioni distinte
    )

    # Azione che forza l'esecuzione per il calcolo dei tempi
    df_filtered.count()

    # Liberiamo la cache dal dataframe ormai inutile
    df.unpersist()

    timings["filtering_s"] = round(time.time() - t1, 3)
    print(f"    Filtering completato in {timings['filtering_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 3a. Percentili per compagnia × ora
    # ─────────────────────────────────────────────

    print("\n[3a] Calcolo percentili P25/P50/P75/P90 per compagnia e fascia oraria...")

    t2 = time.time()

    result_percentiles = (
        df_filtered
        .groupBy(
            F.col("OP_UNIQUE_CARRIER").alias("airline"),
            F.col("HOUR").alias("hour"),
        )
        .agg(
            F.count(F.lit(1)).alias("num_flights"),
            F.percentile_approx("DEP_DELAY", 0.25, PERCENTILE_ACCURACY).alias("p25"),
            F.percentile_approx("DEP_DELAY", 0.50, PERCENTILE_ACCURACY).alias("p50"),
            F.percentile_approx("DEP_DELAY", 0.75, PERCENTILE_ACCURACY).alias("p75"),
            F.percentile_approx("DEP_DELAY", 0.90, PERCENTILE_ACCURACY).alias("p90"),
        )
        .orderBy("airline", "hour")
        .cache() # serve per l'output
    )

    # Azione che forza l'esecuzione
    count_percentiles = result_percentiles.count()

    # Non facciamo df_filtered.unpersist perché serve a minmax


    timings["computation_percentiles_s"] = round(time.time() - t2, 3)
    print(f"    Calcolo percentili completato in {timings['computation_percentiles_s']:.2f}s")
    print(f"    Righe risultato: {count_percentiles}")

    # ─────────────────────────────────────────────
    # 3b. Min e max globali per compagnia
    # ─────────────────────────────────────────────

    print("\n[3b] Calcolo min/max globali per compagnia...")

    t3 = time.time()

    result_minmax = (
        df_filtered
        .groupBy(
            F.col("OP_UNIQUE_CARRIER").alias("airline"),
        )
        .agg(
            F.min("DEP_DELAY").alias("min_delay"),
            F.max("DEP_DELAY").alias("max_delay"),
        )
        .orderBy("airline")
        .cache()
    )

    # Azione che forza l'esecuzione
    count_minmax = result_minmax.count()

    # Ora df_filtered non server più quindi può essere unpersistito
    df_filtered.unpersist()

    timings["computation_minmax_s"] = round(time.time() - t3, 3)
    print(f"    Calcolo min/max completato in {timings['computation_minmax_s']:.2f}s")
    print(f"    Righe risultato: {count_minmax}")

    # ─────────────────────────────────────────────
    # 4. Anteprima (opzionale)
    # ─────────────────────────────────────────────

    if print_preview:
        print("\n[4a] Anteprima percentili (prime 30 righe):")
        result_percentiles.show(30, truncate=False)
        print("\n[4b] Anteprima min/max:")
        result_minmax.show(truncate=False)

    # ─────────────────────────────────────────────
    # 5. Output CSV (opzionale)
    #    Saltato durante il benchmark (save_output=False) per non
    #    inquinare la misurazione con latenza I/O su HDFS.
    # ─────────────────────────────────────────────

    if save_output:
        print("\n[5] Salvataggio CSV...")
        t4 = time.time()
        save_csv(result_percentiles, OUTPUT_PERCENTILES, local=True)
        save_csv(result_percentiles, OUTPUT_PERCENTILES, local=False)
        save_csv(result_minmax, OUTPUT_MINMAX, local=True)
        save_csv(result_minmax, OUTPUT_MINMAX, local=False)
        timings["output_s"] = round(time.time() - t4, 3)
        print(f"    Output completato in {timings['output_s']:.2f}s")

    # Liberiamo RAM appena possibile
    result_percentiles.unpersist()
    result_minmax.unpersist()

    timings["total_s"] = round(
        timings["loading_s"] +
        timings["filtering_s"] +
        timings["computation_percentiles_s"] +
        timings["computation_minmax_s"],
        3
    )

    return result_percentiles, result_minmax, timings


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 3: Hourly Percentiles and Global Min/Max")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query3")

    try:
        # Esecuzione standalone: save_output e print_preview entrambi True
        _, _, timings = run_query3(spark, save_output=True, print_preview=True)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 3")
        print("=" * 72)
        print(f"  Loading:               {timings['loading_s']:.3f}s")
        print(f"  Filtering + cache:     {timings['filtering_s']:.3f}s")
        print(f"  Percentili (3a):       {timings['computation_percentiles_s']:.3f}s")
        print(f"  Min/Max (3b):          {timings['computation_minmax_s']:.3f}s")
        print(f"  Output:                {timings.get('output_s', 0):.3f}s")
        print(f"  Totale (no output):    {timings['total_s']:.3f}s")
        print(f"  Totale script:         {total_elapsed:.3f}s")
        print("\n[✓] Query 3 completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()