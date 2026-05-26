"""
query3.py
─────────
Query 3 - SABD Project 1

Per le compagnie AA (American Airlines), DL (Delta), UA (United), WN (Southwest):

  [CSV 1 — query3_hourly_percentiles]
  - Ricava l'ora dal campo CRS_DEP_TIME (formato HHMM intero: 830 → 8, 1245 → 12)
  - Per ciascuna compagnia × fascia oraria (0–23):
      calcola P25, P50, P75, P90 di DEP_DELAY sui soli voli non cancellati
  - Tecnica: percentile_approx (Spark built-in, basato su Greenwald-Khanna sketch)

  [CSV 2 — query3_global_minmax]
  - Per ciascuna compagnia:
      calcola il minimo e il massimo assoluto di DEP_DELAY sull'intero dataset
      (solo voli non cancellati: DEP_DELAY è null sui cancellati comunque)

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
    HDFS_BASE,
)

# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

PARQUET_PATH = f"{HDFS_BASE}/data/processed/flights/parquet"

TARGET_AIRLINES = ["AA", "DL", "UA", "WN"]

# Accuratezza per percentile_approx:
# valori più alti → risultato più preciso ma più lento.
# 1000 è un buon compromesso per ~2.2M righe.
PERCENTILE_ACCURACY = 1000

OUTPUT_PERCENTILES = "query3_hourly_percentiles"
OUTPUT_MINMAX      = "query3_global_minmax"


# ─────────────────────────────────────────────
# Core
# ─────────────────────────────────────────────

def run_query3(spark):
    """
    Esegue la Query 3 usando DataFrame API.

    Nota metodologica — scelta della tecnica per i percentili:
        Si usa F.percentile_approx, funzione nativa di Spark basata
        sull'algoritmo Greenwald-Khanna (uno sketch per quantili approssimati).
        È l'equivalente della famiglia t-digest/KLL sketch richiesta dal testo:
        lavora in un singolo passaggio sui dati, non materializza l'intero
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

    df = spark.read.parquet(PARQUET_PATH).select(
        "OP_UNIQUE_CARRIER",
        "CRS_DEP_TIME",
        "DEP_DELAY",
        "CANCELLED",
    )

    timings["loading_s"] = time.time() - t0
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
        # DEP_DELAY nullo su voli non cancellati è un'anomalia del dataset:
        # lo escludiamo per non distorcere i percentili.
        .filter(F.col("DEP_DELAY").isNotNull())
        # Ricava l'ora dal campo HHMM intero (es. 1245 → 12, 830 → 8, 45 → 0)
        .withColumn(
            "HOUR",
            (F.col("CRS_DEP_TIME") / 100).cast(IntegerType())
        )
        # Guardia: HOUR deve essere in [0, 23]. Valori corrotti (es. 2400) vengono scartati.
        .filter((F.col("HOUR") >= 0) & (F.col("HOUR") <= 23))
    )

    # Cache: il DataFrame viene riusato per due aggregazioni distinte.
    df_filtered.cache()

    timings["filtering_s"] = time.time() - t1
    print(f"    Filtering completato in {timings['filtering_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 3a. Aggregazione percentili per compagnia × ora
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
    )

    # Materializza per misurare il tempo
    count_percentiles = result_percentiles.count()

    timings["computation_percentiles_s"] = time.time() - t2
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
    )

    count_minmax = result_minmax.count()

    timings["computation_minmax_s"] = time.time() - t3
    print(f"    Calcolo min/max completato in {timings['computation_minmax_s']:.2f}s")
    print(f"    Righe risultato: {count_minmax}")

    # ─────────────────────────────────────────────
    # 4. Output
    # ─────────────────────────────────────────────

    print("\n[4a] Anteprima percentili (prime 30 righe):")
    result_percentiles.show(30, truncate=False)

    print("\n[4b] Anteprima min/max:")
    result_minmax.show(truncate=False)

    print("\n[5] Salvataggio CSV...")

    t4 = time.time()

    save_csv(result_percentiles, OUTPUT_PERCENTILES, local=True)
    save_csv(result_percentiles, OUTPUT_PERCENTILES, local=False)

    save_csv(result_minmax, OUTPUT_MINMAX, local=True)
    save_csv(result_minmax, OUTPUT_MINMAX, local=False)

    timings["output_s"] = time.time() - t4
    print(f"    Output completato in {timings['output_s']:.2f}s")

    timings["total_s"] = sum(timings.values())

    # Libera la cache
    df_filtered.unpersist()

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
        _, _, timings = run_query3(spark)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 3")
        print("=" * 72)
        print(f"Loading:               {timings['loading_s']:.2f}s")
        print(f"Filtering + cache:     {timings['filtering_s']:.2f}s")
        print(f"Percentili (3a):       {timings['computation_percentiles_s']:.2f}s")
        print(f"Min/Max (3b):          {timings['computation_minmax_s']:.2f}s")
        print(f"Output:                {timings['output_s']:.2f}s")
        print(f"Totale fasi:           {timings['total_s']:.2f}s")
        print(f"Totale script:         {total_elapsed:.2f}s")

        print("\n[✓] Query 3 completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()