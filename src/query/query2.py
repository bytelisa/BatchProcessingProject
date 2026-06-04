"""
query2.py
─────────
Query 2 - Implementazione con Dataframe

Output prodotti:

1) query2_all_airlines_stats
   Statistiche aggregate per ogni compagnia:
   - numero di voli non cancellati e non deviati
   - ARR_DELAY medio
   - media delle componenti di ritardo

2) query2_top10_arrival_delay
   Classifica delle prime 10 compagnie, tra quelle con almeno 500 voli validi,
   ordinate per ARR_DELAY medio decrescente.

Nota metodologica:
I valori null delle componenti di ritardo devono essere già imputati a 0
nel preprocessing. In questo modo un valore mancante viene interpretato come
assenza di contributo della relativa causa di ritardo.
"""

import time

from pyspark.sql import functions as F

from utils import (
    get_spark_session,
    save_csv,
)

from config import HDFS_PROCESSED_PARQUET_PATH

PARQUET_PATH = HDFS_PROCESSED_PARQUET_PATH


# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

OUTPUT_ALL_AIRLINES = "query2_all_airlines_stats"
OUTPUT_TOP10 = "query2_top10_arrival_delay"

MIN_VALID_FLIGHTS = 500

DELAY_CAUSE_COLUMNS = [
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
]


def build_all_airlines_stats(df):
    """
    Costruisce il primo output della Q2:
    statistiche aggregate per ogni compagnia, ricevendo solo voli:
    - non cancellati
    - non deviati
    - con ARR_DELAY non nullo
    """

    all_airlines_stats = (
        df
        .groupBy(F.col("OP_UNIQUE_CARRIER").alias("carrier"))
        .agg(
            F.count(F.lit(1)).alias("num_flights"),
            F.avg("ARR_DELAY").alias("arrdelay_mean"),
            F.avg("CARRIER_DELAY").alias("carrier_delay_mean"),
            F.avg("WEATHER_DELAY").alias("weather_delay_mean"),
            F.avg("NAS_DELAY").alias("nas_delay_mean"),
            F.avg("SECURITY_DELAY").alias("security_delay_mean"),
            F.avg("LATE_AIRCRAFT_DELAY").alias("late_aircraft_delay_mean"),
        )
        .select(
            "carrier",
            "num_flights",
            F.round("arrdelay_mean", 4).alias("arrdelay_mean"),
            F.round("carrier_delay_mean", 4).alias("carrier_delay_mean"),
            F.round("weather_delay_mean", 4).alias("weather_delay_mean"),
            F.round("nas_delay_mean", 4).alias("nas_delay_mean"),
            F.round("security_delay_mean", 4).alias("security_delay_mean"),
            F.round("late_aircraft_delay_mean", 4).alias("late_aircraft_delay_mean"),
        )
        .orderBy("carrier")
    )

    return all_airlines_stats


def build_top10_arrival_delay(all_airlines_stats):
    """
    Costruisce il secondo output della Q2:
    top 10 compagnie con almeno 500 voli validi, ordinate per ARR_DELAY medio
    decrescente.
    """

    # Include filtering in quanto è una richiesta specfica della seconda parte della query

    top10_arrival_delay = (
        all_airlines_stats
        .filter(F.col("num_flights") >= MIN_VALID_FLIGHTS)
        .orderBy(F.col("arrdelay_mean").desc())
        .limit(10)
    )

    return top10_arrival_delay


def run_query2(spark, save_output=True, print_preview=True):
    """
    Esegue la Query 2 usando DataFrame API.

    Parametri:
      save_output:
        se True salva entrambi gli output CSV.
        se False non salva gli output, utile per benchmark.

      print_preview:
        se True mostra a console l'anteprima dei due risultati.
        se False evita le show(), utile per benchmark.

    Ritorna:
      all_airlines_stats, top10_arrival_delay, timings
    """

    timings = {}

    # ─────────────────────────────────────────────
    # 1. Loading
    # ─────────────────────────────────────────────

    print("\n[1] Lettura dataset processed Parquet...")
    print(f"    Input path: {PARQUET_PATH}")

    t0 = time.time()

    df = (
        spark.read
        .parquet(PARQUET_PATH)
        .select(
            "OP_UNIQUE_CARRIER",
            "ARR_DELAY",
            "CANCELLED",
            "DIVERTED",
            *DELAY_CAUSE_COLUMNS,
        )
    )

    # Azione che forza l'esecuzione
    df.count()

    timings["loading_s"] = round(time.time() - t0, 3)
    print(f"    Loading completato in {timings['loading_s']:.3f}s")

    # ─────────────────────────────────────────────
    # 2. Filterin/Preprocessing
    # ─────────────────────────────────────────────
    t1 = time.time()

    df_filtered= (
        df
        .filter((F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 0))
        .filter(F.col("ARR_DELAY").isNotNull())
    )

    # Azione che forza l'esecuzione
    df_filtered.count()

    timings["filtering_s"] = round(time.time() - t1, 3)
    print(f"    Filtering completato in {timings['filtering_s']:.3f}s")

    # ─────────────────────────────────────────────
    # 2. Aggregazione per tutte le compagnie
    # ─────────────────────────────────────────────

    print("\n[2] Calcolo statistiche per tutte le compagnie...")

    t2 = time.time()

    all_airlines_stats = build_all_airlines_stats(df_filtered)

    # Cache qui utile perché questo dataframe viene:
    # - contato
    # - mostrato
    # - usato per costruire la top 10
    # - eventualmente salvato
    all_airlines_stats = all_airlines_stats.cache()

    # Azione che forza l'esecuzione
    all_count = all_airlines_stats.count()

    timings["all_airlines_computation_s"] = round(time.time() - t2, 3)

    print(f"    Aggregazione completata in {timings['all_airlines_computation_s']:.3f}s")
    print(f"    Compagnie aggregate: {all_count}")

    # ─────────────────────────────────────────────
    # 3. Top 10 compagnie per ARR_DELAY medio
    # ─────────────────────────────────────────────

    print("\n[3] Calcolo top 10 compagnie per ARR_DELAY medio...")

    t3 = time.time()

    # Caching perché poi il risultato è riutilizzato nella fase di output
    top10_arrival_delay = build_top10_arrival_delay(all_airlines_stats).cache()

    # Materializzo per misurare il tempo di calcolo della top 10
    top10_count = top10_arrival_delay.count()

    timings["top10_computation_s"] = round(time.time() - t3, 3)

    print(f"    Top 10 calcolata in {timings['top10_computation_s']:.3f}s")
    print(f"    Righe top 10: {top10_count}")

    # ─────────────────────────────────────────────
    # 4. Anteprime
    # ─────────────────────────────────────────────

    if print_preview:
        print("\n[4.1] Anteprima statistiche per tutte le compagnie:")
        all_airlines_stats.show(50, truncate=False)

        print("\n[4.2] Anteprima top 10 compagnie per ARR_DELAY medio:")
        top10_arrival_delay.show(10, truncate=False)

    # ─────────────────────────────────────────────
    # 5. Salvataggio CSV
    # ─────────────────────────────────────────────

    if save_output:
        print("\n[5] Salvataggio output CSV...")

        t3 = time.time()

        # Output 1: tutte le compagnie
        save_csv(all_airlines_stats, OUTPUT_ALL_AIRLINES, local=True)
        save_csv(all_airlines_stats, OUTPUT_ALL_AIRLINES, local=False)

        # Output 2: top 10
        save_csv(top10_arrival_delay, OUTPUT_TOP10, local=True)
        save_csv(top10_arrival_delay, OUTPUT_TOP10, local=False)

        timings["output_s"] = round(time.time() - t3, 3)
        print(f"    Output completato in {timings['output_s']:.3f}s")

    timings["total_s"] = round(
        timings["loading_s"]
        + timings["filtering_s"]
        + timings["all_airlines_computation_s"]
        + timings["top10_computation_s"],
        3,
    )

    return all_airlines_stats, top10_arrival_delay, timings


def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 2")
    print("  Airline Arrival Delay and Delay Causes Analysis")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query2")

    try:
        _, _, timings = run_query2(
            spark,
            save_output=True,
            print_preview=True,
        )

        wall_total_s = round(time.time() - total_start, 3)

        print("\n" + "=" * 72)
        print("TEMPI QUERY 2")
        print("=" * 72)
        print(f"  Loading:                    {timings['loading_s']:.3f}s")
        print(f"  Filtering:                  {timings['filtering_s']:.3f}s")
        print(f"  All airlines computation:   {timings['all_airlines_computation_s']:.3f}s")
        print(f"  Top 10 computation:         {timings['top10_computation_s']:.3f}s")
        print(f"  Output:                     {timings.get('output_s', 0):.3f}s")
        print(f"  Total without output:       {timings['total_s']:.3f}s")
        print(f"  Wall total:                 {wall_total_s:.3f}s")
        print("=" * 72)

        print("\n[✓] Query 2 completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()