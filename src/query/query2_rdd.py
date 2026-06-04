"""
query2_rdd.py
─────────────
Query 2 - Implementazione con RDD

Per ogni compagnia, considerando solo voli:
- non cancellati: CANCELLED = 0
- non deviati:    DIVERTED = 0

calcolare:
- numero totale di voli validi
- ARR_DELAY medio
- contributo medio delle componenti di ritardo:
    CARRIER_DELAY
    WEATHER_DELAY
    NAS_DELAY
    SECURITY_DELAY
    LATE_AIRCRAFT_DELAY

Inoltre produrre una classifica delle compagnie con almeno 500 voli validi,
ordinate per ARR_DELAY medio decrescente, mostrando le prime 10 compagnie
e il contributo medio delle diverse cause di ritardo.

L'output viene scritto direttamente come RDD su HDFS e come CSV locale
tramite Python puro, senza conversione a DataFrame.

Output:
- CSV locale:
    /opt/output/query2_rdd_all_airlines_stats.csv
    /opt/output/query2_rdd_top10_arrival_delay.csv

- CSV HDFS:
    hdfs://.../data/output/flights/query2_rdd_all_airlines_stats/
    hdfs://.../data/output/flights/query2_rdd_top10_arrival_delay/
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


# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

PARQUET_PATH = HDFS_PROCESSED_PARQUET_PATH

OUTPUT_ALL_NAME = "query2_rdd_all_airlines_stats"
OUTPUT_TOP10_NAME = "query2_rdd_top10_arrival_delay"

HDFS_OUTPUT_ALL = f"{HDFS_OUT_PATH}/{OUTPUT_ALL_NAME}"
HDFS_OUTPUT_TOP10 = f"{HDFS_OUT_PATH}/{OUTPUT_TOP10_NAME}"

LOCAL_OUTPUT_ALL = f"{LOCAL_OUT_PATH}/{OUTPUT_ALL_NAME}.csv"
LOCAL_OUTPUT_TOP10 = f"{LOCAL_OUT_PATH}/{OUTPUT_TOP10_NAME}.csv"

MIN_FLIGHTS_FOR_RANKING = 500

CSV_HEADER = (
    "carrier,"
    "num_flights,"
    "arrdelay_mean,"
    "carrier_delay_mean,"
    "weather_delay_mean,"
    "nas_delay_mean,"
    "security_delay_mean,"
    "late_aircraft_delay_mean"
)


# ─────────────────────────────────────────────
# Utility salvataggio RDD
# ─────────────────────────────────────────────

def save_rdd_local(rows, filepath, header):
    """
    Scrive le righe raccolte localmente in un file CSV locale
    usando Python puro.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header.split(","))
        writer.writerows(rows)

    print(f"[✓] CSV locale salvato in: {filepath}")


def delete_hdfs_if_exists(sc, hdfs_path):
    """
    Elimina il path HDFS se esiste.
    saveAsTextFile non supporta overwrite nativo.
    """
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(hdfs_path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)

    if fs.exists(path):
        fs.delete(path, True)
        print(f"    [!] Path HDFS esistente eliminato: {hdfs_path}")


def row_to_csv_line(row):
    """
    Converte una tupla risultato in riga CSV.
    """
    return (
        f"{row[0]},"
        f"{row[1]},"
        f"{row[2]},"
        f"{row[3]},"
        f"{row[4]},"
        f"{row[5]},"
        f"{row[6]},"
        f"{row[7]}"
    )


def save_rdd_hdfs(result_rdd, hdfs_path, header):
    """
    Scrive l'RDD su HDFS come testo CSV usando saveAsTextFile.
    Prepende l'header tramite union.
    """
    sc = result_rdd.context

    delete_hdfs_if_exists(sc, hdfs_path)

    header_rdd = sc.parallelize([header])

    (
        header_rdd
        .union(result_rdd.map(row_to_csv_line))
        .coalesce(1)
        .saveAsTextFile(hdfs_path)
    )

    print(f"[✓] CSV HDFS salvato in: {hdfs_path}")


# ─────────────────────────────────────────────
# Utility valori
# ─────────────────────────────────────────────

def safe_float(value, default=0.0):
    """
    Converte un valore numerico Spark/Python a float.
    Per le componenti di ritardo, eventuali null residui vengono trattati
    come 0.0, coerentemente con il preprocessing richiesto.
    """
    if value is None:
        return default
    return float(value)


def safe_int(value, default=0):
    if value is None:
        return default
    return int(value)


# ─────────────────────────────────────────────
# Core Query 2 RDD
# ─────────────────────────────────────────────

def run_query2_rdd(spark, save_output=True, print_preview=True):
    """
    Esegue la Query 2 usando l'API RDD.

    Parametri:
      save_output   — se False salta la scrittura CSV.
      print_preview — se False salta la stampa dell'anteprima.

    Strategia:
    1. Lettura Parquet e selezione colonne minime.
    2. Conversione a RDD.
    3. Filtro voli validi: CANCELLED = 0 e DIVERTED = 0.
    4. Aggregazione per compagnia con combineByKey.
    5. Produzione di:
       - output completo per tutte le compagnie;
       - top 10 compagnie con almeno 500 voli, ordinate per ARR_DELAY medio.
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
        .select(
            "OP_UNIQUE_CARRIER",
            "CANCELLED",
            "DIVERTED",
            "ARR_DELAY",
            "CARRIER_DELAY",
            "WEATHER_DELAY",
            "NAS_DELAY",
            "SECURITY_DELAY",
            "LATE_AIRCRAFT_DELAY",
        )
        .rdd
        .map(lambda row: (
            row["OP_UNIQUE_CARRIER"],                # 0 carrier
            safe_int(row["CANCELLED"]),              # 1 cancelled
            safe_int(row["DIVERTED"]),               # 2 diverted
            safe_float(row["ARR_DELAY"]),            # 3 arr delay
            safe_float(row["CARRIER_DELAY"]),        # 4 carrier delay
            safe_float(row["WEATHER_DELAY"]),        # 5 weather delay
            safe_float(row["NAS_DELAY"]),            # 6 nas delay
            safe_float(row["SECURITY_DELAY"]),       # 7 security delay
            safe_float(row["LATE_AIRCRAFT_DELAY"]),  # 8 late aircraft delay
        ))
    )

    rdd_base.count()

    timings["loading_s"] = round(time.time() - t0, 3)
    print(f"    Loading completato in {timings['loading_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 2. Filtering
    # ─────────────────────────────────────────────

    print("\n[2] Filtro voli non cancellati e non deviati...")

    t1 = time.time()

    valid_flights = (
        rdd_base
        .filter(lambda r: r[1] == 0 and r[2] == 0)
        .cache()
    )

    valid_flights.count()

    timings["filtering_s"] = round(time.time() - t1, 3)
    print(f"    Filtering completato in {timings['filtering_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 3. Aggregazione
    # ─────────────────────────────────────────────

    print("\n[3] Aggregazione per compagnia con RDD...")

    t2 = time.time()

    def create_combiner(v):
        # v = (arr, carrier, weather, nas, security, late)
        return (
            1,      # count
            v[0],   # arr delay sum
            v[1],   # carrier delay sum
            v[2],   # weather delay sum
            v[3],   # nas delay sum
            v[4],   # security delay sum
            v[5],   # late aircraft delay sum
        )

    def merge_value(acc, v):
        return (
            acc[0] + 1,
            acc[1] + v[0],
            acc[2] + v[1],
            acc[3] + v[2],
            acc[4] + v[3],
            acc[5] + v[4],
            acc[6] + v[5],
        )

    def merge_combiners(a, b):
        return (
            a[0] + b[0],
            a[1] + b[1],
            a[2] + b[2],
            a[3] + b[3],
            a[4] + b[4],
            a[5] + b[5],
            a[6] + b[6],
        )

    def to_result_row(kv):
        carrier = kv[0]
        acc = kv[1]

        count = acc[0]

        return (
            carrier,
            count,
            round(acc[1] / count, 4),  # arrdelay_mean
            round(acc[2] / count, 4),  # carrier_delay_mean
            round(acc[3] / count, 4),  # weather_delay_mean
            round(acc[4] / count, 4),  # nas_delay_mean
            round(acc[5] / count, 4),  # security_delay_mean
            round(acc[6] / count, 4),  # late_aircraft_delay_mean
        )

    all_airlines_rdd = (
        valid_flights
        .map(lambda r: (
            r[0],
            (
                r[3],  # ARR_DELAY
                r[4],  # CARRIER_DELAY
                r[5],  # WEATHER_DELAY
                r[6],  # NAS_DELAY
                r[7],  # SECURITY_DELAY
                r[8],  # LATE_AIRCRAFT_DELAY
            )
        ))
        .combineByKey(create_combiner, merge_value, merge_combiners)
        .map(to_result_row)
        .sortBy(lambda r: r[0])
        .cache()
    )

    top10_rdd = (
        all_airlines_rdd
        .filter(lambda r: r[1] >= MIN_FLIGHTS_FOR_RANKING)
        .sortBy(lambda r: -r[2])
        .zipWithIndex()
        .filter(lambda kv: kv[1] < 10)
        .map(lambda kv: kv[0])
        .cache()
    )

    all_rows = all_airlines_rdd.collect()
    top10_rows = top10_rdd.collect()

    timings["computation_s"] = round(time.time() - t2, 3)

    print(f"    Aggregazione completata in {timings['computation_s']:.2f}s")
    print(f"    Compagnie totali: {len(all_rows)}")
    print(f"    Compagnie in top 10: {len(top10_rows)}")

    # ─────────────────────────────────────────────
    # 4. Anteprima
    # ─────────────────────────────────────────────

    if print_preview:
        print("\n[4] Anteprima output completo:")
        print(
            f"  {'carrier':8s} {'flights':10s} {'arr_mean':10s} "
            f"{'carrier':10s} {'weather':10s} {'nas':10s} "
            f"{'security':10s} {'late_air':10s}"
        )
        print("  " + "-" * 90)

        for r in all_rows[:20]:
            print(
                f"  {r[0]:8s} {r[1]:10d} {r[2]:10.4f} "
                f"{r[3]:10.4f} {r[4]:10.4f} {r[5]:10.4f} "
                f"{r[6]:10.4f} {r[7]:10.4f}"
            )

        print("\n[4b] Top 10 compagnie per ARR_DELAY medio:")
        print(
            f"  {'rank':4s} {'carrier':8s} {'flights':10s} {'arr_mean':10s} "
            f"{'carrier':10s} {'weather':10s} {'nas':10s} "
            f"{'security':10s} {'late_air':10s}"
        )
        print("  " + "-" * 100)

        for i, r in enumerate(top10_rows, start=1):
            print(
                f"  {i:<4d} {r[0]:8s} {r[1]:10d} {r[2]:10.4f} "
                f"{r[3]:10.4f} {r[4]:10.4f} {r[5]:10.4f} "
                f"{r[6]:10.4f} {r[7]:10.4f}"
            )

    # ─────────────────────────────────────────────
    # 5. Output CSV
    # ─────────────────────────────────────────────

    if save_output:
        print("\n[5] Salvataggio CSV (RDD puro)...")

        t3 = time.time()

        save_rdd_local(all_rows, LOCAL_OUTPUT_ALL, CSV_HEADER)
        save_rdd_local(top10_rows, LOCAL_OUTPUT_TOP10, CSV_HEADER)

        save_rdd_hdfs(all_airlines_rdd, HDFS_OUTPUT_ALL, CSV_HEADER)
        save_rdd_hdfs(top10_rdd, HDFS_OUTPUT_TOP10, CSV_HEADER)

        timings["output_s"] = round(time.time() - t3, 3)

        print(f"    Output completato in {timings['output_s']:.2f}s")

    valid_flights.unpersist()
    all_airlines_rdd.unpersist()
    top10_rdd.unpersist()

    timings["total_s"] = round(
        timings["loading_s"] + timings["filtering_s"] + timings["computation_s"], 3
    )

    return all_rows, top10_rows, timings


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 2 RDD: Arrival Delay Ranking")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query2-RDD")

    try:
        _, timings = run_query2_rdd(
            spark,
            save_output=True,
            print_preview=True,
        )

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 2 (RDD)")
        print("=" * 72)
        print(f"  Loading:      {timings['loading_s']:.3f}s")
        print(f"  Filtering:    {timings['filtering_s']:.3f}s")
        print(f"  Computation:  {timings['computation_s']:.3f}s")
        print(f"  Output:       {timings.get('output_s', 0):.3f}s")
        print(f"  Totale:       {total_elapsed:.3f}s")
        print("\n[✓] Query 2 RDD completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()