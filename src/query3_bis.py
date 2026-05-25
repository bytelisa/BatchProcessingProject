"""
query3_bis.py
─────────────
Query 3 (variante t-digest) - SABD Project 1

Stessa query di query3.py, ma il calcolo dei percentili usa la libreria
t-digest invece di F.percentile_approx.

Tecnica:
    t-digest è uno sketch probabilistico per la stima di quantili proposto
    da Dunning (2021). Mantiene un insieme compatto di "centroidi" che
    approssimano la distribuzione cumulativa dei dati. La precisione è
    adattiva: è massima vicino alle code (P5, P95) e leggermente inferiore
    al centro (P50), esattamente al contrario di Greenwald-Khanna usato
    da percentile_approx.

    In PySpark non esiste una funzione t-digest nativa, quindi si usa
    applyInPandas: per ogni gruppo (airline, hour) Spark raccoglie le
    righe su un executor, le converte in un pandas DataFrame, e applica
    la UDF Python che costruisce il t-digest e calcola i percentili.

    Dipendenza da installare su tutti i nodi del cluster:
        pip install tdigest

Output (stessi file di query3.py, con suffisso _tdigest):
    - /opt/results/query3_bis_hourly_percentiles.csv
    - /opt/results/query3_bis_global_minmax.csv
    - HDFS: /data/processed/flights/query3_bis_hourly_percentiles
    - HDFS: /data/processed/flights/query3_bis_global_minmax
"""

import time

import pandas as pd
from tdigest import TDigest

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from utils import (
    HDFS_BASE,
    get_spark_session,
    save_csv,
)

# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

PARQUET_PATH = f"{HDFS_BASE}/data/processed/flights/parquet"

TARGET_AIRLINES = ["AA", "DL", "UA", "WN"]

# Parametro delta del t-digest (libreria tdigest 0.5.x).
# Controlla la precisione dello sketch: valori più piccoli → più preciso
# ma sketch più grande in memoria.
# 0.01 è il default della libreria ed è già molto accurato
# (errore tipico < 0.1% sui quantili centrali).
TDIGEST_DELTA = 0.01

OUTPUT_PERCENTILES = "query3_bis_hourly_percentiles"
OUTPUT_MINMAX      = "query3_bis_global_minmax"

# Schema che applyInPandas deve restituire per ogni gruppo (airline, hour)
PERCENTILE_SCHEMA = StructType([
    StructField("airline",     StringType(),  False),
    StructField("hour",        IntegerType(), False),
    StructField("num_flights", LongType(),    False),
    StructField("p25",         DoubleType(),  True),
    StructField("p50",         DoubleType(),  True),
    StructField("p75",         DoubleType(),  True),
    StructField("p90",         DoubleType(),  True),
])


# ─────────────────────────────────────────────
# UDF t-digest
# ─────────────────────────────────────────────

def tdigest_percentiles(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Funzione applicata da applyInPandas a ogni gruppo (airline, hour).

    Riceve un pandas DataFrame con le colonne:
        OP_UNIQUE_CARRIER, HOUR, DEP_DELAY

    Restituisce un pandas DataFrame con una sola riga:
        airline, hour, num_flights, p25, p50, p75, p90

    Il t-digest viene costruito incrementalmente su tutti i valori
    DEP_DELAY del gruppo, poi si interroga per i 4 quantili.
    """

    # Recupera i metadati del gruppo dalle prime riga
    airline = pdf["OP_UNIQUE_CARRIER"].iloc[0]
    hour    = int(pdf["HOUR"].iloc[0])

    delays = pdf["DEP_DELAY"].dropna().tolist()
    n = len(delays)

    if n == 0:
        # Gruppo vuoto (non dovrebbe accadere dopo il filtro, ma per sicurezza)
        return pd.DataFrame([{
            "airline":     airline,
            "hour":        hour,
            "num_flights": 0,
            "p25":         None,
            "p50":         None,
            "p75":         None,
            "p90":         None,
        }])

    # Costruisce il t-digest e alimenta tutti i valori del gruppo
    td = TDigest(delta=TDIGEST_DELTA)
    for v in delays:
        td.update(v)

    return pd.DataFrame([{
        "airline":     airline,
        "hour":        hour,
        "num_flights": n,
        "p25":         td.percentile(25),
        "p50":         td.percentile(50),
        "p75":         td.percentile(75),
        "p90":         td.percentile(90),
    }])


# ─────────────────────────────────────────────
# Core
# ─────────────────────────────────────────────

def run_query3_bis(spark):
    """
    Esegue la Query 3 con t-digest.

    Differenze rispetto a query3.py:
      - Il calcolo dei percentili avviene tramite applyInPandas + TDigest
        invece di F.percentile_approx.
      - La precisione di t-digest è adattiva: migliore alle code,
        leggermente peggiore al centro rispetto a Greenwald-Khanna.
      - Il parametro delta controlla la precisione (default 0.01).
      - Il min/max globale usa ancora le funzioni native di Spark
        (F.min / F.max), che sono esatte — non ha senso approssimarle.
    """

    timings = {}

    # ─────────────────────────────────────────────
    # 1. Loading
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
        .filter(F.col("DEP_DELAY").isNotNull())
        .withColumn(
            "HOUR",
            (F.col("CRS_DEP_TIME") / 100).cast(IntegerType())
        )
        .filter((F.col("HOUR") >= 0) & (F.col("HOUR") <= 23))
    )

    # Cache: il DataFrame viene riusato per applyInPandas e per min/max.
    df_filtered.cache()

    timings["filtering_s"] = time.time() - t1
    print(f"    Filtering completato in {timings['filtering_s']:.2f}s")

    # ─────────────────────────────────────────────
    # 3a. Percentili con t-digest via applyInPandas
    # ─────────────────────────────────────────────

    print("\n[3a] Calcolo percentili P25/P50/P75/P90 con t-digest...")
    print(f"     delta = {TDIGEST_DELTA}")

    t2 = time.time()

    # applyInPandas richiede che il groupBy contenga esattamente
    # le colonne su cui si raggruppa; la UDF riceve un pandas DataFrame
    # per ogni gruppo e deve restituire un pandas DataFrame con lo schema
    # dichiarato in PERCENTILE_SCHEMA.
    result_percentiles = (
        df_filtered
        .select("OP_UNIQUE_CARRIER", "HOUR", "DEP_DELAY")
        .groupBy("OP_UNIQUE_CARRIER", "HOUR")
        .applyInPandas(tdigest_percentiles, schema=PERCENTILE_SCHEMA)
        .orderBy("airline", "hour")
    )

    count_percentiles = result_percentiles.count()

    timings["computation_percentiles_s"] = time.time() - t2
    print(f"    Calcolo percentili completato in {timings['computation_percentiles_s']:.2f}s")
    print(f"    Righe risultato: {count_percentiles}")

    # ─────────────────────────────────────────────
    # 3b. Min e max globali — esatti, con funzioni native Spark
    # ─────────────────────────────────────────────

    print("\n[3b] Calcolo min/max globali per compagnia...")

    t3 = time.time()

    result_minmax = (
        df_filtered
        .groupBy(F.col("OP_UNIQUE_CARRIER").alias("airline"))
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

    df_filtered.unpersist()

    return result_percentiles, result_minmax, timings


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  SABD Project 1 - Query 3 BIS: t-digest Percentiles")
    print("=" * 72)

    total_start = time.time()

    spark = get_spark_session("SABD-Query3-TDigest")

    # applyInPandas richiede Arrow per la serializzazione pandas ↔ Spark.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    try:
        _, _, timings = run_query3_bis(spark)

        total_elapsed = time.time() - total_start

        print("\n" + "=" * 72)
        print("TEMPI QUERY 3 BIS (t-digest)")
        print("=" * 72)
        print(f"Loading:               {timings['loading_s']:.2f}s")
        print(f"Filtering + cache:     {timings['filtering_s']:.2f}s")
        print(f"Percentili t-digest (delta={TDIGEST_DELTA}): {timings['computation_percentiles_s']:.2f}s")
        print(f"Min/Max (esatti):      {timings['computation_minmax_s']:.2f}s")
        print(f"Output:                {timings['output_s']:.2f}s")
        print(f"Totale fasi:           {timings['total_s']:.2f}s")
        print(f"Totale script:         {total_elapsed:.2f}s")

        print("\n[✓] Query 3 BIS completata correttamente")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()