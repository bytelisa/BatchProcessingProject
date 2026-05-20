"""
preprocess.py
─────────────
Converte i CSV grezzi caricati da NiFi su HDFS in formato Parquet.
Va eseguito UNA SOLA VOLTA prima di lanciare le query.

Esecuzione:
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/scripts/preprocess.py
"""

import time
from utils import (
    get_spark_session,
    load_flights,
    add_hour_column,
    HDFS_BASE,
    FLIGHT_SCHEMA,
)
from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# Path di output Parquet su HDFS
# ─────────────────────────────────────────────
PARQUET_PATH = f"{HDFS_BASE}/data/processed/flights/parquet"


def convert_csv_to_parquet(spark):
    """
    Legge i CSV grezzi da HDFS, applica pulizia e salva in Parquet.
    """
    print("\n[1] Lettura CSV da HDFS...")
    t0 = time.time()
    df = load_flights(spark)
    print(f"    Schema caricato in {time.time() - t0:.1f}s")

    # ── Pulizia ──────────────────────────────
    print("\n[2] Pulizia dati...")

    # Seleziona solo le colonne utili al progetto (scarta tutto il resto)
    df = df.select(
        "YEAR",
        "MONTH",
        "DAY_OF_MONTH",
        "OP_UNIQUE_CARRIER",
        "ORIGIN_AIRPORT_ID",
        "DEST_AIRPORT_ID",
        "CRS_DEP_TIME",
        "DEP_DELAY",
        "ARR_DELAY",
        "CANCELLED",
        "DIVERTED",
        "CARRIER_DELAY",
        "WEATHER_DELAY",
        "NAS_DELAY",
        "SECURITY_DELAY",
        "LATE_AIRCRAFT_DELAY",
    )

    # Aggiunge colonna HOUR ricavata da CRS_DEP_TIME (es. 830 → 8)
    # Serve per la Query 3 (aggregazione per fascia oraria)
    df = add_hour_column(df)

    # Normalizza CANCELLED e DIVERTED: assicura che siano 0 o 1
    df = df.withColumn("CANCELLED", F.col("CANCELLED").cast("integer")) \
           .withColumn("DIVERTED",  F.col("DIVERTED").cast("integer"))

    # Rimuove righe senza carrier (righe completamente vuote/corrotte)
    df = df.filter(F.col("OP_UNIQUE_CARRIER").isNotNull())

    print(f"    Righe dopo pulizia: {df.count():,}")

    # ── Salvataggio Parquet ───────────────────
    print(f"\n[3] Salvataggio Parquet in: {PARQUET_PATH}")
    t1 = time.time()

    df.write \
      .mode("overwrite") \
      .parquet(PARQUET_PATH)

    elapsed = time.time() - t1
    print(f"    Parquet salvato in {elapsed:.1f}s")

    return df


def verify_parquet(spark):
    """
    Rilegge il Parquet appena creato e stampa statistiche di verifica.
    """
    print(f"\n[4] Verifica Parquet...")
    df = spark.read.parquet(PARQUET_PATH)

    print(f"    Righe totali:  {df.count():,}")
    print(f"    Colonne:       {df.columns}")

    print("\n    Distribuzione per mese:")
    df.groupBy("MONTH").count().orderBy("MONTH").show()

    print("\n    Distribuzione per compagnia (top 10):")
    df.groupBy("OP_UNIQUE_CARRIER") \
      .count() \
      .orderBy(F.desc("count")) \
      .show(10)

    print("\n    Anteprima prime 3 righe:")
    df.show(3, truncate=False)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  SABD Project 1 - Preprocessing CSV → Parquet")
    print("=" * 55)

    t_start = time.time()

    spark = get_spark_session("SABD-Preprocess")

    convert_csv_to_parquet(spark)
    verify_parquet(spark)

    total = time.time() - t_start
    print(f"\n[✓] Preprocessing completato in {total:.1f}s totali")

    spark.stop()