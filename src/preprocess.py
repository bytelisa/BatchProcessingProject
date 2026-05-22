"""
preprocess.py
─────────────
Converte i CSV raw caricati da NiFi su HDFS in un dataset Parquet processed.

Questo script:
- legge i CSV raw da HDFS;
- applica uno schema esplicito;
- seleziona solo le colonne utili alle query obbligatorie;
- crea colonne derivate generali;
- non fa imputazione dei valori null;
- non elimina ritardi negativi;
- non esegue aggregazioni specifiche di query;
- salva il dataset processed in Parquet.

Esecuzione:
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/scripts/preprocess.py
"""

import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from utils import (
    get_spark_session,
    HDFS_BASE,
)


# ─────────────────────────────────────────────
# Path
# ─────────────────────────────────────────────

RAW_CSV_PATH = f"{HDFS_BASE}/data/raw/flights/csv/20250*_T_ONTIME_REPORTING.csv"
PARQUET_PATH = f"{HDFS_BASE}/data/processed/flights_parquet"


# ─────────────────────────────────────────────
# Schema esplicito del CSV raw
# ─────────────────────────────────────────────

RAW_FLIGHT_SCHEMA = StructType([
    StructField("YEAR", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY_OF_MONTH", IntegerType(), True),
    StructField("OP_UNIQUE_CARRIER", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", IntegerType(), True),
    StructField("ORIGIN_AIRPORT_ID", IntegerType(), True),
    StructField("ORIGIN_CITY_MARKET_ID", IntegerType(), True),
    StructField("ORIGIN_STATE_ABR", StringType(), True),
    StructField("DEST_AIRPORT_ID", IntegerType(), True),
    StructField("DEST_CITY_MARKET_ID", IntegerType(), True),
    StructField("DEST_STATE_ABR", StringType(), True),
    StructField("CRS_DEP_TIME", IntegerType(), True),
    StructField("DEP_TIME", DoubleType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("CRS_ARR_TIME", IntegerType(), True),
    StructField("ARR_TIME", DoubleType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("CANCELLED", IntegerType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", IntegerType(), True),
    StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("CARRIER_DELAY", DoubleType(), True),
    StructField("WEATHER_DELAY", DoubleType(), True),
    StructField("NAS_DELAY", DoubleType(), True),
    StructField("SECURITY_DELAY", DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
])


# ─────────────────────────────────────────────
# Colonne processed
# ─────────────────────────────────────────────

PROCESSED_COLUMNS = [
    "YEAR",
    "MONTH",
    "DAY_OF_MONTH",
    "OP_UNIQUE_CARRIER",
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
]

DELAY_CAUSE_COLUMNS = [
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
]


def read_raw_csv(spark):
    """
    Legge i CSV raw da HDFS usando schema esplicito.
    """

    print("\n[1] Lettura CSV raw da HDFS con schema esplicito...")
    print(f"    Path input: {RAW_CSV_PATH}")

    t0 = time.time()

    df = (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .option("emptyValue", "")
        .schema(RAW_FLIGHT_SCHEMA)
        .csv(RAW_CSV_PATH)
    )

    print(f"    Lettura lazy configurata in {time.time() - t0:.1f}s")

    return df


def build_processed_dataframe(raw_df):
    """
    Costruisce il dataset processed.

    Non introduce logiche specifiche di query:
    - niente aggregazioni;
    - niente rimozione dei ritardi negativi.
    """

    print("\n[2] Selezione colonne utili...")

    df = raw_df.select(*PROCESSED_COLUMNS)

    print("\n[3] Creazione colonne derivate generali...")

    df = (
        df
        .withColumn(
            "IS_CANCELLED",
            F.col("CANCELLED") == F.lit(1),
        )
        .withColumn(
            "IS_DIVERTED",
            F.col("DIVERTED") == F.lit(1),
        )
        .withColumn(
            "DEP_HOUR",
            F.when(F.col("CRS_DEP_TIME") == F.lit(2400), F.lit(0))
             .otherwise(F.floor(F.col("CRS_DEP_TIME") / F.lit(100)))
             .cast(IntegerType()),
        )
        .withColumn(
            "IS_OFFICIAL_ARRIVAL_DELAY",
            F.col("ARR_DELAY") >= F.lit(15.0),
        )
    )

    print("\n[4] Filtro minimo su righe senza carrier...")

    # Non è un dropna globale.
    # Serve solo a evitare eventuali righe completamente corrotte.
    df = df.filter(F.col("OP_UNIQUE_CARRIER").isNotNull())

    return df


def write_processed_parquet(df):
    """
    Scrive il dataset processed in formato Parquet.
    """

    print(f"\n[5] Scrittura Parquet processed in: {PARQUET_PATH}")

    t0 = time.time()

    (
        df.write
        .mode("overwrite")
        .parquet(PARQUET_PATH)
    )

    print(f"    Scrittura completata in {time.time() - t0:.1f}s")


def verify_processed_parquet(spark):
    """
    Rilegge il Parquet e stampa controlli minimi.
    """

    print("\n[6] Verifica dataset Parquet processed...")

    df = spark.read.parquet(PARQUET_PATH)

    print("\n    Schema:")
    df.printSchema()

    print("\n    Righe totali:")
    print(f"    {df.count():,}")

    print("\n    Colonne:")
    print(f"    {df.columns}")

    print("\n    Distribuzione per mese:")
    (
        df.groupBy("MONTH")
        .count()
        .orderBy("MONTH")
        .show(truncate=False)
    )

    print("\n    Distribuzione per compagnia:")
    (
        df.groupBy("OP_UNIQUE_CARRIER")
        .count()
        .orderBy(F.desc("count"))
        .show(30, truncate=False)
    )

    print("\n    Distribuzione CANCELLED / IS_CANCELLED:")
    (
        df.groupBy("CANCELLED", "IS_CANCELLED")
        .count()
        .orderBy("CANCELLED")
        .show(truncate=False)
    )

    print("\n    Distribuzione DIVERTED / IS_DIVERTED:")
    (
        df.groupBy("DIVERTED", "IS_DIVERTED")
        .count()
        .orderBy("DIVERTED")
        .show(truncate=False)
    )

    print("\n    Distribuzione DEP_HOUR:")
    (
        df.groupBy("DEP_HOUR")
        .count()
        .orderBy("DEP_HOUR")
        .show(30, truncate=False)
    )

    print("\n    Controllo valori null sulle cause di ritardo:")
    null_exprs = [
        F.sum(F.col(c).isNull().cast("integer")).alias(f"{c}_NULLS")
        for c in DELAY_CAUSE_COLUMNS
    ]

    df.agg(*null_exprs).show(truncate=False)

    print("\n    Controllo ARR_DELAY >= 15 e cause tutte null:")

    all_causes_null = (
        F.col("CARRIER_DELAY").isNull()
        & F.col("WEATHER_DELAY").isNull()
        & F.col("NAS_DELAY").isNull()
        & F.col("SECURITY_DELAY").isNull()
        & F.col("LATE_AIRCRAFT_DELAY").isNull()
    )

    (
        df.agg(
            F.sum((F.col("ARR_DELAY") >= 15).cast("integer")).alias("ARR_DELAY_GE_15"),
            F.sum(
                ((F.col("ARR_DELAY") >= 15) & all_causes_null).cast("integer")
            ).alias("ARR_DELAY_GE_15_ALL_CAUSES_NULL"),
        )
        .show(truncate=False)
    )

    print("\n    Anteprima:")
    df.show(5, truncate=False)


def main():
    print("=" * 72)
    print("  SABD Project 1 - Preprocessing CSV raw → Parquet processed")
    print("=" * 72)

    t_start = time.time()

    spark = get_spark_session("SABD-Preprocess")

    try:
        raw_df = read_raw_csv(spark)
        processed_df = build_processed_dataframe(raw_df)

        print("\n[INFO] Conteggio righe processed prima della scrittura...")
        t_count = time.time()
        processed_count = processed_df.count()
        print(f"       Righe processed: {processed_count:,}")
        print(f"       Count completato in {time.time() - t_count:.1f}s")

        write_processed_parquet(processed_df)
        verify_processed_parquet(spark)

        total_time = time.time() - t_start
        print(f"\n[✓] Preprocessing completato in {total_time:.1f}s totali")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()