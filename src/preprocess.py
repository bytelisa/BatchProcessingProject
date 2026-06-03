"""
preprocess.py
─────────────
Preprocessing generale

Trasforma i CSV raw caricati su HDFS in un dataset Parquet processed.

Lo script:
- legge i CSV raw da HDFS;
- applica schema esplicito;
- seleziona solo le colonne originali utili alle query obbligatorie;
- corregge i tipi delle colonne principali;
- non crea colonne derivate;
- imputa a 0 i valori null delle componenti di ritardo;
- non elimina ritardi negativi;
- non aggrega;
- non esegue quality check completo;
- scrive un unico dataset Parquet logico, lasciando a Spark la gestione dei part-file.

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

""" 
    Importiamo i path dal file di configurazione config.py
"""

from config import (
    HDFS_RAW_CSV_PATH,
    HDFS_PROCESSED_PARQUET_PATH,
)

RAW_CSV_PATH = HDFS_RAW_CSV_PATH
PARQUET_PATH = HDFS_PROCESSED_PARQUET_PATH


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

    # Letti come double perché nel CSV possono comparire come 0.00 / 1.00.
    # Verranno poi convertiti a integer nel dataset processed.
    StructField("CANCELLED", DoubleType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", DoubleType(), True),

    StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("CARRIER_DELAY", DoubleType(), True),
    StructField("WEATHER_DELAY", DoubleType(), True),
    StructField("NAS_DELAY", DoubleType(), True),
    StructField("SECURITY_DELAY", DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
])


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
    print("\n[1] Reading raw CSV from HDFS")
    print(f"    Input: {RAW_CSV_PATH}")

    return (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .option("emptyValue", "")
        .schema(RAW_FLIGHT_SCHEMA)
        .csv(RAW_CSV_PATH)
    )


def build_processed_dataframe(raw_df):
    print("\n[2] Building processed dataframe")

    df = (
        raw_df
        .select(*PROCESSED_COLUMNS)
        .withColumn("CANCELLED", F.col("CANCELLED").cast("integer"))
        .withColumn("DIVERTED", F.col("DIVERTED").cast("integer"))
        .filter(F.col("OP_UNIQUE_CARRIER").isNotNull())
    )

    # Per Q2: i valori mancanti nelle componenti di ritardo vengono
    # interpretati come assenza di contributo della relativa causa.
    
    df = df.fillna(0.0, subset=DELAY_CAUSE_COLUMNS)

    return df


def write_parquet(df):
    print("\n[3] Writing processed Parquet")
    print(f"    Output: {PARQUET_PATH}")
    print("    Output layout: Spark-managed Parquet part-files")

    (
        df.write
        .mode("overwrite")
        .parquet(PARQUET_PATH)
    )


def main():
    print("=" * 72)
    print("  SABD Project 1 - Preprocessing CSV raw → Parquet processed")
    print("=" * 72)

    start = time.time()

    spark = get_spark_session("SABD-Preprocess")

    try:
        raw_df = read_raw_csv(spark)
        processed_df = build_processed_dataframe(raw_df)

        print("\n[INFO] Materializing processed dataframe")
        row_count = processed_df.count()
        print(f"       Rows: {row_count:,}")

        write_parquet(processed_df)

        elapsed = time.time() - start
        print(f"\n[✓] Preprocessing completed in {elapsed:.1f}s")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()