from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from utils import get_spark_session, HDFS_BASE


PARQUET_PATH = f"{HDFS_BASE}/data/processed/flights/parquet"


def main():
    spark = get_spark_session("Check-WN-Night-Flights")

    df = spark.read.parquet(PARQUET_PATH).select(
        "OP_UNIQUE_CARRIER",
        "CRS_DEP_TIME",
        "DEP_DELAY",
        "CANCELLED",
        "DIVERTED",
    )

    df = df.withColumn(
        "HOUR",
        (F.col("CRS_DEP_TIME") / 100).cast(IntegerType())
    )

    wn = df.filter(F.col("OP_UNIQUE_CARRIER") == "WN")

    print("\n[1] WN - distribuzione completa per ora, tutti i voli")
    (
        wn.groupBy("HOUR")
        .agg(
            F.count("*").alias("total_flights"),
            F.sum((F.col("CANCELLED") == 1).cast("int")).alias("cancelled"),
            F.sum((F.col("CANCELLED") == 0).cast("int")).alias("not_cancelled"),
            F.sum(F.col("DEP_DELAY").isNull().cast("int")).alias("dep_delay_null"),
        )
        .orderBy("HOUR")
        .show(30, truncate=False)
    )

    print("\n[2] WN - solo ore 0-4, tutti i voli")
    (
        wn.filter((F.col("HOUR") >= 0) & (F.col("HOUR") <= 4))
        .orderBy("CRS_DEP_TIME")
        .show(100, truncate=False)
    )

    print("\n[3] WN - solo ore 0-4, voli non cancellati")
    (
        wn.filter((F.col("HOUR") >= 0) & (F.col("HOUR") <= 4))
        .filter(F.col("CANCELLED") == 0)
        .orderBy("CRS_DEP_TIME")
        .show(100, truncate=False)
    )

    print("\n[4] WN - solo ore 0-4, voli non cancellati e DEP_DELAY valorizzato")
    (
        wn.filter((F.col("HOUR") >= 0) & (F.col("HOUR") <= 4))
        .filter(F.col("CANCELLED") == 0)
        .filter(F.col("DEP_DELAY").isNotNull())
        .orderBy("CRS_DEP_TIME")
        .show(100, truncate=False)
    )

    print("\n[5] WN - min/max CRS_DEP_TIME")
    wn.agg(
        F.min("CRS_DEP_TIME").alias("min_crs_dep_time"),
        F.max("CRS_DEP_TIME").alias("max_crs_dep_time"),
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()