from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType
)
from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────

HDFS_BASE       = "hdfs://namenode:9000"
HDFS_CSV_PATH   = f"{HDFS_BASE}/data/raw/flights/csv/20250*_T_ONTIME_REPORTING.csv"
HDFS_OUT_PATH   = f"{HDFS_BASE}/data/processed/flights"
LOCAL_OUT_PATH  = "/opt/results"   # montato su ./results sul tuo PC

# ─────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────

def get_spark_session(app_name: str) -> SparkSession:
    """
    Crea (o recupera) una SparkSession connessa al cluster Docker.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_BASE)
        # permette al driver di leggere da HDFS
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        # evita warning inutili
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ─────────────────────────────────────────────
# Schema esplicito (più veloce di inferSchema)
# ─────────────────────────────────────────────

FLIGHT_SCHEMA = None  # non serve più

# ─────────────────────────────────────────────
# Caricamento dati
# ─────────────────────────────────────────────

def load_flights(spark):
    """
    Carica i CSV usando i nomi delle colonne dall'header.
    Più robusto dello schema fisso quando ci sono colonne extra.
    """
    df = (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .csv(HDFS_CSV_PATH)
    )

    # Casta solo le colonne che servono al progetto
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, FloatType

    df = df \
        .withColumn("YEAR",                F.col("YEAR").cast(IntegerType())) \
        .withColumn("MONTH",               F.col("MONTH").cast(IntegerType())) \
        .withColumn("DAY_OF_MONTH",        F.col("DAY_OF_MONTH").cast(IntegerType())) \
        .withColumn("CRS_DEP_TIME",        F.col("CRS_DEP_TIME").cast(IntegerType())) \
        .withColumn("DEP_DELAY",           F.col("DEP_DELAY").cast(FloatType())) \
        .withColumn("ARR_DELAY",           F.col("ARR_DELAY").cast(FloatType())) \
        .withColumn("CANCELLED",           F.col("CANCELLED").cast(FloatType())) \
        .withColumn("DIVERTED",            F.col("DIVERTED").cast(FloatType())) \
        .withColumn("CARRIER_DELAY",       F.col("CARRIER_DELAY").cast(FloatType())) \
        .withColumn("WEATHER_DELAY",       F.col("WEATHER_DELAY").cast(FloatType())) \
        .withColumn("NAS_DELAY",           F.col("NAS_DELAY").cast(FloatType())) \
        .withColumn("SECURITY_DELAY",      F.col("SECURITY_DELAY").cast(FloatType())) \
        .withColumn("LATE_AIRCRAFT_DELAY", F.col("LATE_AIRCRAFT_DELAY").cast(FloatType()))

    return df

def add_hour_column(df):
    """
    Ricava l'ora del giorno dal campo CRS_DEP_TIME (formato HHMM intero).
    Es: 830 → 8, 1245 → 12, 0 → 0
    """
    return df.withColumn("HOUR", (F.col("CRS_DEP_TIME") / 100).cast(IntegerType()))

# ─────────────────────────────────────────────
# Salvataggio risultati
# ─────────────────────────────────────────────

def save_csv(df, filename: str, local: bool = True):
    """
    Salva il DataFrame come CSV.

    - local=True:
        salva un singolo file CSV locale in /opt/results/<filename>.csv
        usando Python standard, non Spark writer.
        Questo evita problemi di chmod sui bind mount Windows/WSL.

    - local=False:
        salva su HDFS in /data/processed/flights/<filename>
        usando Spark writer.

    Nota:
    local=True è pensato per risultati aggregati piccoli, come Q1/Q2/Q3.
    """
    import csv
    import os

    if local:
        os.makedirs(LOCAL_OUT_PATH, exist_ok=True)

        path = f"{LOCAL_OUT_PATH}/{filename}.csv"

        rows = df.collect()
        columns = df.columns

        with open(path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)

            for row in rows:
                writer.writerow([row[col] for col in columns])

        print(f"[✓] Risultato locale salvato in: {path}")

    else:
        path = f"{HDFS_OUT_PATH}/{filename}"

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(path)
        )

        print(f"[✓] Risultato HDFS salvato in: {path}")

# ─────────────────────────────────────────────
# Verifica setup (esegui questo file direttamente)
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  SABD Project 1 - Verifica setup Spark")
    print("=" * 50)

    spark = get_spark_session("SABD-Setup-Check")

    print(f"\n[1] SparkSession creata: {spark.version}")
    print(f"    Master: {spark.sparkContext.master}")

    print("\n[2] Caricamento dati da HDFS...")
    df = load_flights(spark)

    total = df.count()
    print(f"    Righe totali: {total:,}")

    print("\n[3] Schema del DataFrame:")
    df.printSchema()

    print("\n[4] Anteprima prime 3 righe:")
    df.show(3, truncate=False)

    print("\n[5] Distribuzione per mese:")
    df.groupBy("MONTH").count().orderBy("MONTH").show()

    print("\n[6] Verifica colonne disponibili:")
    print("    Colonne:", df.columns)

    spark.stop()
    print("\n[✓] Setup completato correttamente!")