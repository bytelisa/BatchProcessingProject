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

FLIGHT_SCHEMA = StructType([
    StructField("YEAR",                IntegerType(), True),
    StructField("MONTH",               IntegerType(), True),
    StructField("DAY_OF_MONTH",        IntegerType(), True),
    StructField("OP_UNIQUE_CARRIER",   StringType(),  True),
    StructField("ORIGIN_AIRPORT_ID",   IntegerType(), True),
    StructField("DEST_AIRPORT_ID",     IntegerType(), True),
    StructField("CRS_DEP_TIME",        IntegerType(), True),   # es. 830 = 08:30
    StructField("DEP_DELAY",           FloatType(),   True),
    StructField("ARR_DELAY",           FloatType(),   True),
    StructField("CANCELLED",           FloatType(),   True),   # 1.0 = cancellato
    StructField("DIVERTED",            FloatType(),   True),   # 1.0 = deviato
    StructField("CARRIER_DELAY",       FloatType(),   True),
    StructField("WEATHER_DELAY",       FloatType(),   True),
    StructField("NAS_DELAY",           FloatType(),   True),
    StructField("SECURITY_DELAY",      FloatType(),   True),
    StructField("LATE_AIRCRAFT_DELAY", FloatType(),   True),
])

# ─────────────────────────────────────────────
# Caricamento dati
# ─────────────────────────────────────────────

def load_flights(spark: SparkSession):
    """
    Carica tutti e 4 i CSV mensili da HDFS in un unico DataFrame.
    Usa lo schema esplicito per evitare il costoso inferSchema.
    """
    df = (
        spark.read
        .option("header", "true")
        .option("nullValue", "")          # celle vuote → null
        .option("nanValue", "NA")         # valori NA → null
        .schema(FLIGHT_SCHEMA)
        .csv(HDFS_CSV_PATH)
    )
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
    - local=True  → /opt/results/<filename>  (visibile su ./results sul PC)
    - local=False → HDFS /data/processed/flights/<filename>
    Usa coalesce(1) per ottenere un singolo file CSV invece di tanti part-*.
    """
    if local:
        path = f"{LOCAL_OUT_PATH}/{filename}"
    else:
        path = f"{HDFS_OUT_PATH}/{filename}"

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    print(f"[✓] Risultato salvato in: {path}")

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