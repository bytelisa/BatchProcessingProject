"""
config.py
─────────
Configurazione centrale dei path del progetto SABD.
"""

import os


HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:9000")

# Input raw su HDFS
HDFS_RAW_CSV_PATH = os.getenv(
    "HDFS_RAW_CSV_PATH",
    f"{HDFS_BASE}/data/raw/flights/csv/20250*_T_ONTIME_REPORTING.csv",
)

# Dataset preprocessato in Parquet
HDFS_PROCESSED_PARQUET_PATH = os.getenv(
    "HDFS_PROCESSED_PARQUET_PATH",
    f"{HDFS_BASE}/data/processed/flights/parquet",
)

# Output query su HDFS
HDFS_QUERY_OUTPUT_PATH = os.getenv(
    "HDFS_QUERY_OUTPUT_PATH",
    f"{HDFS_BASE}/data/output/flights",
)

# Output query locale, montato su ./output
LOCAL_OUTPUT_PATH = os.getenv(
    "LOCAL_OUTPUT_PATH",
    "/opt/output",
)