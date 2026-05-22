"""
sabd_pipeline.py
────────────────
DAG Airflow
Pipeline end-to-end:
  1. NiFi: scarica il dataset e carica i CSV su HDFS
  2. Spark: preprocessing CSV → Parquet
  3. Spark: Query 1, 2, 3 (in parallelo)
  4. Export risultati su Redis

Posizionare in: airflow/dags/sabd_pipeline.py
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import subprocess
import time

# ─────────────────────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────────────────────



# NiFi
NIFI_BASE_URL = "http://nifi:9090/nifi-api"
NIFI_PG_ID    = "4b2d988c-019e-1000-121f-1ffedd856d87"   # targz-from-url-to-csv-on-hdfs

# Nomi container Docker (verifica con: docker compose ps)
NAMENODE_CONTAINER   = "batchprocessingproject-namenode-1"
SPARK_MASTER_CONTAINER = "batchprocessingproject-spark-master-1"

# Percorso HDFS dove NiFi deposita i CSV
HDFS_CSV_PATH = "/data/raw/flights/csv/"

# Comando base spark-submit
SPARK_SUBMIT = (
    f"docker exec {SPARK_MASTER_CONTAINER} "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "/opt/scripts/{script}"
)

# ─────────────────────────────────────────────────────────────
# Default args del DAG
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "sabd",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# ─────────────────────────────────────────────────────────────
# Funzioni Python per i task NiFi
# ─────────────────────────────────────────────────────────────

def nifi_start_flow(**kwargs):
    """
    Avvia il Process Group NiFi 'targz-from-url-to-csv-on-hdfs'
    tramite la REST API di NiFi.
    """
    # Prima recupera la revision corrente (obbligatoria per la PUT)
    url_pg = f"{NIFI_BASE_URL}/process-groups/{NIFI_PG_ID}"
    resp = requests.get(url_pg)
    resp.raise_for_status()
    revision = resp.json()["revision"]

    # Avvia il process group
    url_run = f"{NIFI_BASE_URL}/flow/process-groups/{NIFI_PG_ID}"
    payload = {
        "id": NIFI_PG_ID,
        "state": "RUNNING",
        "disconnectedNodeAcknowledged": False,
    }
    resp = requests.put(url_run, json=payload)
    resp.raise_for_status()

    print(f"[NiFi] ✓ Flow avviato (HTTP {resp.status_code})")
    print(f"[NiFi]   Process Group: {NIFI_PG_ID}")


def nifi_wait_completion(**kwargs):
    """
    Polling sulla REST API di NiFi: aspetta che il flow abbia
    completato l'ingestion verificando due condizioni:
      1. Almeno 4 file CSV presenti su HDFS (gennaio-aprile 2025)
      2. Nessun FlowFile in coda nel Process Group (queued = 0)

    Timeout massimo: 30 minuti.
    """
    print("[NiFi] Attendo completamento ingestion...")

    MAX_WAIT_SECONDS = 1800   # 30 minuti
    POLL_INTERVAL    = 15     # check ogni 15 secondi
    STABLE_ROUNDS    = 3      # quante volte consecutive deve risultare stabile
    MIN_CSV_FILES    = 4      # almeno 4 CSV (uno per mese: gen, feb, mar, apr)

    elapsed      = 0
    stable_count = 0

    while elapsed < MAX_WAIT_SECONDS:

        # ── Check 1: quanti CSV ci sono su HDFS? ──────────────
        result = subprocess.run(
            ["docker", "exec", NAMENODE_CONTAINER,
             "hdfs", "dfs", "-ls", HDFS_CSV_PATH],
            capture_output=True, text=True
        )
        csv_count = result.stdout.count(".csv")

        # ── Check 2: quanti FlowFile sono ancora in coda? ─────
        url_status = f"{NIFI_BASE_URL}/process-groups/{NIFI_PG_ID}"
        try:
            resp = requests.get(url_status, timeout=5)
            resp.raise_for_status()
            pg_data    = resp.json()
            queued     = pg_data["status"]["aggregateSnapshot"]["flowFilesQueued"]
            running    = pg_data["component"]["runningCount"]
        except Exception as e:
            print(f"  [WARN] Errore lettura stato NiFi: {e}")
            queued  = -1
            running = -1

        print(
            f"  t={elapsed:>4}s | CSV su HDFS: {csv_count} | "
            f"FlowFile in coda: {queued} | Processor running: {running}"
        )

        # ── Condizione di completamento ───────────────────────
        if csv_count >= MIN_CSV_FILES and queued == 0:
            stable_count += 1
            print(f"  Condizione stabile ({stable_count}/{STABLE_ROUNDS})")
        else:
            stable_count = 0

        if stable_count >= STABLE_ROUNDS:
            print(f"[NiFi] ✓ Ingestion completata: {csv_count} CSV su HDFS")
            return

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    # Se arriviamo qui è scaduto il timeout
    raise TimeoutError(
        f"[NiFi] ✗ Timeout: ingestion non completata in {MAX_WAIT_SECONDS}s. "
        f"CSV trovati: {csv_count}"
    )


def nifi_stop_flow(**kwargs):
    """
    Ferma il Process Group NiFi dopo il completamento dell'ingestion.
    """
    url_run = f"{NIFI_BASE_URL}/flow/process-groups/{NIFI_PG_ID}"
    payload = {
        "id": NIFI_PG_ID,
        "state": "STOPPED",
        "disconnectedNodeAcknowledged": False,
    }
    resp = requests.put(url_run, json=payload)
    resp.raise_for_status()

    print(f"[NiFi] ✓ Flow fermato (HTTP {resp.status_code})")


def check_parquet_ready(**kwargs):
    """
    Verifica che il Parquet sia stato scritto correttamente su HDFS
    prima di lanciare le query. Controlla che la directory non sia vuota.
    """
    HDFS_PARQUET_PATH = "/data/processed/flights/parquet"

    result = subprocess.run(
        ["docker", "exec", NAMENODE_CONTAINER,
         "hdfs", "dfs", "-ls", HDFS_PARQUET_PATH],
        capture_output=True, text=True
    )

    if result.returncode != 0 or "parquet" not in result.stdout:
        raise FileNotFoundError(
            f"[Spark] Parquet non trovato in {HDFS_PARQUET_PATH}. "
            "Verifica che il preprocessing sia andato a buon fine."
        )

    # Conta i file part-* generati da Spark
    part_count = result.stdout.count("part-")
    print(f"[Spark] ✓ Parquet pronto: {part_count} partition file trovati")


# ─────────────────────────────────────────────────────────────
# Definizione del DAG
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="sabd_project1_pipeline",
    description=(
        "Pipeline SABD Project 1: "
        "NiFi ingestion → Spark preprocessing → Query 1/2/3 → Redis export"
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # triggering manuale dalla UI
    catchup=False,
    tags=["sabd", "spark", "nifi", "hdfs", "redis"],
) as dag:

    # ── Stage 1: NiFi Ingestion ───────────────────────────────

    t_nifi_start = PythonOperator(
        task_id="nifi_start_ingestion",
        python_callable=nifi_start_flow,
        doc_md="Avvia il Process Group NiFi `targz-from-url-to-csv-on-hdfs` via REST API.",
    )

    t_nifi_wait = PythonOperator(
        task_id="nifi_wait_completion",
        python_callable=nifi_wait_completion,
        doc_md=(
            "Polling ogni 15s: aspetta che NiFi abbia scaricato e "
            "caricato almeno 4 CSV su HDFS e che la coda sia vuota."
        ),
        execution_timeout=timedelta(minutes=35),   # timeout task > timeout interno
    )

    t_nifi_stop = PythonOperator(
        task_id="nifi_stop_flow",
        python_callable=nifi_stop_flow,
        doc_md="Ferma il Process Group NiFi dopo il completamento dell'ingestion.",
    )

    # ── Stage 2: Preprocessing Spark ─────────────────────────

    t_preprocess = BashOperator(
        task_id="spark_preprocessing",
        bash_command=SPARK_SUBMIT.format(script="preprocess.py"),
        doc_md="Converte i CSV grezzi da HDFS in formato Parquet ottimizzato.",
    )

    t_check_parquet = PythonOperator(
        task_id="check_parquet_ready",
        python_callable=check_parquet_ready,
        doc_md="Verifica che il Parquet sia stato scritto correttamente prima di lanciare le query.",
    )

    # ── Stage 3: Query Spark (parallele) ─────────────────────

    t_query1 = BashOperator(
        task_id="query1_monthly_stats",
        bash_command=SPARK_SUBMIT.format(script="query1.py"),
        doc_md=(
            "Q1: statistiche mensili (avg/min/max DEP_DELAY, cancellation rate) "
            "per AA e DL."
        ),
    )

    t_query2 = BashOperator(
        task_id="query2_top10_airlines",
        bash_command=SPARK_SUBMIT.format(script="query2.py"),
        doc_md=(
            "Q2: top 10 compagnie per ARR_DELAY medio, "
            "con breakdown delle cause di ritardo."
        ),
    )

    t_query3 = BashOperator(
        task_id="query3_hourly_percentiles",
        bash_command=SPARK_SUBMIT.format(script="query3.py"),
        doc_md=(
            "Q3: percentili DEP_DELAY (p25/p50/p75/p90) per fascia oraria "
            "per AA, DL, UA, WN."
        ),
    )

    # ── Stage 4: Export su Redis ──────────────────────────────

    t_export = BashOperator(
        task_id="export_to_redis",
        bash_command=SPARK_SUBMIT.format(script="export.py"),
        doc_md="Esporta i CSV risultato da HDFS su Redis.",
    )

    # ─────────────────────────────────────────────────────────
    # Grafo delle dipendenze
    #
    #  t_nifi_start
    #       │
    #  t_nifi_wait
    #       │
    #  t_nifi_stop
    #       │
    #  t_preprocess
    #       │
    #  t_check_parquet
    #       │
    #  ┌────┼────┐
    #  Q1   Q2   Q3   (parallele)
    #  └────┼────┘
    #       │
    #   t_export
    # ─────────────────────────────────────────────────────────

    (
        t_nifi_start
        >> t_nifi_wait
        >> t_nifi_stop
        >> t_preprocess
        >> t_check_parquet
        >> [t_query1, t_query2, t_query3]
        >> t_export
    )