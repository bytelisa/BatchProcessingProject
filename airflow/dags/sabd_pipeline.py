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
NIFI_PG_NAME  = "targz-from-url-to-csv-on-hdfs"

# Nomi container Docker (verifica con: docker compose ps)
NAMENODE_CONTAINER     = "batchprocessingproject-namenode-1"
SPARK_MASTER_CONTAINER = "batchprocessingproject-spark-master-1"

# Percorso HDFS dove NiFi deposita i CSV
HDFS_CSV_PATH = "/data/raw/flights/csv/"
HDFS_OUTPUT_PATH = "/data/output/flights"

EXPECTED_QUERY_OUTPUTS = [
    "query1_monthly_stats",
    "query2_all_airlines_stats",
    "query2_top10_arrival_delay",
    "query3_hourly_percentiles",
    "query3_global_minmax",
]


# Comando base spark-submit
SPARK_SUBMIT = (
    f"docker exec {SPARK_MASTER_CONTAINER} "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "/opt/scripts/{script}"
)

# Comando per lanciare lo script di export verso Redis
EXPORT_TO_REDIS = (
    f"docker exec "
    f"-e REDIS_HOST=redis "
    f"-e EXPORT_SOURCE=hdfs "
    f"{SPARK_MASTER_CONTAINER} "
    "/opt/spark/bin/spark-submit "
    "--master local[*] "
    "/opt/scripts/export_output_to_redis.py"
)

# ─────────────────────────────────────────────────────────────
# Default args del DAG
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "sabd",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,
}

# ─────────────────────────────────────────────────────────────
# Helper: sessione HTTP senza keep-alive (fix DNS intermittente)
# ─────────────────────────────────────────────────────────────

def nifi_session() -> requests.Session:
    """
    FIX DNS: crea una nuova Session requests con Connection: close.
    Impedisce il riuso delle connessioni TCP che causa il problema
    di name resolution intermittente nel DNS interno di Docker.
    Usare sempre questa funzione invece di requests.get/put direttamente.
    """
    s = requests.Session()
    s.headers.update({"Connection": "close"})
    return s


# ─────────────────────────────────────────────────────────────
# Helper: recupera dinamicamente il Process Group ID da NiFi
# ─────────────────────────────────────────────────────────────

def get_nifi_pg_id(pg_name: str) -> str:
    """
    Recupera dinamicamente l'ID del Process Group cercandolo
    per nome tramite la REST API di NiFi.
    """
    s = nifi_session()

    # Step 1: recupera l'ID del root process group
    resp = s.get(f"{NIFI_BASE_URL}/process-groups/root", timeout=10)
    resp.raise_for_status()
    root_id = resp.json()["id"]
    s.close()

    # Step 2: lista tutti i process group figli del root
    s = nifi_session()
    resp = s.get(
        f"{NIFI_BASE_URL}/process-groups/{root_id}/process-groups",
        timeout=10
    )
    resp.raise_for_status()
    groups = resp.json().get("processGroups", [])
    s.close()

    # Step 3: cerca per nome
    pg = next(
        (g for g in groups if g["component"]["name"] == pg_name),
        None
    )
    if pg is None:
        available = [g["component"]["name"] for g in groups]
        raise ValueError(
            f"[NiFi] Process Group '{pg_name}' non trovato. "
            f"Process Group disponibili: {available}"
        )

    pg_id = pg["id"]
    print(f"[NiFi] ✓ Process Group '{pg_name}' trovato con ID: {pg_id}")
    return pg_id


# ─────────────────────────────────────────────────────────────
# Funzioni Python per i task NiFi
# ─────────────────────────────────────────────────────────────

def nifi_start_flow(**kwargs):
    """
    Avvia il Process Group NiFi tramite la REST API.
    """
    pg_id = get_nifi_pg_id(NIFI_PG_NAME)

    # Recupera la revision corrente (obbligatoria per la PUT)
    s = nifi_session()
    url_pg = f"{NIFI_BASE_URL}/process-groups/{pg_id}"
    resp = s.get(url_pg, timeout=10)
    resp.raise_for_status()
    revision = resp.json()["revision"]
    s.close()

    # Avvia il process group
    s = nifi_session()
    url_run = f"{NIFI_BASE_URL}/flow/process-groups/{pg_id}"
    payload = {
        "id": pg_id,
        "state": "RUNNING",
        "disconnectedNodeAcknowledged": False,
    }
    resp = s.put(url_run, json=payload, timeout=10)
    resp.raise_for_status()
    s.close()

    print(f"[NiFi] ✓ Flow avviato (HTTP {resp.status_code})")
    print(f"[NiFi]   Process Group: {pg_id}")

    # Salva l'ID nel XCom per i task successivi
    kwargs["ti"].xcom_push(key="nifi_pg_id", value=pg_id)


def nifi_wait_completion(**kwargs):
    """
    Polling sulla REST API di NiFi: aspetta che il flow abbia
    completato l'ingestion verificando due condizioni:
      1. Almeno 4 file CSV presenti su HDFS (gennaio-aprile 2025)
      2. Nessun FlowFile in coda nel Process Group (queued = 0)
    Timeout massimo: 30 minuti.
    """
    print("[NiFi] Attendo completamento ingestion...")

    pg_id = kwargs["ti"].xcom_pull(task_ids="nifi_start_ingestion", key="nifi_pg_id")
    if not pg_id:
        pg_id = get_nifi_pg_id(NIFI_PG_NAME)

    MAX_WAIT_SECONDS = 1800
    POLL_INTERVAL    = 15
    STABLE_ROUNDS    = 3
    MIN_CSV_FILES    = 4

    elapsed      = 0
    stable_count = 0
    csv_count    = 0

    while elapsed < MAX_WAIT_SECONDS:

        # Check 1: quanti CSV ci sono su HDFS?
        result = subprocess.run(
            ["docker", "exec", NAMENODE_CONTAINER,
             "hdfs", "dfs", "-ls", HDFS_CSV_PATH],
            capture_output=True, text=True
        )
        csv_count = result.stdout.count(".csv")

        # Check 2: quanti FlowFile sono ancora in coda?
        try:
            s = nifi_session()
            url_status = f"{NIFI_BASE_URL}/process-groups/{pg_id}"
            resp = s.get(url_status, timeout=5)
            resp.raise_for_status()
            pg_data = resp.json()
            queued  = pg_data["status"]["aggregateSnapshot"]["flowFilesQueued"]
            running = pg_data["component"]["runningCount"]
            s.close()
        except Exception as e:
            print(f"  [WARN] Errore lettura stato NiFi: {e}")
            queued  = -1
            running = -1

        print(
            f"  t={elapsed:>4}s | CSV su HDFS: {csv_count} | "
            f"FlowFile in coda: {queued} | Processor running: {running}"
        )

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

    raise TimeoutError(
        f"[NiFi] ✗ Timeout: ingestion non completata in {MAX_WAIT_SECONDS}s. "
        f"CSV trovati: {csv_count}"
    )


def nifi_stop_flow(**kwargs):
    """
    Ferma il Process Group NiFi dopo il completamento dell'ingestion.
    """
    pg_id = kwargs["ti"].xcom_pull(task_ids="nifi_start_ingestion", key="nifi_pg_id")
    if not pg_id:
        pg_id = get_nifi_pg_id(NIFI_PG_NAME)

    s = nifi_session()
    url_run = f"{NIFI_BASE_URL}/flow/process-groups/{pg_id}"
    payload = {
        "id": pg_id,
        "state": "STOPPED",
        "disconnectedNodeAcknowledged": False,
    }
    resp = s.put(url_run, json=payload, timeout=10)
    resp.raise_for_status()
    s.close()

    print(f"[NiFi] ✓ Flow fermato (HTTP {resp.status_code})")


def check_parquet_ready(**kwargs):
    """
    Verifica che il Parquet sia stato scritto correttamente su HDFS.
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

    part_count = result.stdout.count("part-")
    print(f"[Spark] ✓ Parquet pronto: {part_count} partition file trovati")

def check_query_outputs_ready(**kwargs):
    """
    Verifica che tutti gli output delle query siano presenti su HDFS
    prima di esportare verso Redis.
    """
    missing = []

    for output_dir in EXPECTED_QUERY_OUTPUTS:
        path = f"{HDFS_OUTPUT_PATH}/{output_dir}"

        result = subprocess.run(
            [
                "docker", "exec", NAMENODE_CONTAINER,
                "hdfs", "dfs", "-test", "-e", path,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            missing.append(path)
            continue

        result_ls = subprocess.run(
            [
                "docker", "exec", NAMENODE_CONTAINER,
                "hdfs", "dfs", "-ls", path,
            ],
            capture_output=True,
            text=True,
        )

        part_count = result_ls.stdout.count("part-")
        print(f"[HDFS] ✓ Output presente: {path} ({part_count} part file)")

        if part_count == 0:
            missing.append(f"{path} (nessun part file)")

    if missing:
        raise FileNotFoundError(
            "[HDFS] Output query mancanti o incompleti:\n"
            + "\n".join(f"  - {p}" for p in missing)
        )

    print("[HDFS] ✓ Tutti gli output delle query sono pronti")


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
    schedule_interval=None,
    catchup=False,
    tags=["sabd", "spark", "nifi", "hdfs", "redis"],
) as dag:

    t_nifi_start = PythonOperator(
        task_id="nifi_start_ingestion",
        python_callable=nifi_start_flow,
        doc_md="Avvia il Process Group NiFi via REST API.",
    )

    t_nifi_wait = PythonOperator(
        task_id="nifi_wait_completion",
        python_callable=nifi_wait_completion,
        doc_md="Polling ogni 15s: aspetta completamento ingestion su HDFS.",
        execution_timeout=timedelta(minutes=35),
    )

    t_nifi_stop = PythonOperator(
        task_id="nifi_stop_flow",
        python_callable=nifi_stop_flow,
        doc_md="Ferma il Process Group NiFi dopo il completamento.",
    )

    t_preprocess = BashOperator(
        task_id="spark_preprocessing",
        bash_command=SPARK_SUBMIT.format(script="preprocess.py"),
        doc_md="Converte i CSV grezzi da HDFS in formato Parquet ottimizzato.",
    )

    t_check_parquet = PythonOperator(
        task_id="check_parquet_ready",
        python_callable=check_parquet_ready,
        doc_md="Verifica che il Parquet sia stato scritto correttamente.",
    )

    t_query1 = BashOperator(
        task_id="query1_monthly_stats",
        bash_command=SPARK_SUBMIT.format(script="query1.py"),
        doc_md="Q1: statistiche mensili per AA e DL.",
    )

    t_query2 = BashOperator(
        task_id="query2_top10_airlines",
        bash_command=SPARK_SUBMIT.format(script="query2.py"),
        doc_md="Q2: top 10 compagnie per ARR_DELAY medio.",
    )

    t_query3 = BashOperator(
        task_id="query3_hourly_percentiles",
        bash_command=SPARK_SUBMIT.format(script="query3.py"),
        doc_md="Q3: percentili DEP_DELAY per fascia oraria.",
    )

    t_check_query_outputs = PythonOperator(
        task_id="check_query_outputs_ready",
        python_callable=check_query_outputs_ready,
        doc_md="Verifica che gli output CSV delle query siano presenti su HDFS.",
    )

    t_export = BashOperator(
        task_id="export_output_to_redis",
        bash_command=EXPORT_TO_REDIS,
        doc_md="Esporta gli output delle query da HDFS a Redis per Grafana.",
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
    #   t_check_query_outputs
    #       |
    #   t_export
    # ─────────────────────────────────────────────────────────

    (
            t_nifi_start
            >> t_nifi_wait
            >> t_nifi_stop
            >> t_preprocess
            >> t_check_parquet
            >> [t_query1, t_query2, t_query3]
            >> t_check_query_outputs
            >> t_export
    )