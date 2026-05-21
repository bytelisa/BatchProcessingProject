"""
sabd_spark_only_pipeline.py
────────────────────────────
DAG Airflow – Pipeline Spark-only (senza NiFi)

Presupposto: i dati sono già su HDFS perché NiFi è stato eseguito
in precedenza. Questa pipeline parte direttamente da Spark:

  1. [check]       Verifica che i CSV grezzi siano già su HDFS
  2. [check]       Verifica se il Parquet preprocessato esiste già
  3. [conditional] Preprocessing CSV → Parquet (saltato se Parquet esiste)
  4. [parallel]    Query 1, 2, 3 in parallelo
  5. [export]      Export risultati su Redis

Grafo delle dipendenze:
  check_csv_on_hdfs
        │
  check_parquet_exists
        │
  ┌─────┴────────────────────────┐
  preprocessing (se mancante)    skip_preprocessing (se già presente)
  └─────┬────────────────────────┘
        │  (entrambi convergono su parquet_ready)
  parquet_ready
        │
  ┌─────┼─────┐
  Q1    Q2    Q3   (parallele)
  └─────┼─────┘
        │
   export_to_redis

Posizionare in: airflow/dags/sabd_spark_only_pipeline.py
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess

# ─────────────────────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────────────────────

NAMENODE_CONTAINER     = "batchprocessingproject-namenode-1"
SPARK_MASTER_CONTAINER = "batchprocessingproject-spark-master-1"

# Percorsi HDFS
HDFS_CSV_PATH     = "/data/raw/flights/csv/"
HDFS_PARQUET_PATH = "/data/processed/flights/parquet"

# Numero minimo di CSV attesi (uno per mese: gen, feb, mar, apr)
MIN_CSV_FILES = 4

# Comando base spark-submit (l'f-string viene espansa al momento del task)
SPARK_SUBMIT_TPL = (
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
# Funzioni Python per i task di verifica/branching
# ─────────────────────────────────────────────────────────────

def check_csv_on_hdfs(**kwargs):
    """
    Verifica che i CSV grezzi siano presenti su HDFS.
    Lancia un'eccezione se non vengono trovati almeno MIN_CSV_FILES file.

    Questo task è una guardia: impedisce di eseguire la pipeline
    su dati mancanti. Se NiFi non è mai stato eseguito, il DAG
    si blocca qui con un messaggio chiaro.
    """
    result = subprocess.run(
        ["docker", "exec", NAMENODE_CONTAINER,
         "hdfs", "dfs", "-ls", HDFS_CSV_PATH],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        raise FileNotFoundError(
            f"[HDFS] Directory {HDFS_CSV_PATH} non trovata o non accessibile. "
            "Assicurati che NiFi sia stato eseguito almeno una volta."
        )

    csv_count = result.stdout.count(".csv")
    print(f"[HDFS] Trovati {csv_count} file CSV in {HDFS_CSV_PATH}")

    if csv_count < MIN_CSV_FILES:
        raise ValueError(
            f"[HDFS] Trovati solo {csv_count} CSV, attesi almeno {MIN_CSV_FILES}. "
            "Eseguire prima il DAG completo con NiFi."
        )

    print(f"[HDFS] ✓ Dataset grezzo disponibile ({csv_count} CSV)")


def branch_on_parquet(**kwargs):
    """
    BranchPythonOperator: decide se eseguire il preprocessing o saltarlo.

    - Se il Parquet NON esiste su HDFS  → branch 'spark_preprocessing'
    - Se il Parquet ESISTE già su HDFS  → branch 'skip_preprocessing'

    Il Parquet è considerato esistente se:
      1. La directory esiste su HDFS (hdfs dfs -ls ritorna exit code 0)
      2. Sono presenti file 'part-*' (cioè Spark ha completato la scrittura)
    """
    result = subprocess.run(
        ["docker", "exec", NAMENODE_CONTAINER,
         "hdfs", "dfs", "-ls", HDFS_PARQUET_PATH],
        capture_output=True, text=True
    )

    parquet_exists = (result.returncode == 0) and ("part-" in result.stdout)

    if parquet_exists:
        part_count = result.stdout.count("part-")
        print(
            f"[Spark] Parquet già presente ({part_count} partition file). "
            "Preprocessing saltato."
        )
        return "skip_preprocessing"
    else:
        print(
            "[Spark] Parquet non trovato (o incompleto). "
            "Avvio preprocessing CSV → Parquet."
        )
        return "spark_preprocessing"


def verify_parquet_ready(**kwargs):
    """
    Verifica finale che il Parquet sia correttamente leggibile da Spark.
    Viene eseguita sia dopo il preprocessing che dopo lo skip,
    garantendo che le query trovino dati validi.
    """
    result = subprocess.run(
        ["docker", "exec", NAMENODE_CONTAINER,
         "hdfs", "dfs", "-ls", HDFS_PARQUET_PATH],
        capture_output=True, text=True
    )

    if result.returncode != 0 or "part-" not in result.stdout:
        raise FileNotFoundError(
            f"[Spark] Parquet non disponibile in {HDFS_PARQUET_PATH}. "
            "Controlla l'esito del task di preprocessing."
        )

    part_count = result.stdout.count("part-")
    print(f"[Spark] ✓ Parquet pronto: {part_count} partition file trovati in {HDFS_PARQUET_PATH}")


# ─────────────────────────────────────────────────────────────
# Definizione del DAG
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="sabd_spark_only_pipeline",
    description=(
        "Pipeline SABD Project 1 (Spark-only): "
        "i dati sono già su HDFS via NiFi. "
        "Esegue: [check CSV] → [check/skip preprocessing] → "
        "[Query 1/2/3 in parallelo] → [Redis export]"
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # triggering manuale dalla UI Airflow
    catchup=False,
    tags=["sabd", "spark", "hdfs", "redis", "no-nifi"],
) as dag:

    # ── Stage 0: Guardia – CSV già su HDFS? ──────────────────

    t_check_csv = PythonOperator(
        task_id="check_csv_on_hdfs",
        python_callable=check_csv_on_hdfs,
        doc_md=(
            "Verifica che i CSV grezzi siano presenti su HDFS. "
            "Fallisce con messaggio chiaro se NiFi non è mai stato eseguito."
        ),
    )

    # ── Stage 1: Branch – Parquet già presente? ───────────────

    t_branch = BranchPythonOperator(
        task_id="check_parquet_exists",
        python_callable=branch_on_parquet,
        doc_md=(
            "Branch: se il Parquet è già su HDFS salta il preprocessing, "
            "altrimenti lo esegue. Utile per ri-eseguire solo le query "
            "senza rifare la conversione CSV→Parquet."
        ),
    )

    # ── Stage 2a: Preprocessing (eseguito solo se Parquet manca) ─

    t_preprocess = BashOperator(
        task_id="spark_preprocessing",
        bash_command=SPARK_SUBMIT_TPL.format(script="preprocess.py"),
        doc_md="Converte i CSV grezzi da HDFS in formato Parquet ottimizzato (colonnare).",
    )

    # ── Stage 2b: Skip preprocessing (Parquet già presente) ──

    t_skip_preprocess = EmptyOperator(
        task_id="skip_preprocessing",
        doc_md="No-op: il Parquet è già su HDFS, il preprocessing viene saltato.",
    )

    # ── Stage 2c: Punto di convergenza (join dei due branch) ─

    t_parquet_ready = PythonOperator(
        task_id="parquet_ready",
        python_callable=verify_parquet_ready,
        # trigger_rule=ALL_DONE: viene eseguito indipendentemente da quale
        # branch è stato attivato (preprocessing o skip)
        trigger_rule="none_failed_min_one_success",
        doc_md=(
            "Verifica finale che il Parquet sia leggibile. "
            "Converge i due branch (preprocessing / skip) prima delle query."
        ),
    )

    # ── Stage 3: Query Spark (in parallelo) ──────────────────

    t_query1 = BashOperator(
        task_id="query1_monthly_stats",
        bash_command=SPARK_SUBMIT_TPL.format(script="query1.py"),
        doc_md=(
            "Q1: statistiche mensili (avg/min/max DEP_DELAY, cancellation rate) "
            "per AA e DL. Output: CSV su HDFS + locale."
        ),
    )

    t_query2 = BashOperator(
        task_id="query2_top10_airlines",
        bash_command=SPARK_SUBMIT_TPL.format(script="query2.py"),
        doc_md=(
            "Q2: top 10 compagnie per ARR_DELAY medio, "
            "con breakdown delle cause di ritardo."
        ),
    )

    t_query3 = BashOperator(
        task_id="query3_hourly_percentiles",
        bash_command=SPARK_SUBMIT_TPL.format(script="query3.py"),
        doc_md=(
            "Q3: percentili DEP_DELAY (p25/p50/p75/p90) per fascia oraria "
            "per AA, DL, UA, WN."
        ),
    )

    # ── Stage 4: Export su Redis ──────────────────────────────

    t_export = BashOperator(
        task_id="export_to_redis",
        bash_command=SPARK_SUBMIT_TPL.format(script="export.py"),
        doc_md="Esporta i CSV risultato da HDFS su Redis.",
    )

    # ─────────────────────────────────────────────────────────
    # Grafo delle dipendenze
    #
    #   check_csv_on_hdfs
    #          │
    #   check_parquet_exists  ← BranchPythonOperator
    #       ╱         ╲
    #  spark_         skip_
    #  preprocessing  preprocessing
    #       ╲         ╱
    #       parquet_ready   ← join (none_failed_min_one_success)
    #          │
    #   ┌──────┼──────┐
    #   Q1     Q2     Q3   (parallele)
    #   └──────┼──────┘
    #          │
    #    export_to_redis
    # ─────────────────────────────────────────────────────────

    # Percorso principale
    t_check_csv >> t_branch

    # Due branch alternativi
    t_branch >> t_preprocess >> t_parquet_ready
    t_branch >> t_skip_preprocess >> t_parquet_ready

    # Convergenza → query in parallelo → export
    t_parquet_ready >> [t_query1, t_query2, t_query3] >> t_export