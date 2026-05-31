"""
run_worker_benchmarks.py
────────────────────────
Runner host-side per eseguire benchmark Spark al variare del numero di worker.

Responsabilità:
1. legge src/benchmark_config.json;
2. scala spark-worker a N repliche;
3. aspetta che Spark Master veda N worker ALIVE;
4. lancia benchmark_rdd_vs_df.py dentro spark-master;
5. passa SPARK_WORKER_COUNT=N;
6. produce un report separato per ogni N;
7. crea un CSV aggregato locale in output/benchmarks/benchmark_scaling_summary.csv.

Esecuzione da root progetto:
  python3 tools/run_worker_benchmarks.py
"""

import csv
import json
import os
import subprocess
import sys
import time
from urllib.request import urlopen

# ─────────────────────────────────────────────
# Path progetto
# ─────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

CONFIG_PATH = os.path.join(PROJECT_ROOT, "src", "benchmark_config.json")

SPARK_MASTER_JSON_URL = "http://localhost:8080/json"

SUMMARY_PATH = "output/benchmarks/benchmark_scaling_summary.csv"

SUMMARY_RAW_PATH = os.path.join(
    PROJECT_ROOT,
    "output",
    "benchmarks",
    "benchmark_raw_iterations_summary.csv",
)




def run_cmd(cmd, check=True, timeout=180, stream_output=False):
    print("\n[CMD] " + " ".join(cmd))

    try:
        if stream_output:
            result = subprocess.run(
                cmd,
                cwd=PROJECT_ROOT,
                text=True,
                timeout=timeout,
            )
        else:
            result = subprocess.run(
                cmd,
                cwd=PROJECT_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )

            if result.stdout:
                print(result.stdout)

            if result.stderr:
                print(result.stderr)

    except subprocess.TimeoutExpired:
        raise TimeoutError(
            "Timeout dopo "
            + str(timeout)
            + "s sul comando: "
            + " ".join(cmd)
        )

    if check and result.returncode != 0:
        raise RuntimeError(
            "Comando fallito con exit code "
            + str(result.returncode)
            + ": "
            + " ".join(cmd)
        )

    return result

def load_config():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError("Config non trovata: " + CONFIG_PATH)

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def start_required_services():
    """
    Avvia i servizi minimi necessari al benchmark:
    - HDFS: namenode + datanode
    - Spark master
    I worker vengono scalati separatamente da scale_workers().
    """
    print("\n[INFO] Avvio servizi richiesti: namenode, datanode, spark-master")

    run_cmd([
        "docker",
        "compose",
        "up",
        "-d",
        "namenode",
        "datanode",
        "spark-master",
    ])


def get_alive_worker_count():
    with urlopen(SPARK_MASTER_JSON_URL, timeout=5) as response:
        data = json.loads(response.read().decode("utf-8"))

    workers = data.get("workers", [])
    alive = [
        w for w in workers
        if str(w.get("state", "")).upper() == "ALIVE"
    ]

    return len(alive)


def wait_for_workers(expected_count, timeout_seconds=180):
    print(
        "\n[INFO] Attendo "
        + str(expected_count)
        + " worker ALIVE sullo Spark Master..."
    )

    start = time.time()

    while time.time() - start < timeout_seconds:
        try:
            alive_count = get_alive_worker_count()
        except Exception as exc:
            print("[WARN] Impossibile leggere Spark Master: " + str(exc))
            alive_count = 0

        print("       worker ALIVE: " + str(alive_count) + "/" + str(expected_count))

        if alive_count == expected_count:
            print("[✓] Numero worker corretto")
            return

        time.sleep(5)

    raise TimeoutError(
        "Timeout: Spark Master vede "
        + str(alive_count)
        + " worker ALIVE, attesi "
        + str(expected_count)
    )

def wait_for_hdfs(timeout_seconds=120):
    """
    Aspetta che HDFS sia pronto e che il path Parquet preprocessato esista.
    """
    print("[INFO] Attendo disponibilità HDFS e dataset Parquet...")

    start = time.time()

    while time.time() - start < timeout_seconds:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "namenode",
                "hdfs",
                "dfs",
                "-test",
                "-e",
                "/data/processed/flights/parquet",
            ],
            text=True,
        )

        if result.returncode == 0:
            print("[✓] HDFS pronto: Parquet trovato")
            return

        print("       HDFS non ancora pronto o Parquet non trovato...")
        time.sleep(5)

    raise TimeoutError(
        "Timeout: HDFS non pronto oppure "
        "/data/processed/flights/parquet non esiste"
    )

def scale_workers(worker_count):
    """
    Scala spark-worker al numero richiesto.
    Se Docker Compose resta appeso, fallisce con timeout invece di bloccare
    tutto il terminale.
    """
    print("\n[INFO] Scaling spark-worker a " + str(worker_count) + " repliche")

    run_cmd([
        "docker",
        "compose",
        "up",
        "-d",
        "--scale",
        "spark-worker=" + str(worker_count),
        "spark-worker",
    ], timeout=180)

    wait_for_workers(worker_count, timeout_seconds=180)


def run_benchmark(worker_count, benchmark_script):
    print(
        "\n[INFO] Lancio benchmark con "
        + str(worker_count)
        + " worker: "
        + benchmark_script
    )

    run_cmd([
        "docker",
        "compose",
        "exec",
        "-T",
        "-e",
        "SPARK_WORKER_COUNT=" + str(worker_count),
        "-e",
        "BENCHMARK_CONFIG=/opt/scripts/benchmark_config.json",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "/opt/scripts/" + benchmark_script,
    ], timeout=7200, stream_output=True)


def aggregate_reports(worker_counts):
    os.makedirs(os.path.dirname(SUMMARY_PATH), exist_ok=True)

    all_rows = []
    fieldnames = None

    for worker_count in worker_counts:
        path = (
            "output/benchmarks/"
            "benchmark_rdd_vs_df_workers_"
            + str(worker_count)
            + ".csv"
        )

        if not os.path.exists(path):
            print("[WARN] Report non trovato, salto: " + path)
            continue

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            print("[WARN] Report vuoto, salto: " + path)
            continue

        if fieldnames is None:
            fieldnames = reader.fieldnames

        all_rows.extend(rows)

    if not all_rows:
        print("[WARN] Nessun report aggregato creato")
        return

    with open(SUMMARY_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print("\n[✓] Report aggregato salvato in: " + SUMMARY_PATH)


def aggregate_raw_reports(worker_counts):
    os.makedirs(os.path.dirname(SUMMARY_RAW_PATH), exist_ok=True)

    all_rows = []
    fieldnames = None

    for worker_count in worker_counts:
        path = os.path.join(
            PROJECT_ROOT,
            "output",
            "benchmarks",
            "benchmark_rdd_vs_df_raw_workers_" + str(worker_count) + ".csv",
        )

        if not os.path.exists(path):
            print("[WARN] Raw report non trovato, salto: " + path)
            continue

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            print("[WARN] Raw report vuoto, salto: " + path)
            continue

        if fieldnames is None:
            fieldnames = reader.fieldnames

        all_rows.extend(rows)

    if not all_rows:
        print("[WARN] Nessun raw report aggregato creato")
        return

    with open(SUMMARY_RAW_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print("\n[✓] Raw report aggregato salvato in: " + SUMMARY_RAW_PATH)


def main():
    cfg = load_config()

    worker_counts = cfg.get("worker_counts", [1])
    benchmark_script = cfg.get("benchmark_script", "benchmark_rdd_vs_df.py")

    print("=" * 72)
    print("  SABD Project 1 — Benchmark scaling Spark workers")
    print("=" * 72)
    print("Worker counts:    " + str(worker_counts))
    print("Benchmark script: " + benchmark_script)
    print("=" * 72)

    start_required_services()
    wait_for_hdfs()

    for worker_count in worker_counts:
        print("\n" + "=" * 72)
        print("BENCHMARK CON " + str(worker_count) + " WORKER")
        print("=" * 72)

        scale_workers(worker_count)
        run_benchmark(worker_count, benchmark_script)

    aggregate_reports(worker_counts)
    aggregate_raw_reports(worker_counts)

    print("\n[✓] Benchmark scaling completato")


if __name__ == "__main__":
    main()