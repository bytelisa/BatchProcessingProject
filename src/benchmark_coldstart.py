"""
Benchmark Cold Start delle Query Spark

Variante del benchmark principale in cui la SparkSession viene
fermata e ricreata a ogni iterazione, simulando il cold start
(JVM spenta, nessuna cache di OS/JVM, piano di esecuzione da ricompilare).

Confronto con benchmark_warmup.py (warm session):
  - benchmark_warmup.py          → sessione unica condivisa: misura throughput
                            in condizioni operative (JVM calda, page cache attiva)
  - benchmark_coldstart.py → sessione riavviata ogni volta: misura latenza
                            percepita al primo avvio della query (cold start)

Fasi aggiuntive misurate:
  - spark_start_s — tempo di boot della JVM + connessione al master
  - spark_stop_s — tempo di shutdown
  - wall_total_s — include anche il boot, quindi è il tempo percepito reale da zero

Utilizzo:
    ./run.sh benchmark_coldstart.py

Parametri configurabili (sezione CONFIG):
    TOTAL_ITERATIONS  – numero totale di esecuzioni per query  (default: 10)
    WARMUP_ITERATIONS – iterazioni di warm-up escluse dai dati (default:  0)
    QUERIES_TO_RUN    – lista delle query da eseguire
"""

import time
import math
import csv
import os
import sys

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Ogni iterazione include il boot della JVM
TOTAL_ITERATIONS  = 15
WARMUP_ITERATIONS =  0
QUERIES_TO_RUN    = [1, 2, 3]

BENCHMARK_REPORT_PATH = "/opt/output/benchmark_coldstart_report.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Import
# ─────────────────────────────────────────────────────────────────────────────

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from utils import get_spark_session

if 1 in QUERIES_TO_RUN:
    from query1 import run_query1
if 2 in QUERIES_TO_RUN:
    from query2 import run_query2
if 3 in QUERIES_TO_RUN:
    from query3 import run_query3


# ─────────────────────────────────────────────────────────────────────────────
# Helpers statistici
# ─────────────────────────────────────────────────────────────────────────────

def _mean(values):
    return sum(values) / len(values)

def _std(values):
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    variance = sum((x - m) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(variance)

def _median(values):
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0

def compute_stats(values):
    return {
        "n":        len(values),
        "mean_s":   round(_mean(values),   3),
        "std_s":    round(_std(values),    3),
        "median_s": round(_median(values), 3),
        "min_s":    round(min(values),     3),
        "max_s":    round(max(values),     3),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Singola iterazione con cold start
# ─────────────────────────────────────────────────────────────────────────────

def _run_cold_iteration(query_id, save_output):
    """
    Avvia una SparkSession, esegue la query, ferma la sessione.
    Misura anche il tempo di startup e shutdown della JVM.
    Restituisce il dict timings arricchito con:
      - spark_start_s  : tempo di avvio SparkSession
      - spark_stop_s   : tempo di shutdown SparkSession
      - wall_total_s   : tempo wall totale incluso start/stop Spark
    """

    # ── Avvio Spark ──────────────────────────────────────────────────────────
    t_start = time.time()
    spark = get_spark_session("SABD-Benchmark-Cold")
    spark_start_s = round(time.time() - t_start, 3)

    wall_t0 = time.time()

    try:
        if query_id == 1:
            _, timings = run_query1(spark, save_output=save_output, print_preview=False)
        elif query_id == 2:
            _, _, timings = run_query2(spark, save_output=save_output, print_preview=False)
        elif query_id == 3:
            _, _, timings = run_query3(spark, save_output=save_output, print_preview=False)
        else:
            raise ValueError(f"Query {query_id} non supportata.")
    finally:
        # ── Stop Spark ───────────────────────────────────────────────────────
        t_stop = time.time()
        spark.stop()
        spark_stop_s = round(time.time() - t_stop, 3)

    timings["spark_start_s"] = spark_start_s
    timings["spark_stop_s"]  = spark_stop_s
    timings["wall_total_s"]  = round(time.time() - wall_t0 + spark_start_s, 3)

    return timings


# ─────────────────────────────────────────────────────────────────────────────
# Funzione principale di benchmarking
# ─────────────────────────────────────────────────────────────────────────────

def benchmark_query_cold(query_id):
    label = f"Query {query_id}"
    separator = "─" * 72

    print(f"\n{separator}")
    print(f"  BENCHMARK COLD START {label}")
    print(f"  Iterazioni totali:  {TOTAL_ITERATIONS}")
    print(f"  Warm-up (escluse):  {WARMUP_ITERATIONS}")
    print(f"  Iterazioni valide:  {TOTAL_ITERATIONS - WARMUP_ITERATIONS}")
    print(f"  Nota: SparkSession riavviata a ogni iterazione")
    print(f"{separator}")

    collected_timings = []

    for i in range(1, TOTAL_ITERATIONS + 1):
        is_warmup = i <= WARMUP_ITERATIONS
        tag = "[WARM-UP]" if is_warmup else f"[{i - WARMUP_ITERATIONS:>2}/{TOTAL_ITERATIONS - WARMUP_ITERATIONS}]"
        print(f"\n  {tag} Iterazione {i}/{TOTAL_ITERATIONS} – {label}")

        try:
            timings = _run_cold_iteration(query_id, save_output=not is_warmup)
        except Exception as exc:
            print(f"  [ERRORE] Iterazione {i} fallita: {exc}")
            continue

        print(
            f"         spark_start={timings.get('spark_start_s', '?'):.2f}s  |  "
            f"total_s={timings.get('total_s', '?'):.2f}s  |  "
            f"output_s={timings.get('output_s', 0.0):.2f}s  |  "
            f"wall_total={timings.get('wall_total_s', '?'):.2f}s"
        )

        if not is_warmup:
            collected_timings.append(timings)

    if not collected_timings:
        print(f"\n  [WARN] Nessuna iterazione valida raccolta per {label}.")
        return [], {}

    all_phases = []
    seen = set()
    for t in collected_timings:
        for k in t:
            if k not in seen:
                all_phases.append(k)
                seen.add(k)

    stats_per_phase = {}
    for phase in all_phases:
        values = [t[phase] for t in collected_timings if phase in t]
        if values:
            stats_per_phase[phase] = compute_stats(values)

    print(f"\n  {'─'*68}")
    print(f"  STATISTICHE COLD START {label}  (su {len(collected_timings)} iterazioni valide)")
    print(f"  {'─'*68}")
    print(f"  {'Fase':<32} {'Media':>8} {'Std':>8} {'Mediana':>8} {'Min':>8} {'Max':>8}")
    print(f"  {'─'*68}")
    for phase, s in stats_per_phase.items():
        print(f"  {phase:<32} {s['mean_s']:>8.3f} {s['std_s']:>8.3f} "
              f"{s['median_s']:>8.3f} {s['min_s']:>8.3f} {s['max_s']:>8.3f}")
    print(f"  {'─'*68}")

    return collected_timings, stats_per_phase


# ─────────────────────────────────────────────────────────────────────────────
# Salvataggio report CSV
# ─────────────────────────────────────────────────────────────────────────────

def save_benchmark_report(all_results):
    os.makedirs(os.path.dirname(BENCHMARK_REPORT_PATH), exist_ok=True)

    rows = []
    for query_id, (_, stats_per_phase) in all_results.items():
        for phase, s in stats_per_phase.items():
            rows.append({
                "query":    f"Q{query_id}",
                "phase":    phase,
                "n":        s["n"],
                "mean_s":   s["mean_s"],
                "std_s":    s["std_s"],
                "median_s": s["median_s"],
                "min_s":    s["min_s"],
                "max_s":    s["max_s"],
            })

    fieldnames = ["query", "phase", "n", "mean_s", "std_s", "median_s", "min_s", "max_s"]

    with open(BENCHMARK_REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[✓] Report cold start salvato in: {BENCHMARK_REPORT_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# Riepilogo finale
# ─────────────────────────────────────────────────────────────────────────────

def print_final_summary(all_results):
    sep = "=" * 72
    print(f"\n\n{sep}")
    print("  RIEPILOGO FINALE – COLD START BENCHMARK")
    print(f"  Warm-up escluse: {WARMUP_ITERATIONS}  |  "
          f"Iterazioni valide per query: {TOTAL_ITERATIONS - WARMUP_ITERATIONS}")
    print(sep)

    for query_id, (collected, stats_per_phase) in all_results.items():
        if not collected:
            print(f"\n  Q{query_id}: nessun dato raccolto")
            continue

        wall    = stats_per_phase.get("wall_total_s")
        total   = stats_per_phase.get("total_s")
        out     = stats_per_phase.get("output_s")
        startup = stats_per_phase.get("spark_start_s")

        print(f"\n  ── Query {query_id} ──────────────────────────────────────────")
        if startup:
            print(f"     Spark startup:       "
                  f"media={startup['mean_s']:.3f}s  std={startup['std_s']:.3f}s  "
                  f"[{startup['min_s']:.3f}s – {startup['max_s']:.3f}s]")
        if total:
            print(f"     Loading+Computation: "
                  f"media={total['mean_s']:.3f}s  std={total['std_s']:.3f}s  "
                  f"[{total['min_s']:.3f}s – {total['max_s']:.3f}s]")
        if out:
            print(f"     Solo output:         "
                  f"media={out['mean_s']:.3f}s  std={out['std_s']:.3f}s  "
                  f"[{out['min_s']:.3f}s – {out['max_s']:.3f}s]")
        if wall:
            print(f"     End-to-end (wall):   "
                  f"media={wall['mean_s']:.3f}s  std={wall['std_s']:.3f}s  "
                  f"[{wall['min_s']:.3f}s – {wall['max_s']:.3f}s]")

    print(f"\n{sep}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    assert TOTAL_ITERATIONS > WARMUP_ITERATIONS, (
        f"TOTAL_ITERATIONS ({TOTAL_ITERATIONS}) deve essere > "
        f"WARMUP_ITERATIONS ({WARMUP_ITERATIONS})"
    )

    print("=" * 72)
    print("  SABD Project 1 – Cold Start Benchmark")
    print(f"  Query da eseguire:  {QUERIES_TO_RUN}")
    print(f"  Iterazioni totali:  {TOTAL_ITERATIONS}")
    print(f"  Warm-up (escluse):  {WARMUP_ITERATIONS}")
    print(f"  Iterazioni valide:  {TOTAL_ITERATIONS - WARMUP_ITERATIONS}")
    print(f"  SparkSession riavviata a ogni iterazione")
    print("=" * 72)

    benchmark_start = time.time()
    all_results = {}

    for query_id in QUERIES_TO_RUN:
        collected, stats = benchmark_query_cold(query_id)
        all_results[query_id] = (collected, stats)

    total_benchmark_time = time.time() - benchmark_start

    print_final_summary(all_results)
    save_benchmark_report(all_results)

    print(f"Durata totale benchmark: {total_benchmark_time:.1f}s  "
          f"({total_benchmark_time / 60:.1f} minuti)")
    print("[✓] Cold start benchmark completato.\n")


if __name__ == "__main__":
    main()