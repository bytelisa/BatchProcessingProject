"""
benchmark_rdd_vs_df.py
──────────────────────
Benchmark comparativo DataFrame vs RDD per le Query 1 e 3.

Esegue ciascuna implementazione (DataFrame e RDD) per un numero
configurabile di iterazioni, escludendo le prime N di warm-up.

Scelte metodologiche:
  - La scrittura CSV avviene a OGNI iterazione valida (non solo l'ultima):
    questo permette di calcolare media e std su end_to_end_s (loading +
    computation + output) con la stessa solidità statistica delle altre fasi.
    È coerente con la richiesta della specifica: "run multiple iterations
    and report average performance".

  - Vengono riportate statistiche su TUTTE le fasi:
      loading_s, computation_s (e varianti), output_s, end_to_end_s
    Il campo "note" nel CSV distingue le fasi per il report.

  - Le stampe di anteprima a console sono disabilitate durante il benchmark
    (PRINT_PREVIEW = False) per non inquinare i tempi misurati.

Confronto Q3 (DataFrame vs RDD):
  - DataFrame (query3.py):  usa F.percentile_approx (Greenwald-Khanna sketch,
    nativo Spark). Lo sketch è costruito interamente dagli executor, senza
    collect di dati intermedi.
  - RDD (query3_rdd.py):    usa t-digest (algoritmo a centroidi con densità
    variabile). Ogni partizione costruisce un TDigest locale; i digest (oggetti
    compatti) vengono poi fusi sul driver via merge(). Il collect porta solo
    i digest, non i dati raw, ma introduce comunque un overhead misurabile.

Utilizzo:
    ./run.sh benchmark_rdd_vs_df.py
"""

import csv
import math
import os
import sys
import time

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

from benchmark_config import load_benchmark_config

CFG = load_benchmark_config()

TOTAL_ITERATIONS = int(CFG.get("total_iterations", 20))
WARMUP_ITERATIONS = int(CFG.get("warmup_iterations", 5))

QUERIES_TO_RUN = CFG.get("queries_to_run", [1, 2, 3])
IMPLEMENTATIONS_TO_RUN = CFG.get("implementations", ["df", "rdd"])

PRINT_PREVIEW = bool(CFG.get("print_preview", False))

WORKER_COUNT = int(os.getenv("SPARK_WORKER_COUNT", "0"))

REPORT_DIR = CFG.get("report_dir", "/opt/output/benchmarks")
BENCHMARK_REPORT_PATH = os.path.join(
    REPORT_DIR,
    "benchmark_rdd_vs_df_workers_" + str(WORKER_COUNT) + ".csv"
)

BENCHMARK_RAW_PATH = os.path.join(
    REPORT_DIR,
    "benchmark_rdd_vs_df_raw_workers_" + str(WORKER_COUNT) + ".csv"
)
# ─────────────────────────────────────────────────────────────────────────────
# Import runner delle query
# ─────────────────────────────────────────────────────────────────────────────

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from utils import get_spark_session

if 1 in QUERIES_TO_RUN:
    from query1     import run_query1
    from query1_rdd import run_query1_rdd

if 2 in QUERIES_TO_RUN:
    from query2     import run_query2
    from query2_rdd import run_query2_rdd

if 3 in QUERIES_TO_RUN:
    from query3     import run_query3       # DataFrame + percentile_approx (Greenwald-Khanna)
    from query3_rdd import run_query3_rdd   # RDD + t-digest


# ─────────────────────────────────────────────────────────────────────────────
# Helpers statistici
# ─────────────────────────────────────────────────────────────────────────────

def _mean(v):   return sum(v) / len(v)
def _median(v):
    s = sorted(v); n = len(s); m = n // 2
    return s[m] if n % 2 else (s[m-1] + s[m]) / 2.0
def _std(v):
    if len(v) < 2: return 0.0
    m = _mean(v)
    return math.sqrt(sum((x - m)**2 for x in v) / (len(v) - 1))

def compute_stats(values):
    return {
        "n":        len(values),
        "mean_s":   round(_mean(values),   3),
        "std_s":    round(_std(values),    3),
        "median_s": round(_median(values), 3),
        "min_s":    round(min(values),     3),
        "max_s":    round(max(values),     3),
    }

def get_processing_phase_for_query(query_id, timings):
    """
    Restituisce la fase principale di computazione da usare nel raw report.

    Q1/Q2:
      computation_s

    Q3:
      computation_percentiles_s, perché è la fase più significativa
      della query sui percentili.
    """
    if query_id in (1, 2):
        return "computation_s"

    if query_id == 3:
        if "computation_percentiles_s" in timings:
            return "computation_percentiles_s"
        if "computation_s" in timings:
            return "computation_s"

    if "computation_s" in timings:
        return "computation_s"

    return None

# ─────────────────────────────────────────────────────────────────────────────
# Labels descrittivi per il report
# ─────────────────────────────────────────────────────────────────────────────

IMPL_LABELS = {
    (1, "df"):  "Q1 DataFrame (groupBy+agg)",
    (1, "rdd"): "Q1 RDD (combineByKey)",

    (2, "df"):  "Q2 DataFrame (groupBy+agg)",
    (2, "rdd"): "Q2 RDD (combineByKey)",

    (3, "df"):  "Q3 DataFrame (percentile_approx / Greenwald-Khanna)",
    (3, "rdd"): "Q3 RDD (t-digest, aggregateByKey)",
}


# ─────────────────────────────────────────────────────────────────────────────
# Dispatch
# ─────────────────────────────────────────────────────────────────────────────

def _run(query_id, impl, spark):
    """
    Esegue una singola iterazione con save_output=True e print_preview=False.
    Restituisce (timings, end_to_end_s).

    end_to_end_s è il tempo wall-clock totale misurato esternamente:
    include loading + computation + output + qualsiasi overhead JVM/shuffle.
    timings contiene invece i tempi misurati internamente per fase.
    """
    wall_t0 = time.time()

    if query_id == 1 and impl == "df":
        _, timings = run_query1(
            spark,
            save_output=True,
            print_preview=PRINT_PREVIEW,
        )

    elif query_id == 1 and impl == "rdd":
        _, timings = run_query1_rdd(
            spark,
            save_output=True,
            print_preview=PRINT_PREVIEW,

        )


    elif query_id == 2 and impl == "df":

        _, _, timings = run_query2(

            spark,

            save_output=True,

            print_preview=PRINT_PREVIEW,

        )
        print("[DEBUG Q2 DF timings]", timings)

    elif query_id == 2 and impl == "rdd":

        _, _, timings = run_query2_rdd(

            spark,

            save_output=True,

            print_preview=PRINT_PREVIEW,

        )

    elif query_id == 3 and impl == "df":
        _, _, timings = run_query3(
            spark,
            save_output=True,
            print_preview=PRINT_PREVIEW,
        )

    elif query_id == 3 and impl == "rdd":
        _, _, timings = run_query3_rdd(
            spark,
            save_output=True,
            print_preview=PRINT_PREVIEW,
        )

    else:
        raise ValueError(f"Combinazione non supportata: query={query_id}, impl={impl}")
    end_to_end_s = round(time.time() - wall_t0, 3)
    return timings, end_to_end_s


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark di una singola (query, impl)
# ─────────────────────────────────────────────────────────────────────────────

def benchmark_one(query_id, impl, spark):
    """
    Esegue TOTAL_ITERATIONS iterazioni di (query_id, impl).

    Ogni iterazione valida (post warm-up):
      - scrive i CSV su HDFS (save_output=True sempre)
      - registra i timings per fase + end_to_end_s

    Restituisce (collected_timings, stats_per_phase).
    collected_timings include anche end_to_end_s come campo aggiuntivo.
    """
    label      = IMPL_LABELS.get((query_id, impl), f"Q{query_id} [{impl.upper()}]")
    sep        = "─" * 72
    valid_runs = TOTAL_ITERATIONS - WARMUP_ITERATIONS

    print(f"\n{sep}")
    print(f"  BENCHMARK {label}")
    print(f"  Iterazioni totali:   {TOTAL_ITERATIONS}")
    print(f"  Warm-up (escluse):   {WARMUP_ITERATIONS}")
    print(f"  Iterazioni valide:   {valid_runs}")
    print(f"  Scrittura CSV:       ogni iterazione valida")
    print(f"  Preview a console:   {'abilitata' if PRINT_PREVIEW else 'disabilitata'}")
    print(sep)

    collected = []
    raw_rows = []
    valid_count = 0

    for i in range(1, TOTAL_ITERATIONS + 1):
        is_warmup = i <= WARMUP_ITERATIONS

        tag = "[WARM-UP]" if is_warmup else f"[{valid_count+1:>2}/{valid_runs}]"
        print(f"\n  {tag} Iterazione {i}/{TOTAL_ITERATIONS} – {label}")

        try:
            timings, end_to_end_s = _run(query_id, impl, spark)
        except Exception as exc:
            print(f"  [ERRORE] Iterazione {i} fallita: {exc}")
            raise

        # Aggiunge end_to_end_s al dict timings per trattarlo come le altre fasi
        timings["end_to_end_s"] = end_to_end_s

        # Stampa riassunto compatto
        comp_key = next(
            (k for k in ["computation_s", "computation_percentiles_s"] if k in timings),
            None
        )
        comp_val = f"{timings[comp_key]:.2f}s" if comp_key else "n/a"
        print(f"         end_to_end={end_to_end_s:.2f}s  |  "
              f"loading={timings.get('loading_s', 0):.2f}s  |  "
              f"computation={comp_val}  |  "
              f"output={timings.get('output_s', 0):.2f}s")

        if not is_warmup:
            valid_count += 1
            collected.append(timings)

            # Raw timing per box plot: end-to-end
            if "end_to_end_s" in timings:
                raw_rows.append({
                    "worker_count": WORKER_COUNT,
                    "query": "Q" + str(query_id),
                    "impl": impl,
                    "iteration": valid_count,
                    "phase": "end_to_end_s",
                    "time_s": timings["end_to_end_s"],
                })

            # Raw timing per box plot: processing principale
            processing_phase = get_processing_phase_for_query(query_id, timings)

            if processing_phase is not None and processing_phase in timings:
                raw_rows.append({
                    "worker_count": WORKER_COUNT,
                    "query": "Q" + str(query_id),
                    "impl": impl,
                    "iteration": valid_count,
                    "phase": "processing_s",
                    "time_s": timings[processing_phase],
                })

    if not collected:
        print(f"\n  [WARN] Nessuna iterazione valida per {label}.")
        return [], {}

    # ── Statistiche per tutte le fasi incluso end_to_end_s ──────────────────
    # Esclude solo total_s (ridondante: è la somma delle fasi interne)
    EXCLUDE = {"total_s"}

    all_phases = []
    seen = set()
    for t in collected:
        for k in t:
            if k not in seen and k not in EXCLUDE:
                all_phases.append(k)
                seen.add(k)

    stats_per_phase = {}
    for phase in all_phases:
        values = [t[phase] for t in collected if phase in t]
        if values:
            stats_per_phase[phase] = compute_stats(values)

    # ── Tabella riepilogativa ────────────────────────────────────────────────
    print(f"\n  {'─'*68}")
    print(f"  STATISTICHE {label}  ({len(collected)} iterazioni valide)")
    print(f"  {'─'*68}")
    print(f"  {'Fase':<32} {'Media':>8} {'Std':>8} {'Mediana':>8} {'Min':>8} {'Max':>8}")
    print(f"  {'─'*68}")
    for phase, s in stats_per_phase.items():
        marker = " ◄" if phase == "end_to_end_s" else ""
        print(f"  {phase:<32} {s['mean_s']:>8.3f} {s['std_s']:>8.3f} "
              f"{s['median_s']:>8.3f} {s['min_s']:>8.3f} {s['max_s']:>8.3f}{marker}")
    print(f"  {'─'*68}")
    print(f"  ◄ = tempo end-to-end richiesto dalla specifica (include output CSV)")

    return collected, stats_per_phase, raw_rows


# ─────────────────────────────────────────────────────────────────────────────
# Confronto DF vs RDD
# ─────────────────────────────────────────────────────────────────────────────
def print_comparison(all_results):
    sep = "=" * 72
    print(f"\n\n{sep}")
    print("  CONFRONTO IMPLEMENTAZIONI")
    print(f"  Spark workers: {WORKER_COUNT}")
    print(f"  Warm-up escluse: {WARMUP_ITERATIONS}  |  "
          f"Iterazioni valide: {TOTAL_ITERATIONS - WARMUP_ITERATIONS}")
    print(f"  Implementazioni eseguite: {IMPLEMENTATIONS_TO_RUN}")
    print(f"  Tutte le fasi includono scrittura CSV (end-to-end reale)")
    print(sep)

    # Ordine di visualizzazione delle fasi nel confronto
    COMPARE_PHASES = [
        "loading_s",
        "filtering_s",
        "computation_s",
        "computation_percentiles_s",
        "computation_minmax_s",
        "output_s",
        "end_to_end_s",
    ]

    for query_id in QUERIES_TO_RUN:
        available_impls = [
            impl for impl in IMPLEMENTATIONS_TO_RUN
            if (query_id, impl) in all_results
        ]

        if not available_impls:
            continue

        print(f"\n  ── Query {query_id} ──────────────────────────────────────────────")

        for impl in available_impls:
            label = IMPL_LABELS.get((query_id, impl), impl.upper())
            print(f"  {impl.upper()}: {label}")

        # Caso classico: confronto DF vs RDD
        if "df" in available_impls and "rdd" in available_impls:
            _, df_stats = all_results[(query_id, "df")]
            _, rdd_stats = all_results[(query_id, "rdd")]

            print(f"\n  {'Fase':<34} {'DF media':>10} {'RDD media':>10} "
                  f"{'Delta':>10} {'Speedup':>10}")
            print(f"  {'─' * 68}")

            for phase in COMPARE_PHASES:
                if phase not in df_stats or phase not in rdd_stats:
                    continue

                df_mean = df_stats[phase]["mean_s"]
                rdd_mean = rdd_stats[phase]["mean_s"]

                delta = rdd_mean - df_mean
                speedup = df_mean / rdd_mean if rdd_mean > 0 else float("inf")

                if delta > 0:
                    winner = "← DF"
                elif delta < 0:
                    winner = "← RDD"
                else:
                    winner = "← pari"

                marker = " ◄" if phase == "end_to_end_s" else ""

                print(f"  {phase:<34} {df_mean:>9.3f}s {rdd_mean:>9.3f}s "
                      f"{delta:>+9.3f}s {speedup:>9.2f}x  {winner}{marker}")

            print(f"  {'─' * 68}")

        # Caso non comparativo: è stata eseguita una sola implementazione
        else:
            impl = available_impls[0]
            _, stats = all_results[(query_id, impl)]

            print(f"\n  {'Fase':<34} {impl.upper() + ' media':>12} "
                  f"{'Std':>8} {'Min':>8} {'Max':>8}")
            print(f"  {'─' * 68}")

            for phase in COMPARE_PHASES:
                if phase not in stats:
                    continue

                s = stats[phase]
                marker = " ◄" if phase == "end_to_end_s" else ""

                print(f"  {phase:<34} {s['mean_s']:>11.3f}s "
                      f"{s['std_s']:>7.3f}s {s['min_s']:>7.3f}s "
                      f"{s['max_s']:>7.3f}s{marker}")

            print(f"  {'─' * 68}")

    print(f"\n{sep}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Salvataggio report CSV
# ─────────────────────────────────────────────────────────────────────────────

def save_report(all_results):
    """
    Scrive il report CSV con una riga per ogni (query, impl, phase).
    La colonna 'note' indica se la fase include output CSV o meno.
    La colonna 'algorithm' documenta la tecnica usata per i percentili (Q3).
    """
    os.makedirs(os.path.dirname(BENCHMARK_REPORT_PATH), exist_ok=True)

    ALGO_NOTE = {
        (1, "df"): "groupBy+agg",
        (1, "rdd"): "combineByKey",

        (2, "df"): "groupBy+agg",
        (2, "rdd"): "combineByKey",

        (3, "df"): "percentile_approx (Greenwald-Khanna, Spark native)",
        (3, "rdd"): "t-digest (aggregateByKey, merge sul driver)",
    }

    rows = []
    for (query_id, impl), (_, stats_per_phase) in all_results.items():
        algo = ALGO_NOTE.get((query_id, impl), "")
        for phase, s in stats_per_phase.items():
            note = "includes_output" if phase in ("output_s", "end_to_end_s") \
                   else "computation_only"
            rows.append({
                "worker_count": WORKER_COUNT,
                "query": f"Q{query_id}",
                "impl": impl,
                "algorithm": algo,
                "phase": phase,
                "n": s["n"],
                "mean_s": s["mean_s"],
                "std_s": s["std_s"],
                "median_s": s["median_s"],
                "min_s": s["min_s"],
                "max_s": s["max_s"],
                "total_iterations": TOTAL_ITERATIONS,
                "warmup_iterations": WARMUP_ITERATIONS,
                "valid_iterations": TOTAL_ITERATIONS - WARMUP_ITERATIONS,
                "note": note,
            })

    fieldnames = [
        "worker_count",
        "query",
        "impl",
        "algorithm",
        "phase",
        "n",
        "mean_s",
        "std_s",
        "median_s",
        "min_s",
        "max_s",
        "total_iterations",
        "warmup_iterations",
        "valid_iterations",
        "note",
    ]

    with open(BENCHMARK_REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[✓] Report salvato in: {BENCHMARK_REPORT_PATH}")

def save_raw_report(raw_rows):
    """
    Salva i tempi grezzi delle iterazioni valide.

    Questo file serve per costruire box plot in Grafana.
    Contiene solo:
      - end_to_end_s
      - processing_s

    Non include le iterazioni di warm-up.
    """
    os.makedirs(os.path.dirname(BENCHMARK_RAW_PATH), exist_ok=True)

    fieldnames = [
        "worker_count",
        "query",
        "impl",
        "iteration",
        "phase",
        "time_s",
    ]

    with open(BENCHMARK_RAW_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(raw_rows)

    print(f"\n[✓] Report raw salvato in: {BENCHMARK_RAW_PATH}")

# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    assert TOTAL_ITERATIONS > WARMUP_ITERATIONS, (
        f"TOTAL_ITERATIONS ({TOTAL_ITERATIONS}) deve essere > "
        f"WARMUP_ITERATIONS ({WARMUP_ITERATIONS})"
    )

    valid = TOTAL_ITERATIONS - WARMUP_ITERATIONS
    print("=" * 72)
    print("  SABD Project 1 – Benchmark DataFrame vs RDD")
    print(f"  Query:              {QUERIES_TO_RUN}")
    print(f"  Spark workers:      {WORKER_COUNT}")
    print(f"  Iterazioni totali:  {TOTAL_ITERATIONS}  "
          f"(warm-up: {WARMUP_ITERATIONS}, valide: {valid})")
    print(f"  Scrittura CSV:      ogni iterazione valida")
    print(f"  Fasi misurate:      loading, computation, output, end_to_end")
    print()
    print("  Q3 — confronto algoritmi:")
    print("    DataFrame → F.percentile_approx  (Greenwald-Khanna sketch, Spark native)")
    print("    RDD       → t-digest              (centroidi, aggregateByKey + merge)")
    print("=" * 72)

    spark = get_spark_session("SABD-Benchmark-RDD-vs-DF")

    benchmark_start = time.time()
    all_results = {}   # { (query_id, impl): (collected_timings, stats_per_phase) }
    all_raw_rows = []

    try:
        for query_id in QUERIES_TO_RUN:
            for impl in IMPLEMENTATIONS_TO_RUN:
                collected, stats, raw_rows = benchmark_one(query_id, impl, spark)
                all_results[(query_id, impl)] = (collected, stats)
                all_raw_rows.extend(raw_rows)
    finally:
        spark.stop()

    total_time = time.time() - benchmark_start

    print_comparison(all_results)
    save_report(all_results)
    save_raw_report(all_raw_rows)

    print(f"Durata totale benchmark: {total_time:.1f}s  ({total_time/60:.1f} minuti)")
    print("[✓] Benchmark completato.\n")


if __name__ == "__main__":
    main()