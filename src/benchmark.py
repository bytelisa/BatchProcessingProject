"""
Benchmark delle Query Spark

Esegue ciascuna query per un numero configurabile di iterazioni,
escludendo le prime N iterazioni di warm-up dalla raccolta dati.
Alla fine stampa e salva un report CSV con le statistiche aggregate
(media, mediana, deviazione standard, min, max) per ogni fase di ogni query.

Utilizzo (dall'esterno del container):
    ./run.sh benchmark.py

Oppure direttamente inside il container spark-master:
    spark-submit --master spark://spark-master:7077 /opt/scripts/benchmark.py

Parametri configurabili (sezione CONFIG più in basso):
    TOTAL_ITERATIONS  – numero totale di esecuzioni per query  (default: 20)
    WARMUP_ITERATIONS – iterazioni iniziali escluse dai dati   (default:  5)
    QUERIES_TO_RUN    – lista delle query da includere nel benchmark
"""

import time
import math
import csv
import os
import sys

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG – modifica qui se vuoi cambiare i parametri del benchmark
# ─────────────────────────────────────────────────────────────────────────────

TOTAL_ITERATIONS  = 20   # iterazioni totali per query
WARMUP_ITERATIONS =  5   # iterazioni di warm-up (escluse dalla statistica)
QUERIES_TO_RUN    = [4]  # quali query eseguire; rimuovi quelle che non vuoi eseguire

# Dove scrivere il report finale
BENCHMARK_REPORT_PATH = "/opt/results/benchmark_report.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Import query runner
# ─────────────────────────────────────────────────────────────────────────────

# Aggiungiamo la cartella degli script al path (necessario quando
# benchmark.py non viene eseguito dalla stessa directory degli altri moduli)
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from utils import get_spark_session

# Import condizionale: importa solo le query richieste
if 1 in QUERIES_TO_RUN:
    from query1 import run_query1
if 2 in QUERIES_TO_RUN:
    from query2 import run_query2
if 3 in QUERIES_TO_RUN:
    from query3 import run_query3
if 4 in QUERIES_TO_RUN:
    from query3_bis import run_query3_bis


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
    """
    Restituisce un dict con le statistiche principali su una lista di float.
    """
    return {
        "n":      len(values),
        "mean_s": round(_mean(values),   3),
        "std_s":  round(_std(values),    3),
        "median_s": round(_median(values), 3),
        "min_s":  round(min(values),     3),
        "max_s":  round(max(values),     3),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Funzioni di dispatch per ogni query
# ─────────────────────────────────────────────────────────────────────────────

def _run_and_time_query(query_id, spark):
    """
    Chiama la funzione run_queryN corretta e restituisce il dict timings.
    Le singole run_queryN non salvano output durante il benchmark
    (il salvataggio distorce i tempi): per questo usiamo le versioni
    originali ma intercettiamo solo i timing; i CSV vengono scritti
    solo nell'ultima iterazione utile (non warm-up).
    """
    wall_t0 = time.time()

    if query_id == 1:
        _, timings = run_query1(spark)
    elif query_id == 2:
        _, timings = run_query2(spark)
    elif query_id == 3:
        _, _, timings = run_query3(spark)
    elif query_id == 4:
        _, _, timings = run_query3_bis(spark)
    else:
        raise ValueError(f"Query {query_id} non supportata in questo benchmark.")

    timings["wall_total_s"] = round(time.time() - wall_t0, 3)
    return timings


# ─────────────────────────────────────────────────────────────────────────────
# Funzione principale di benchmarking
# ─────────────────────────────────────────────────────────────────────────────

def benchmark_query(query_id, spark):
    """
    Esegue TOTAL_ITERATIONS esecuzioni della query `query_id`.
    Le prime WARMUP_ITERATIONS vengono eseguite ma escluse dalle statistiche.

    Restituisce:
        collected_timings  – lista di dict timings per le iterazioni valide
        stats_per_phase    – dict { phase_name: stats_dict }
    """
    label = f"Query {query_id}"
    separator = "─" * 72

    print(f"\n{separator}")
    print(f"  BENCHMARK {label}")
    print(f"  Iterazioni totali:  {TOTAL_ITERATIONS}")
    print(f"  Warm-up (escluse):  {WARMUP_ITERATIONS}")
    print(f"  Iterazioni valide:  {TOTAL_ITERATIONS - WARMUP_ITERATIONS}")
    print(f"{separator}")

    collected_timings = []   # solo le iterazioni post-warm-up

    for i in range(1, TOTAL_ITERATIONS + 1):
        is_warmup = i <= WARMUP_ITERATIONS
        tag = "[WARM-UP]" if is_warmup else f"[{i - WARMUP_ITERATIONS:>2}/{TOTAL_ITERATIONS - WARMUP_ITERATIONS}]"
        print(f"\n  {tag} Iterazione {i}/{TOTAL_ITERATIONS} – {label}")

        try:
            timings = _run_and_time_query(query_id, spark)
        except Exception as exc:
            print(f"  [ERRORE] Iterazione {i} fallita: {exc}")
            continue

        # Stampa un riassunto compatto dell'iterazione
        print(f"         wall_total = {timings.get('wall_total_s', '?'):.2f}s  |  "
              f"computation = {timings.get('computation_s', timings.get('computation_percentiles_s', '?'))}")

        if not is_warmup:
            collected_timings.append(timings)

    # ── Calcola statistiche per ogni fase presente nei timings ──────────────

    if not collected_timings:
        print(f"\n  [WARN] Nessuna iterazione valida raccolta per {label}.")
        return [], {}

    # Raccoglie tutti i nomi di fase che compaiono almeno in un timing
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

    # ── Stampa tabella riepilogativa ─────────────────────────────────────────

    print(f"\n  {'─'*68}")
    print(f"  STATISTICHE {label}  (su {len(collected_timings)} iterazioni valide)")
    print(f"  {'─'*68}")
    header = f"  {'Fase':<32} {'Media':>8} {'Std':>8} {'Mediana':>8} {'Min':>8} {'Max':>8}"
    print(header)
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
    """
    Scrive un CSV con le statistiche aggregate di tutte le query e fasi.

    Colonne: query, phase, n, mean_s, std_s, median_s, min_s, max_s
    """
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

    print(f"\n[✓] Report benchmark salvato in: {BENCHMARK_REPORT_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# Stampa riepilogo finale comparativo
# ─────────────────────────────────────────────────────────────────────────────

def print_final_summary(all_results):
    sep = "=" * 72
    print(f"\n\n{sep}")
    print("  RIEPILOGO FINALE BENCHMARK")
    print(f"  Warm-up escluse: {WARMUP_ITERATIONS}  |  "
          f"Iterazioni valide per query: {TOTAL_ITERATIONS - WARMUP_ITERATIONS}")
    print(sep)

    for query_id, (collected, stats_per_phase) in all_results.items():
        if not collected:
            print(f"\n  Q{query_id}: nessun dato raccolto")
            continue

        wall_stats = stats_per_phase.get("wall_total_s")
        comp_key   = next(
            (k for k in ["computation_s", "computation_percentiles_s"] if k in stats_per_phase),
            None
        )
        comp_stats = stats_per_phase.get(comp_key) if comp_key else None

        print(f"\n  ── Query {query_id} ──────────────────────────────────────────")
        if wall_stats:
            print(f"     Tempo totale (wall):  "
                  f"media={wall_stats['mean_s']:.3f}s  "
                  f"std={wall_stats['std_s']:.3f}s  "
                  f"[{wall_stats['min_s']:.3f}s – {wall_stats['max_s']:.3f}s]")
        if comp_stats:
            print(f"     Solo computazione:    "
                  f"media={comp_stats['mean_s']:.3f}s  "
                  f"std={comp_stats['std_s']:.3f}s  "
                  f"[{comp_stats['min_s']:.3f}s – {comp_stats['max_s']:.3f}s]")

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
    print("  SABD Project 1 – Benchmark Query Spark")
    print(f"  Query da eseguire:  {QUERIES_TO_RUN}")
    print(f"  Iterazioni totali:  {TOTAL_ITERATIONS}")
    print(f"  Warm-up (escluse):  {WARMUP_ITERATIONS}")
    print(f"  Iterazioni valide:  {TOTAL_ITERATIONS - WARMUP_ITERATIONS}")
    print("=" * 72)

    # Una sola SparkSession riutilizzata per tutte le query e iterazioni.
    # Questo rispecchia le condizioni reali di esecuzione (JVM già avviata).
    spark = get_spark_session("SABD-Benchmark")

    benchmark_start = time.time()
    all_results = {}  # { query_id: (collected_timings, stats_per_phase) }

    try:
        for query_id in QUERIES_TO_RUN:
            collected, stats = benchmark_query(query_id, spark)
            all_results[query_id] = (collected, stats)

    finally:
        spark.stop()

    total_benchmark_time = time.time() - benchmark_start

    print_final_summary(all_results)
    save_benchmark_report(all_results)

    print(f"Durata totale benchmark: {total_benchmark_time:.1f}s  "
          f"({total_benchmark_time / 60:.1f} minuti)")
    print("[✓] Benchmark completato.\n")


if __name__ == "__main__":
    main()