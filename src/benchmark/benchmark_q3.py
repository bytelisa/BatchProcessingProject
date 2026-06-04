"""
benchmark_q3.py
───────────────
Benchmark comparativo Query 3: DataFrame+percentile_approx vs RDD+t-digest.

Configurazione:
  - 20 iterazioni totali per implementazione
  -  5 iterazioni di warm-up escluse dalla statistica
  - 15 iterazioni valide

Fasi misurate per ogni iterazione:
  loading_s                  — lettura Parquet + (per RDD) conversione in RDD
  computation_percentiles_s  — calcolo P25/P50/P75/P90
  computation_minmax_s       — calcolo min/max globali
  output_s                   — scrittura CSV locale + HDFS
  end_to_end_s               — tempo wall-clock totale (tutte le fasi)

Output:
  /opt/output/benchmark_q3_report.csv   — report CSV con media, std, min, max
"""

import csv
import math
import os
import sys
import time

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

TOTAL_ITERATIONS  = 20
WARMUP_ITERATIONS =  5

PRINT_PREVIEW = False   # disabilitato per non inquinare i tempi

BENCHMARK_REPORT_PATH = "/opt/output/benchmark_q3_report.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Import
# ─────────────────────────────────────────────────────────────────────────────

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from utils      import get_spark_session
from query3     import run_query3        # DataFrame + percentile_approx
from query3_rdd import run_query3_rdd    # RDD + t-digest


# ─────────────────────────────────────────────────────────────────────────────
# Helpers statistici
# ─────────────────────────────────────────────────────────────────────────────

def _mean(v):
    return sum(v) / len(v)

def _median(v):
    s = sorted(v); n = len(s); m = n // 2
    return s[m] if n % 2 else (s[m-1] + s[m]) / 2.0

def _std(v):
    if len(v) < 2:
        return 0.0
    m = _mean(v)
    return math.sqrt(sum((x - m) ** 2 for x in v) / (len(v) - 1))

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
# Singola iterazione
# ─────────────────────────────────────────────────────────────────────────────

def _run(impl, spark):
    wall_t0 = time.time()
    if impl == "df":
        _, _, timings = run_query3(spark, save_output=True,
                                   print_preview=PRINT_PREVIEW)
    else:
        _, _, timings = run_query3_rdd(spark, save_output=True,
                                       print_preview=PRINT_PREVIEW)
    timings["end_to_end_s"] = round(time.time() - wall_t0, 3)
    return timings


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark di una implementazione
# ─────────────────────────────────────────────────────────────────────────────

IMPL_LABEL = {
    "df":  "DataFrame + percentile_approx (Greenwald-Khanna)",
    "rdd": "RDD + t-digest (delta=0.01)",
}

def benchmark_one(impl, spark):
    label      = IMPL_LABEL[impl]
    valid_runs = TOTAL_ITERATIONS - WARMUP_ITERATIONS
    sep        = "─" * 72

    print(f"\n{sep}")
    print(f"  {label}")
    print(f"  Iterazioni totali: {TOTAL_ITERATIONS}  "
          f"(warm-up: {WARMUP_ITERATIONS}, valide: {valid_runs})")
    print(sep)

    collected   = []
    valid_count = 0

    for i in range(1, TOTAL_ITERATIONS + 1):
        is_warmup = i <= WARMUP_ITERATIONS
        tag = "[WARM-UP]" if is_warmup else f"[{valid_count+1:>2}/{valid_runs}]"
        print(f"\n  {tag} Iterazione {i}/{TOTAL_ITERATIONS}")

        try:
            timings = _run(impl, spark)
        except Exception as exc:
            print(f"  [ERRORE] {exc}")
            continue

        e2e = timings["end_to_end_s"]
        lod = timings.get("loading_s", 0)
        pct = timings.get("computation_percentiles_s", 0)
        mm  = timings.get("computation_minmax_s", 0)
        out = timings.get("output_s", 0)
        print(f"    end_to_end={e2e:.2f}s  loading={lod:.2f}s  "
              f"percentili={pct:.2f}s  minmax={mm:.2f}s  output={out:.2f}s")

        if not is_warmup:
            valid_count += 1
            collected.append(timings)

    if not collected:
        print(f"  [WARN] Nessuna iterazione valida.")
        return [], {}

    # Statistiche per fase
    EXCLUDE = {"total_s"}
    all_phases, seen = [], set()
    for t in collected:
        for k in t:
            if k not in seen and k not in EXCLUDE:
                all_phases.append(k)
                seen.add(k)

    stats = {}
    for phase in all_phases:
        vals = [t[phase] for t in collected if phase in t]
        if vals:
            stats[phase] = compute_stats(vals)

    # Tabella riepilogativa
    print(f"\n  {'─'*68}")
    print(f"  STATISTICHE — {label}")
    print(f"  {'─'*68}")
    print(f"  {'Fase':<34} {'Media':>8} {'Std':>8} {'Min':>8} {'Max':>8}")
    print(f"  {'─'*68}")
    for phase, s in stats.items():
        marker = " ◄" if phase == "end_to_end_s" else ""
        print(f"  {phase:<34} {s['mean_s']:>8.3f} {s['std_s']:>8.3f} "
              f"{s['min_s']:>8.3f} {s['max_s']:>8.3f}{marker}")
    print(f"  {'─'*68}")

    return collected, stats


# ─────────────────────────────────────────────────────────────────────────────
# Confronto finale
# ─────────────────────────────────────────────────────────────────────────────

def print_comparison(df_stats, rdd_stats):
    sep = "=" * 72
    valid = TOTAL_ITERATIONS - WARMUP_ITERATIONS
    print(f"\n\n{sep}")
    print("  CONFRONTO Q3: DataFrame vs RDD")
    print(f"  Warm-up: {WARMUP_ITERATIONS}  |  Iterazioni valide: {valid}")
    print(sep)

    PHASES = [
        "loading_s",
        "computation_percentiles_s",
        "computation_minmax_s",
        "output_s",
        "end_to_end_s",
    ]

    print(f"\n  {'Fase':<34} {'DF media':>10} {'RDD media':>10} "
          f"{'Delta':>10} {'Speedup':>10}")
    print(f"  {'─'*68}")

    for phase in PHASES:
        if phase not in df_stats or phase not in rdd_stats:
            continue
        df_mean  = df_stats[phase]["mean_s"]
        rdd_mean = rdd_stats[phase]["mean_s"]
        delta    = rdd_mean - df_mean
        speedup  = df_mean / rdd_mean if rdd_mean > 0 else float("inf")
        winner   = "← DF" if delta > 0 else "← RDD"
        marker   = " ◄" if phase == "end_to_end_s" else ""
        print(f"  {phase:<34} {df_mean:>9.3f}s {rdd_mean:>9.3f}s "
              f"{delta:>+9.3f}s {speedup:>9.2f}x  {winner}{marker}")

    print(f"  {'─'*68}")
    print(f"\n  DF  = DataFrame + F.percentile_approx (Greenwald-Khanna, Spark native)")
    print(f"  RDD = RDD + t-digest (aggregateByKey + merge sul driver, delta=0.01)")
    print(f"  ◄   = metrica end-to-end richiesta dalla specifica")
    print(f"\n{sep}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Salvataggio report CSV
# ─────────────────────────────────────────────────────────────────────────────

def save_report(df_stats, rdd_stats):
    os.makedirs(os.path.dirname(BENCHMARK_REPORT_PATH), exist_ok=True)

    rows = []
    for impl, stats in [("df", df_stats), ("rdd", rdd_stats)]:
        algo = ("percentile_approx (Greenwald-Khanna)" if impl == "df"
                else "t-digest (delta=0.01)")
        for phase, s in stats.items():
            note = ("includes_output" if phase in ("output_s", "end_to_end_s")
                    else "computation_only")
            rows.append({
                "query":     "Q3",
                "impl":      impl,
                "algorithm": algo,
                "phase":     phase,
                "n":         s["n"],
                "mean_s":    s["mean_s"],
                "std_s":     s["std_s"],
                "median_s":  s["median_s"],
                "min_s":     s["min_s"],
                "max_s":     s["max_s"],
                "note":      note,
            })

    fieldnames = ["query", "impl", "algorithm", "phase", "n",
                  "mean_s", "std_s", "median_s", "min_s", "max_s", "note"]

    with open(BENCHMARK_REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[✓] Report salvato in: {BENCHMARK_REPORT_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    valid = TOTAL_ITERATIONS - WARMUP_ITERATIONS
    print("=" * 72)
    print("  SABD Project 1 — Benchmark Query 3: DataFrame vs RDD")
    print(f"  Iterazioni totali:  {TOTAL_ITERATIONS}  "
          f"(warm-up: {WARMUP_ITERATIONS}, valide: {valid})")
    print(f"  DF  → F.percentile_approx  (Greenwald-Khanna, Spark native)")
    print(f"  RDD → t-digest             (aggregateByKey + merge sul driver)")
    print("=" * 72)

    spark = get_spark_session("SABD-Benchmark-Q3")
    benchmark_start = time.time()

    try:
        _, df_stats  = benchmark_one("df",  spark)
        _, rdd_stats = benchmark_one("rdd", spark)
    finally:
        spark.stop()

    total_time = time.time() - benchmark_start

    print_comparison(df_stats, rdd_stats)
    save_report(df_stats, rdd_stats)

    print(f"Durata totale benchmark: {total_time:.1f}s  ({total_time/60:.1f} min)")
    print("[✓] Benchmark Q3 completato.\n")


if __name__ == "__main__":
    main()