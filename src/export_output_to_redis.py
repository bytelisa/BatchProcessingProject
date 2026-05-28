"""
export_output_to_redis.py
─────────────────────────
Esporta gli output CSV delle query su Redis Stack per Grafana.

Q1:
- mantiene la logica dello script export_q1_to_redis.py
- esporta:
    q1:row:<airline>:<month>
    q1:metric:<metric>:<airline>
    q1:ts:<metric>:<airline>

Q2:
- esporta solo query2_top10_arrival_delay.csv
- usa una logica minimale orientata ai grafici Grafana:
    q2:top10:carriers
    q2:ranking:arrdelay_mean
    q2:metric:num_flights
    q2:cause:<delay_cause_metric>

Q3:
- mantiene la logica dello script export_q3_to_redis.py
- esporta:
    q3:row:hourly:<airline>:<hour>
    q3:metric:<metric>:<airline>
    q3:ts:<metric>:<airline>
    q3:minmax:<airline>
    q3:metric:min_delay
    q3:metric:max_delay
"""

import csv
import os
import sys
from datetime import datetime, timezone, timedelta

import redis


# ─────────────────────────────────────────────
# Config Redis
# ─────────────────────────────────────────────

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None


# ─────────────────────────────────────────────
# CSV paths
# ─────────────────────────────────────────────

Q1_CSV_PATH = os.getenv(
    "Q1_CSV_PATH",
    os.getenv("CSV_PATH", "output/query1_monthly_stats.csv"),
)

Q2_TOP10_CSV_PATH = os.getenv(
    "Q2_TOP10_CSV_PATH",
    "output/query2_top10_arrival_delay.csv",
)

Q3_PERCENTILES_CSV_PATH = os.getenv(
    "Q3_PERCENTILES_CSV_PATH",
    "output/query3_hourly_percentiles.csv",
)

Q3_MINMAX_CSV_PATH = os.getenv(
    "Q3_MINMAX_CSV_PATH",
    "output/query3_global_minmax.csv",
)


# ─────────────────────────────────────────────
# Q1 constants
# ─────────────────────────────────────────────

Q1_METRICS = [
    "dep_delay_mean",
    "dep_delay_min",
    "dep_delay_max",
    "arr_delay_mean",
    "arr_delay_min",
    "arr_delay_max",
    "cancellation_rate",
]

MONTH_TIMESTAMPS_MS = {
    1: int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
    2: int(datetime(2025, 2, 1, tzinfo=timezone.utc).timestamp() * 1000),
    3: int(datetime(2025, 3, 1, tzinfo=timezone.utc).timestamp() * 1000),
    4: int(datetime(2025, 4, 1, tzinfo=timezone.utc).timestamp() * 1000),
}


# ─────────────────────────────────────────────
# Q2 constants
# ─────────────────────────────────────────────

Q2_REQUIRED_COLUMNS = {
    "carrier",
    "num_flights",
    "arrdelay_mean",
    "carrier_delay_mean",
    "weather_delay_mean",
    "nas_delay_mean",
    "security_delay_mean",
    "late_aircraft_delay_mean",
}

Q2_CAUSE_METRICS = [
    "carrier_delay_mean",
    "weather_delay_mean",
    "nas_delay_mean",
    "security_delay_mean",
    "late_aircraft_delay_mean",
]


# ─────────────────────────────────────────────
# Q3 constants
# ─────────────────────────────────────────────

Q3_PERCENTILE_METRICS = [
    "p25",
    "p50",
    "p75",
    "p90",
]

Q3_OTHER_HOURLY_METRICS = [
    "num_flights",
]

Q3_ALL_HOURLY_METRICS = Q3_OTHER_HOURLY_METRICS + Q3_PERCENTILE_METRICS


# ─────────────────────────────────────────────
# Utility generali
# ─────────────────────────────────────────────

def connect_redis() -> redis.Redis:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )

    try:
        r.ping()
    except redis.RedisError as e:
        print(f"[ERROR] Impossibile connettersi a Redis {REDIS_HOST}:{REDIS_PORT}")
        print(e)
        sys.exit(1)

    print(f"[INFO] Connesso a Redis: {REDIS_HOST}:{REDIS_PORT}")
    return r


def require_file(path: str) -> None:
    if not os.path.exists(path):
        print(f"[ERROR] File non trovato: {path}")
        sys.exit(1)


def load_csv_rows(path: str) -> list[dict]:
    require_file(path)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print(f"[ERROR] CSV vuoto: {path}")
        sys.exit(1)

    return rows


def require_columns(rows: list[dict], required_columns: set[str], csv_path: str) -> None:
    found_columns = set(rows[0].keys())
    missing_columns = required_columns - found_columns

    if missing_columns:
        print(f"[ERROR] Colonne mancanti in {csv_path}: {sorted(missing_columns)}")
        print(f"[INFO] Colonne trovate: {list(rows[0].keys())}")
        sys.exit(1)


def delete_keys_by_prefix(r: redis.Redis, prefix: str) -> int:
    keys = list(r.scan_iter(f"{prefix}:*"))
    if keys:
        r.delete(*keys)
    return len(keys)


def to_float(value) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def to_int(value) -> int:
    if value is None or value == "":
        return 0
    return int(float(value))


# ─────────────────────────────────────────────
# Q1 export
# Logica mantenuta dallo script originale
# ─────────────────────────────────────────────

def q1_ts_create_if_needed(pipe, key: str, metric: str, airline: str):
    """
    Crea una TimeSeries se non esiste già.
    DUPLICATE_POLICY LAST permette di sovrascrivere lo stesso timestamp.
    """
    pipe.execute_command(
        "TS.CREATE",
        key,
        "DUPLICATE_POLICY",
        "LAST",
        "LABELS",
        "query",
        "q1",
        "metric",
        metric,
        "airline",
        airline,
    )


def export_q1(r: redis.Redis) -> None:
    print("\n" + "=" * 72)
    print("EXPORT Q1")
    print("=" * 72)

    print(f"[INFO] Lettura CSV Q1: {Q1_CSV_PATH}")

    rows = load_csv_rows(Q1_CSV_PATH)

    required_columns = {"month", "airline", *Q1_METRICS}
    require_columns(rows, required_columns, Q1_CSV_PATH)

    deleted = delete_keys_by_prefix(r, "q1")
    print(f"[INFO] Eliminate {deleted} chiavi q1:* precedenti")

    airlines = sorted({row["airline"] for row in rows})

    pipe = r.pipeline(transaction=False)

    # Crea le serie temporali per ogni metrica/compagnia.
    for airline in airlines:
        for metric in Q1_METRICS:
            ts_key = f"q1:ts:{metric}:{airline}"
            q1_ts_create_if_needed(pipe, ts_key, metric, airline)

    pipe.execute()

    pipe = r.pipeline(transaction=False)
    exported = 0

    for row in rows:
        month = int(row["month"])
        airline = row["airline"]

        if month not in MONTH_TIMESTAMPS_MS:
            raise ValueError(f"Mese non valido per Q1: {month}")

        timestamp_ms = MONTH_TIMESTAMPS_MS[month]

        # Hash per debug/ispezione.
        row_key = f"q1:row:{airline}:{month}"
        pipe.hset(
            row_key,
            mapping={
                "month": month,
                "airline": airline,
                **{metric: float(row[metric]) for metric in Q1_METRICS},
            },
        )
        exported += 1

        # Hash per metrica, ancora utile per debug rapido.
        for metric in Q1_METRICS:
            pipe.hset(
                f"q1:metric:{metric}:{airline}",
                str(month),
                float(row[metric]),
            )
            exported += 1

            # TimeSeries per Grafana.
            pipe.execute_command(
                "TS.ADD",
                f"q1:ts:{metric}:{airline}",
                timestamp_ms,
                float(row[metric]),
                "ON_DUPLICATE",
                "LAST",
            )
            exported += 1

    pipe.execute()

    print("[✓] Q1 esportata su Redis Stack")
    print(f"[INFO] Elementi scritti: {exported}")


# ─────────────────────────────────────────────
# Q2 export
# ─────────────────────────────────────────────

def export_q2(r: redis.Redis) -> None:
    print("\n" + "=" * 72)
    print("EXPORT Q2")
    print("=" * 72)

    print(f"[INFO] Lettura CSV Q2 top 10: {Q2_TOP10_CSV_PATH}")

    rows = load_csv_rows(Q2_TOP10_CSV_PATH)
    require_columns(rows, Q2_REQUIRED_COLUMNS, Q2_TOP10_CSV_PATH)

    deleted = delete_keys_by_prefix(r, "q2")
    print(f"[INFO] Eliminate {deleted} chiavi q2:* precedenti")

    pipe = r.pipeline(transaction=False)
    exported = 0

    for rank, row in enumerate(rows, start=1):
        carrier = row["carrier"]

        arrdelay_mean = to_float(row["arrdelay_mean"])
        num_flights = to_int(row["num_flights"])

        cause_values = {
            "carrier_delay_mean": to_float(row["carrier_delay_mean"]),
            "weather_delay_mean": to_float(row["weather_delay_mean"]),
            "nas_delay_mean": to_float(row["nas_delay_mean"]),
            "security_delay_mean": to_float(row["security_delay_mean"]),
            "late_aircraft_delay_mean": to_float(row["late_aircraft_delay_mean"]),
        }

        total_causes = sum(cause_values.values())

        # ─────────────────────────────────────────
        # Grafico 1: ranking arrdelay medio
        # ─────────────────────────────────────────
        pipe.hset("q2:chart:ranking_arrdelay_mean", carrier, arrdelay_mean)
        exported += 1

        # Metadati utili per ordinamento/controllo
        pipe.hset("q2:chart:rank", carrier, rank)
        exported += 1

        pipe.hset("q2:chart:num_flights", carrier, num_flights)
        exported += 1

        # ─────────────────────────────────────────
        # Grafico 2A: cause assolute
        # ─────────────────────────────────────────
        for metric, value in cause_values.items():
            pipe.hset(f"q2:chart:cause_abs:{metric}", carrier, value)
            exported += 1

        # ─────────────────────────────────────────
        # Grafico 2B: cause percentuali
        # percentuale sul totale delle 5 cause
        # ─────────────────────────────────────────
        for metric, value in cause_values.items():
            pct = 0.0
            if total_causes > 0:
                pct = round((value / total_causes) * 100.0, 4)

            pipe.hset(f"q2:chart:cause_pct:{metric}", carrier, pct)
            exported += 1

    pipe.execute()

    print("[✓] Q2 esportata su Redis Stack")
    print(f"[INFO] Elementi scritti: {exported}")
    print("[INFO] Chiavi Q2 create:")
    print("       q2:chart:ranking_arrdelay_mean")
    print("       q2:chart:rank")
    print("       q2:chart:num_flights")
    print("       q2:chart:cause_abs:carrier_delay_mean")
    print("       q2:chart:cause_abs:weather_delay_mean")
    print("       q2:chart:cause_abs:nas_delay_mean")
    print("       q2:chart:cause_abs:security_delay_mean")
    print("       q2:chart:cause_abs:late_aircraft_delay_mean")
    print("       q2:chart:cause_pct:carrier_delay_mean")
    print("       q2:chart:cause_pct:weather_delay_mean")
    print("       q2:chart:cause_pct:nas_delay_mean")
    print("       q2:chart:cause_pct:security_delay_mean")
    print("       q2:chart:cause_pct:late_aircraft_delay_mean")

# ─────────────────────────────────────────────
# Q3 export
# Logica mantenuta dallo script originale
# ─────────────────────────────────────────────

def q3_hour_to_timestamp_ms(hour: int) -> int:
    """
    Converte l'ora 0-23 in timestamp fittizio.

    Usiamo il giorno 2025-01-01:
      hour=0  -> 2025-01-01 00:00
      hour=1  -> 2025-01-01 01:00
      ...
      hour=23 -> 2025-01-01 23:00

    Questo serve solo per visualizzare in Grafana una curva oraria.
    """
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    ts = base + timedelta(hours=hour)
    return int(ts.timestamp() * 1000)


def q3_create_timeseries_if_needed(pipe, key: str, metric: str, airline: str):
    """
    Crea una serie RedisTimeSeries.

    DUPLICATE_POLICY LAST consente di rieseguire l'export e sovrascrivere
    il valore associato allo stesso timestamp.
    """
    pipe.execute_command(
        "TS.CREATE",
        key,
        "DUPLICATE_POLICY",
        "LAST",
        "LABELS",
        "query",
        "q3",
        "metric",
        metric,
        "airline",
        airline,
    )


def export_q3_hourly_percentiles(r: redis.Redis, rows: list[dict]) -> int:
    required_columns = {
        "airline",
        "hour",
        "num_flights",
        "p25",
        "p50",
        "p75",
        "p90",
    }

    require_columns(rows, required_columns, Q3_PERCENTILES_CSV_PATH)

    airlines = sorted({row["airline"] for row in rows})

    pipe = r.pipeline(transaction=False)

    # Crea una TimeSeries per ogni compagnia e metrica oraria.
    for airline in airlines:
        for metric in Q3_ALL_HOURLY_METRICS:
            ts_key = f"q3:ts:{metric}:{airline}"
            q3_create_timeseries_if_needed(pipe, ts_key, metric, airline)

    pipe.execute()

    pipe = r.pipeline(transaction=False)
    exported = 0

    for row in rows:
        airline = row["airline"]
        hour = int(row["hour"])

        if hour < 0 or hour > 23:
            raise ValueError(f"Ora non valida in Q3: {hour}")

        timestamp_ms = q3_hour_to_timestamp_ms(hour)

        # Hash per debug/ispezione: una riga per compagnia × ora.
        row_key = f"q3:row:hourly:{airline}:{hour}"
        pipe.hset(
            row_key,
            mapping={
                "airline": airline,
                "hour": hour,
                "num_flights": int(row["num_flights"]),
                "p25": float(row["p25"]),
                "p50": float(row["p50"]),
                "p75": float(row["p75"]),
                "p90": float(row["p90"]),
            },
        )
        exported += 1

        # Hash per metrica/compagnia: field=hour, value=metric.
        for metric in Q3_ALL_HOURLY_METRICS:
            value = float(row[metric])

            pipe.hset(
                f"q3:metric:{metric}:{airline}",
                str(hour),
                value,
            )
            exported += 1

            # TimeSeries per Grafana.
            pipe.execute_command(
                "TS.ADD",
                f"q3:ts:{metric}:{airline}",
                timestamp_ms,
                value,
                "ON_DUPLICATE",
                "LAST",
            )
            exported += 1

    pipe.execute()
    return exported


def export_q3_global_minmax(r: redis.Redis, rows: list[dict]) -> int:
    required_columns = {
        "airline",
        "min_delay",
        "max_delay",
    }

    require_columns(rows, required_columns, Q3_MINMAX_CSV_PATH)

    pipe = r.pipeline(transaction=False)
    exported = 0

    for row in rows:
        airline = row["airline"]
        min_delay = float(row["min_delay"])
        max_delay = float(row["max_delay"])

        # Hash per compagnia.
        pipe.hset(
            f"q3:minmax:{airline}",
            mapping={
                "airline": airline,
                "min_delay": min_delay,
                "max_delay": max_delay,
            },
        )
        exported += 1

        # Hash per metrica: utile per controlli rapidi e grafici semplici.
        pipe.hset("q3:metric:min_delay", airline, min_delay)
        pipe.hset("q3:metric:max_delay", airline, max_delay)
        exported += 2

    pipe.execute()
    return exported


def export_q3(r: redis.Redis) -> None:
    print("\n" + "=" * 72)
    print("EXPORT Q3")
    print("=" * 72)

    require_file(Q3_PERCENTILES_CSV_PATH)
    require_file(Q3_MINMAX_CSV_PATH)

    print(f"[INFO] Lettura CSV percentili: {Q3_PERCENTILES_CSV_PATH}")
    print(f"[INFO] Lettura CSV min/max:    {Q3_MINMAX_CSV_PATH}")

    deleted = delete_keys_by_prefix(r, "q3")
    print(f"[INFO] Eliminate {deleted} chiavi q3:* precedenti")

    percentile_rows = load_csv_rows(Q3_PERCENTILES_CSV_PATH)
    minmax_rows = load_csv_rows(Q3_MINMAX_CSV_PATH)

    exported_percentiles = export_q3_hourly_percentiles(r, percentile_rows)
    exported_minmax = export_q3_global_minmax(r, minmax_rows)

    print("[✓] Q3 esportata su Redis Stack")
    print(f"[INFO] Elementi orari scritti:   {exported_percentiles}")
    print(f"[INFO] Elementi min/max scritti: {exported_minmax}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    r = connect_redis()

    export_q1(r)
    export_q2(r)
    export_q3(r)

    print("\n" + "=" * 72)
    print("[✓] Export completo Q1 + Q2 + Q3 su Redis Stack")
    print("=" * 72)


if __name__ == "__main__":
    main()