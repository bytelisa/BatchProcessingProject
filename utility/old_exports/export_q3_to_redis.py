import csv
import os
import sys
from datetime import datetime, timezone, timedelta

import redis


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

PERCENTILES_CSV_PATH = os.getenv(
    "Q3_PERCENTILES_CSV_PATH",
    "output/query3_hourly_percentiles.csv",
)

MINMAX_CSV_PATH = os.getenv(
    "Q3_MINMAX_CSV_PATH",
    "output/query3_global_minmax.csv",
)

PERCENTILE_METRICS = [
    "p25",
    "p50",
    "p75",
    "p90",
]

OTHER_HOURLY_METRICS = [
    "num_flights",
]

ALL_HOURLY_METRICS = OTHER_HOURLY_METRICS + PERCENTILE_METRICS


def hour_to_timestamp_ms(hour: int) -> int:
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


def require_file(path: str):
    if not os.path.exists(path):
        print(f"[ERROR] File non trovato: {path}")
        sys.exit(1)


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

    return r


def delete_q3_keys(r: redis.Redis) -> int:
    keys = list(r.scan_iter("q3:*"))
    if keys:
        r.delete(*keys)
    return len(keys)


def create_timeseries_if_needed(pipe, key: str, metric: str, airline: str):
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


def load_csv_rows(path: str):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print(f"[ERROR] CSV vuoto: {path}")
        sys.exit(1)

    return rows


def export_hourly_percentiles(r: redis.Redis, rows: list[dict]) -> int:
    required_columns = {
        "airline",
        "hour",
        "num_flights",
        "p25",
        "p50",
        "p75",
        "p90",
    }

    missing = required_columns - set(rows[0].keys())
    if missing:
        print(f"[ERROR] Colonne mancanti in {PERCENTILES_CSV_PATH}: {sorted(missing)}")
        print(f"[INFO] Colonne trovate: {list(rows[0].keys())}")
        sys.exit(1)

    airlines = sorted({row["airline"] for row in rows})

    pipe = r.pipeline(transaction=False)

    # Crea una TimeSeries per ogni compagnia e metrica oraria.
    for airline in airlines:
        for metric in ALL_HOURLY_METRICS:
            ts_key = f"q3:ts:{metric}:{airline}"
            create_timeseries_if_needed(pipe, ts_key, metric, airline)

    pipe.execute()

    pipe = r.pipeline(transaction=False)
    exported = 0

    for row in rows:
        airline = row["airline"]
        hour = int(row["hour"])

        if hour < 0 or hour > 23:
            raise ValueError(f"Ora non valida in Q3: {hour}")

        timestamp_ms = hour_to_timestamp_ms(hour)

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
        for metric in ALL_HOURLY_METRICS:
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


def export_global_minmax(r: redis.Redis, rows: list[dict]) -> int:
    required_columns = {
        "airline",
        "min_delay",
        "max_delay",
    }

    missing = required_columns - set(rows[0].keys())
    if missing:
        print(f"[ERROR] Colonne mancanti in {MINMAX_CSV_PATH}: {sorted(missing)}")
        print(f"[INFO] Colonne trovate: {list(rows[0].keys())}")
        sys.exit(1)

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


def main():
    require_file(PERCENTILES_CSV_PATH)
    require_file(MINMAX_CSV_PATH)

    r = connect_redis()

    print(f"[INFO] Connesso a Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"[INFO] Lettura CSV percentili: {PERCENTILES_CSV_PATH}")
    print(f"[INFO] Lettura CSV min/max:    {MINMAX_CSV_PATH}")

    deleted = delete_q3_keys(r)
    print(f"[INFO] Eliminate {deleted} chiavi q3:* precedenti")

    percentile_rows = load_csv_rows(PERCENTILES_CSV_PATH)
    minmax_rows = load_csv_rows(MINMAX_CSV_PATH)

    exported_percentiles = export_hourly_percentiles(r, percentile_rows)
    exported_minmax = export_global_minmax(r, minmax_rows)

    print("[✓] Q3 esportata su Redis Stack")
    print(f"[INFO] Elementi orari scritti:   {exported_percentiles}")
    print(f"[INFO] Elementi min/max scritti: {exported_minmax}")


if __name__ == "__main__":
    main()