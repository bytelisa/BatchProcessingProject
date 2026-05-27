import csv
import os
import sys
from datetime import datetime, timezone

import redis


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

CSV_PATH = os.getenv("CSV_PATH", "output/query1_monthly_stats.csv")


METRICS = [
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


def delete_q1_keys(r: redis.Redis) -> int:
    keys = list(r.scan_iter("q1:*"))
    if keys:
        r.delete(*keys)
    return len(keys)


def ts_create_if_needed(pipe, key: str, metric: str, airline: str):
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


def main():
    if not os.path.exists(CSV_PATH):
        print(f"[ERROR] CSV non trovato: {CSV_PATH}")
        sys.exit(1)

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
    print(f"[INFO] Lettura CSV: {CSV_PATH}")

    deleted = delete_q1_keys(r)
    print(f"[INFO] Eliminate {deleted} chiavi q1:* precedenti")

    # Leggiamo prima tutte le righe, così possiamo creare le serie una volta sola.
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("[ERROR] CSV vuoto")
        sys.exit(1)

    required_columns = {"month", "airline", *METRICS}
    missing_columns = required_columns - set(rows[0].keys())

    if missing_columns:
        print(f"[ERROR] Colonne mancanti nel CSV: {sorted(missing_columns)}")
        print(f"[INFO] Colonne trovate: {list(rows[0].keys())}")
        sys.exit(1)

    airlines = sorted({row["airline"] for row in rows})

    pipe = r.pipeline(transaction=False)

    # Crea le serie temporali per ogni metrica/compagnia.
    for airline in airlines:
        for metric in METRICS:
            ts_key = f"q1:ts:{metric}:{airline}"
            ts_create_if_needed(pipe, ts_key, metric, airline)

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
                **{metric: float(row[metric]) for metric in METRICS},
            },
        )
        exported += 1

        # Hash per metrica, ancora utile per debug rapido.
        for metric in METRICS:
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


if __name__ == "__main__":
    main()