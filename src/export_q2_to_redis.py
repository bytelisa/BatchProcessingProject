"""
export_to_redis.py
──────────────────
Esporta i risultati CSV delle query su Redis Stack per visualizzazione Grafana.

Modello Redis scelto:

Q1 - dati mensili:
    RedisTimeSeries
    q1:ts:<metric>:<airline>

Q2 - ranking compagnie:
    Hash per riga + Sorted Set per classifica
    q2:all:<carrier>
    q2:top10:<carrier>
    q2:z:arrdelay_rank

Q3 - percentili orari:
    RedisTimeSeries
    q3:ts:<percentile>:<airline>
    q3:minmax:<airline>

Nota:
Q1 e Q3 sono naturalmente time-like.
Q2 è categorica, quindi viene modellata con Hash e Sorted Set.
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
# Path CSV locali
# ─────────────────────────────────────────────

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")

Q1_CSV = os.path.join(OUTPUT_DIR, "query1_monthly_stats.csv")

Q2_ALL_CSV = os.path.join(OUTPUT_DIR, "query2_all_airlines_stats.csv")
Q2_TOP10_CSV = os.path.join(OUTPUT_DIR, "query2_top10_arrival_delay.csv")

Q3_PERCENTILES_CSV = os.path.join(OUTPUT_DIR, "query3_hourly_percentiles.csv")
Q3_MINMAX_CSV = os.path.join(OUTPUT_DIR, "query3_global_minmax.csv")


# ─────────────────────────────────────────────
# Metriche
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

Q2_METRICS = [
    "num_flights",
    "arrdelay_mean",
    "carrier_delay_mean",
    "weather_delay_mean",
    "nas_delay_mean",
    "security_delay_mean",
    "late_aircraft_delay_mean",
]

Q3_PERCENTILES = ["p25", "p50", "p75", "p90"]


# ─────────────────────────────────────────────
# Timestamp sintetici per Grafana
# ─────────────────────────────────────────────

MONTH_TIMESTAMPS_MS = {
    1: int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
    2: int(datetime(2025, 2, 1, tzinfo=timezone.utc).timestamp() * 1000),
    3: int(datetime(2025, 3, 1, tzinfo=timezone.utc).timestamp() * 1000),
    4: int(datetime(2025, 4, 1, tzinfo=timezone.utc).timestamp() * 1000),
}

# Per Q3 usiamo una giornata fittizia.
# Serve solo per far capire a Grafana l'ordine delle 24 fasce orarie.
Q3_BASE_DATETIME = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)


def hour_timestamp_ms(hour: int) -> int:
    return int((Q3_BASE_DATETIME + timedelta(hours=hour)).timestamp() * 1000)


# ─────────────────────────────────────────────
# Utility
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


def require_csv(path: str) -> None:
    if not os.path.exists(path):
        print(f"[ERROR] CSV non trovato: {path}")
        sys.exit(1)


def read_csv(path: str) -> list[dict]:
    require_csv(path)

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print(f"[ERROR] CSV vuoto: {path}")
        sys.exit(1)

    return rows


def require_columns(rows: list[dict], required_columns: set[str], csv_name: str) -> None:
    found = set(rows[0].keys())
    missing = required_columns - found

    if missing:
        print(f"[ERROR] Colonne mancanti in {csv_name}: {sorted(missing)}")
        print(f"[INFO] Colonne trovate: {sorted(found)}")
        sys.exit(1)


def to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def to_int(value):
    if value is None or value == "":
        return None
    return int(float(value))


def delete_keys_by_prefix(r: redis.Redis, prefix: str) -> int:
    keys = list(r.scan_iter(f"{prefix}:*"))
    if keys:
        r.delete(*keys)
    return len(keys)


def ts_create_if_needed(
    pipe,
    key: str,
    labels: dict[str, str],
):
    """
    Crea una RedisTimeSeries.
    Se la chiave esiste già, TS.CREATE darebbe errore.
    Qui cancelliamo prima le chiavi q1/q3, quindi non serve gestire EXISTS.
    """
    args = [
        "TS.CREATE",
        key,
        "DUPLICATE_POLICY",
        "LAST",
        "LABELS",
    ]

    for label_key, label_value in labels.items():
        args.append(label_key)
        args.append(label_value)

    pipe.execute_command(*args)


# ─────────────────────────────────────────────
# Export Q1
# ─────────────────────────────────────────────

def export_q1(r: redis.Redis) -> None:
    print("\n" + "=" * 72)
    print("EXPORT Q1 - Monthly statistics")
    print("=" * 72)

    rows = read_csv(Q1_CSV)

    required_columns = {"month", "airline", *Q1_METRICS}
    require_columns(rows, required_columns, Q1_CSV)

    deleted = delete_keys_by_prefix(r, "q1")
    print(f"[INFO] Eliminate {deleted} chiavi q1:* precedenti")

    airlines = sorted({row["airline"] for row in rows})

    pipe = r.pipeline(transaction=False)

    for airline in airlines:
        for metric in Q1_METRICS:
            ts_key = f"q1:ts:{metric}:{airline}"

            ts_create_if_needed(
                pipe,
                ts_key,
                labels={
                    "query": "q1",
                    "kind": "monthly",
                    "metric": metric,
                    "airline": airline,
                },
            )

    pipe.execute()

    pipe = r.pipeline(transaction=False)
    written = 0

    for row in rows:
        month = to_int(row["month"])
        airline = row["airline"]

        if month not in MONTH_TIMESTAMPS_MS:
            raise ValueError(f"Mese non valido per Q1: {month}")

        timestamp_ms = MONTH_TIMESTAMPS_MS[month]

        # Hash per debug/ispezione.
        row_key = f"q1:row:{airline}:{month}"
        pipe.hset(
            row_key,
            mapping={
                key: row[key]
                for key in row.keys()
            },
        )
        written += 1

        # TimeSeries per Grafana.
        for metric in Q1_METRICS:
            value = to_float(row[metric])
            if value is None:
                continue

            pipe.execute_command(
                "TS.ADD",
                f"q1:ts:{metric}:{airline}",
                timestamp_ms,
                value,
                "ON_DUPLICATE",
                "LAST",
            )
            written += 1

    pipe.execute()

    print("[✓] Q1 esportata su Redis")
    print(f"[INFO] Elementi scritti: {written}")


# ─────────────────────────────────────────────
# Export Q2
# ─────────────────────────────────────────────

def export_q2(r: redis.Redis) -> None:
    print("\n" + "=" * 72)
    print("EXPORT Q2 - Airline ranking and delay causes")
    print("=" * 72)

    all_rows = read_csv(Q2_ALL_CSV)
    top10_rows = read_csv(Q2_TOP10_CSV)

    required_columns = {"carrier", *Q2_METRICS}

    require_columns(all_rows, required_columns, Q2_ALL_CSV)
    require_columns(top10_rows, required_columns, Q2_TOP10_CSV)

    deleted = delete_keys_by_prefix(r, "q2")
    print(f"[INFO] Eliminate {deleted} chiavi q2:* precedenti")

    pipe = r.pipeline(transaction=False)
    written = 0

    # 1. Tutte le compagnie aggregate.
    for row in all_rows:
        carrier = row["carrier"]

        pipe.hset(
            f"q2:all:{carrier}",
            mapping={
                "carrier": carrier,
                "num_flights": to_int(row["num_flights"]),
                "arrdelay_mean": to_float(row["arrdelay_mean"]),
                "carrier_delay_mean": to_float(row["carrier_delay_mean"]),
                "weather_delay_mean": to_float(row["weather_delay_mean"]),
                "nas_delay_mean": to_float(row["nas_delay_mean"]),
                "security_delay_mean": to_float(row["security_delay_mean"]),
                "late_aircraft_delay_mean": to_float(row["late_aircraft_delay_mean"]),
            },
        )
        written += 1

    # 2. Top 10.
    for rank, row in enumerate(top10_rows, start=1):
        carrier = row["carrier"]
        arrdelay_mean = to_float(row["arrdelay_mean"])

        pipe.hset(
            f"q2:top10:{carrier}",
            mapping={
                "rank": rank,
                "carrier": carrier,
                "num_flights": to_int(row["num_flights"]),
                "arrdelay_mean": arrdelay_mean,
                "carrier_delay_mean": to_float(row["carrier_delay_mean"]),
                "weather_delay_mean": to_float(row["weather_delay_mean"]),
                "nas_delay_mean": to_float(row["nas_delay_mean"]),
                "security_delay_mean": to_float(row["security_delay_mean"]),
                "late_aircraft_delay_mean": to_float(row["late_aircraft_delay_mean"]),
            },
        )
        written += 1

        # Sorted Set per ranking Grafana/table.
        # Score = ARR_DELAY medio.
        pipe.zadd("q2:z:arrdelay_rank", {carrier: arrdelay_mean})
        written += 1

        # Set ordinato per recuperare solo i carrier top10.
        pipe.sadd("q2:set:top10_carriers", carrier)
        written += 1

    pipe.execute()

    print("[✓] Q2 esportata su Redis")
    print(f"[INFO] Elementi scritti: {written}")
    print("[INFO] Ranking Q2: q2:z:arrdelay_rank")
    print("[INFO] Hash top10: q2:top10:<carrier>")
    print("[INFO] Hash all: q2:all:<carrier>")


# ─────────────────────────────────────────────
# Export Q3
# ─────────────────────────────────────────────

def export_q3(r: redis.Redis) -> None:
    print("\n" + "=" * 72)
    print("EXPORT Q3 - Hourly percentiles")
    print("=" * 72)

    percentile_rows = read_csv(Q3_PERCENTILES_CSV)
    minmax_rows = read_csv(Q3_MINMAX_CSV)

    require_columns(
        percentile_rows,
        {"airline", "hour", "num_flights", *Q3_PERCENTILES},
        Q3_PERCENTILES_CSV,
    )

    require_columns(
        minmax_rows,
        {"airline", "min_delay", "max_delay"},
        Q3_MINMAX_CSV,
    )

    deleted = delete_keys_by_prefix(r, "q3")
    print(f"[INFO] Eliminate {deleted} chiavi q3:* precedenti")

    airlines = sorted({row["airline"] for row in percentile_rows})

    pipe = r.pipeline(transaction=False)

    for airline in airlines:
        for percentile in Q3_PERCENTILES:
            ts_key = f"q3:ts:{percentile}:{airline}"

            ts_create_if_needed(
                pipe,
                ts_key,
                labels={
                    "query": "q3",
                    "kind": "hourly",
                    "metric": percentile,
                    "airline": airline,
                },
            )

        # opzionale ma utile: serie del numero voli per ora
        ts_create_if_needed(
            pipe,
            f"q3:ts:num_flights:{airline}",
            labels={
                "query": "q3",
                "kind": "hourly",
                "metric": "num_flights",
                "airline": airline,
            },
        )

    pipe.execute()

    pipe = r.pipeline(transaction=False)
    written = 0

    # Percentili orari.
    for row in percentile_rows:
        airline = row["airline"]
        hour = to_int(row["hour"])

        if hour is None or hour < 0 or hour > 23:
            raise ValueError(f"Ora non valida per Q3: {hour}")

        timestamp_ms = hour_timestamp_ms(hour)

        # Hash per ispezione/debug.
        row_key = f"q3:row:{airline}:{hour}"
        pipe.hset(
            row_key,
            mapping={
                key: row[key]
                for key in row.keys()
            },
        )
        written += 1

        # TimeSeries per Grafana.
        for percentile in Q3_PERCENTILES:
            value = to_float(row[percentile])
            if value is None:
                continue

            pipe.execute_command(
                "TS.ADD",
                f"q3:ts:{percentile}:{airline}",
                timestamp_ms,
                value,
                "ON_DUPLICATE",
                "LAST",
            )
            written += 1

        num_flights = to_float(row["num_flights"])
        if num_flights is not None:
            pipe.execute_command(
                "TS.ADD",
                f"q3:ts:num_flights:{airline}",
                timestamp_ms,
                num_flights,
                "ON_DUPLICATE",
                "LAST",
            )
            written += 1

    # Min/max globale per compagnia.
    for row in minmax_rows:
        airline = row["airline"]

        pipe.hset(
            f"q3:minmax:{airline}",
            mapping={
                "airline": airline,
                "min_delay": to_float(row["min_delay"]),
                "max_delay": to_float(row["max_delay"]),
            },
        )
        written += 1

    pipe.execute()

    print("[✓] Q3 esportata su Redis")
    print(f"[INFO] Elementi scritti: {written}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    r = connect_redis()

    export_q1(r)
    export_q2(r)
    export_q3(r)

    print("\n" + "=" * 72)
    print("[✓] Export completo su Redis Stack")
    print("=" * 72)

    print("\nChiavi principali per Grafana:")
    print("  Q1 TimeSeries:")
    print("    q1:ts:dep_delay_mean:AA")
    print("    q1:ts:cancellation_rate:DL")
    print()
    print("  Q2 Ranking / Hash:")
    print("    q2:z:arrdelay_rank")
    print("    q2:top10:<carrier>")
    print("    q2:all:<carrier>")
    print()
    print("  Q3 TimeSeries:")
    print("    q3:ts:p25:AA")
    print("    q3:ts:p50:AA")
    print("    q3:ts:p75:AA")
    print("    q3:ts:p90:AA")
    print("    q3:minmax:AA")


if __name__ == "__main__":
    main()