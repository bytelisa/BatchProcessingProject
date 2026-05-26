import csv
import os
import sys
import redis


# Se esegui lo script dal PC host: localhost
# Se lo esegui da un container Docker nella stessa compose: redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
 # REDIS_PASSWORD = "change_me" or None

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


def main():
    if not os.path.exists(CSV_PATH):
        print(f"[ERROR] CSV non trovato: {CSV_PATH}")
        sys.exit(1)

    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        # password=REDIS_PASSWORD,
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

    pipe = r.pipeline()

    # Pulisce solo le chiavi della Query 1
    keys_to_delete = list(r.scan_iter("q1:*"))
    if keys_to_delete:
        pipe.delete(*keys_to_delete)
        pipe.execute()
        pipe = r.pipeline()
        print(f"[INFO] Eliminate {len(keys_to_delete)} chiavi q1:* precedenti")

    exported_keys = 0

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required_columns = {"month", "airline", *METRICS}
        missing_columns = required_columns - set(reader.fieldnames or [])

        if missing_columns:
            print(f"[ERROR] Colonne mancanti nel CSV: {sorted(missing_columns)}")
            print(f"[INFO] Colonne trovate: {reader.fieldnames}")
            sys.exit(1)

        metrics = [
            "dep_delay_mean",
            "dep_delay_min",
            "dep_delay_max",
            "arr_delay_mean",
            "arr_delay_min",
            "arr_delay_max",
            "cancellation_rate",
        ]

        for row in reader:
            month = int(row["month"])
            airline = row["airline"]

            # Hash per riga: comodo per debug/ispezione
            pipe.hset(
                f"q1:row:{airline}:{month}",
                mapping={
                    "month": month,
                    "airline": airline,
                    **{metric: float(row[metric]) for metric in metrics},
                },
            )

            # Hash per metrica e compagnia: comodo per Grafana con HMGET
            for metric in metrics:
                pipe.hset(
                    f"q1:metric:{metric}:{airline}",
                    str(month),
                    float(row[metric]),
                )
            exported_keys += 1

    pipe.execute()

    print(f"[✓] Q1 esportata su Redis")
    print(f"[INFO] Chiavi/hash scritti: {exported_keys}")


if __name__ == "__main__":
    main()