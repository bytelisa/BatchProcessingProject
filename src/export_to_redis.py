import csv
import os
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

CSV_PATH = "Results/q1/q1_results.csv"

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
)

# Pulisci solo chiavi Q1 precedenti
for key in r.scan_iter("q1:*"):
    r.delete(key)

with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    pipe = r.pipeline()

    for row in reader:
        month = int(row["month"])
        airline = row["airline"]

        pipe.set(f"q1:dep_delay_mean:{airline}:{month}", row["dep_delay_mean"])
        pipe.set(f"q1:dep_delay_min:{airline}:{month}", row["dep_delay_min"])
        pipe.set(f"q1:dep_delay_max:{airline}:{month}", row["dep_delay_max"])
        pipe.set(f"q1:cancellation_rate:{airline}:{month}", row["cancellation_rate"])

    pipe.execute()

print("Q1 exported to Redis")