"""
benchmark_config.py
───────────────────
Utility per leggere la configurazione dei benchmark da JSON.

Il path di default dentro il container Spark è:
  /opt/scripts/benchmark_config.json

Può essere sovrascritto con variabile d'ambiente:
  BENCHMARK_CONFIG=/path/to/config.json
"""

import json
import os


DEFAULT_CONFIG_PATH = "/opt/scripts/benchmark_config.json"


def load_benchmark_config():
    path = os.getenv("BENCHMARK_CONFIG", DEFAULT_CONFIG_PATH)

    if not os.path.exists(path):
        raise FileNotFoundError(
            "File di configurazione benchmark non trovato: " + path
        )

    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    return cfg