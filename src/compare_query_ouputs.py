"""
compare_query_outputs.py
────────────────────────
Confronta gli output CSV delle query DataFrame e RDD.

Esempi:

  python3 src/compare_query_outputs.py --query q1

  python3 src/compare_query_outputs.py --query q2 --part all

  python3 src/compare_query_outputs.py --query q2 --part top10

  python3 src/compare_query_outputs.py --query q3 --part percentiles

  python3 src/compare_query_outputs.py --query q3 --part minmax

Lo script confronta:
- stesse chiavi logiche;
- stesse colonne rilevanti;
- valori numerici con tolleranza configurabile.

Default:
- cartella output: output/
- tolleranza float: 0.0001
"""

import argparse
import csv
import math
import os
import sys


DEFAULT_OUTPUT_DIR = "output"


COMPARISONS = {
    "q1": {
        "default_part": "main",
        "parts": {
            "main": {
                "df": "query1_monthly_stats.csv",
                "rdd": "query1_rdd_monthly_stats.csv",
                "keys": ["month", "airline"],
                "columns": [
                    "dep_delay_mean",
                    "dep_delay_min",
                    "dep_delay_max",
                    "arr_delay_mean",
                    "arr_delay_min",
                    "arr_delay_max",
                    "cancellation_rate",
                ],
            }
        },
    },

    "q2": {
        "default_part": "top10",
        "parts": {
            "all": {
                "df": "query2_all_airlines_stats.csv",
                "rdd": "query2_rdd_all_airlines_stats.csv",
                "keys": ["carrier"],
                "columns": [
                    "num_flights",
                    "arrdelay_mean",
                    "carrier_delay_mean",
                    "weather_delay_mean",
                    "nas_delay_mean",
                    "security_delay_mean",
                    "late_aircraft_delay_mean",
                ],
            },
            "top10": {
                "df": "query2_top10_arrival_delay.csv",
                "rdd": "query2_rdd_top10_arrival_delay.csv",
                "keys": ["carrier"],
                "columns": [
                    "num_flights",
                    "arrdelay_mean",
                    "carrier_delay_mean",
                    "weather_delay_mean",
                    "nas_delay_mean",
                    "security_delay_mean",
                    "late_aircraft_delay_mean",
                ],
                "check_order": True,
            },
        },
    },

    "q3": {
        "default_part": "percentiles",
        "parts": {
            "percentiles": {
                "df": "query3_hourly_percentiles.csv",
                "rdd": "query3_rdd_hourly_percentiles.csv",
                "keys": ["airline", "hour"],
                "columns": [
                    "num_flights",
                    "p25",
                    "p50",
                    "p75",
                    "p90",
                ],
            },
            "minmax": {
                "df": "query3_global_minmax.csv",
                "rdd": "query3_rdd_global_minmax.csv",
                "keys": ["airline"],
                "columns": [
                    "min_delay",
                    "max_delay",
                ],
            },
        },
    },
}


def read_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File non trovato: {path}")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise ValueError(f"CSV vuoto: {path}")

    return rows


def normalize_value(value):
    if value is None:
        return ""

    return str(value).strip()


def key_for_row(row, key_columns):
    return tuple(normalize_value(row.get(col)) for col in key_columns)


def to_float_or_none(value):
    value = normalize_value(value)

    if value == "":
        return None

    try:
        return float(value)
    except ValueError:
        return None


def values_equal(a, b, tolerance):
    a_norm = normalize_value(a)
    b_norm = normalize_value(b)

    a_float = to_float_or_none(a_norm)
    b_float = to_float_or_none(b_norm)

    if a_float is not None and b_float is not None:
        return math.isclose(a_float, b_float, rel_tol=tolerance, abs_tol=tolerance)

    return a_norm == b_norm


def build_index(rows, key_columns, label):
    index = {}

    for row in rows:
        key = key_for_row(row, key_columns)

        if key in index:
            raise ValueError(f"Chiave duplicata in {label}: {key}")

        index[key] = row

    return index


def validate_columns(rows, required_columns, label):
    available = set(rows[0].keys())
    missing = [col for col in required_columns if col not in available]

    if missing:
        raise ValueError(
            f"Colonne mancanti in {label}: {missing}\n"
            f"Colonne disponibili: {sorted(available)}"
        )


def compare_order(df_rows, rdd_rows, key_columns):
    df_order = [key_for_row(row, key_columns) for row in df_rows]
    rdd_order = [key_for_row(row, key_columns) for row in rdd_rows]

    return df_order == rdd_order, df_order, rdd_order


def compare_outputs(df_path, rdd_path, key_columns, value_columns, tolerance, check_order):
    print("=" * 72)
    print("CONFRONTO OUTPUT QUERY")
    print("=" * 72)
    print(f"DF :  {df_path}")
    print(f"RDD:  {rdd_path}")
    print(f"Key columns:   {key_columns}")
    print(f"Value columns: {value_columns}")
    print(f"Tolerance:     {tolerance}")
    print()

    df_rows = read_csv(df_path)
    rdd_rows = read_csv(rdd_path)

    required_columns = key_columns + value_columns

    validate_columns(df_rows, required_columns, "DataFrame CSV")
    validate_columns(rdd_rows, required_columns, "RDD CSV")

    df_index = build_index(df_rows, key_columns, "DataFrame CSV")
    rdd_index = build_index(rdd_rows, key_columns, "RDD CSV")

    df_keys = set(df_index.keys())
    rdd_keys = set(rdd_index.keys())

    only_df = sorted(df_keys - rdd_keys)
    only_rdd = sorted(rdd_keys - df_keys)

    errors = []

    if only_df:
        errors.append(f"Chiavi presenti solo nel CSV DataFrame: {only_df[:20]}")

    if only_rdd:
        errors.append(f"Chiavi presenti solo nel CSV RDD: {only_rdd[:20]}")

    common_keys = sorted(df_keys & rdd_keys)

    mismatches = []

    for key in common_keys:
        df_row = df_index[key]
        rdd_row = rdd_index[key]

        for col in value_columns:
            df_value = df_row.get(col)
            rdd_value = rdd_row.get(col)

            if not values_equal(df_value, rdd_value, tolerance):
                mismatches.append(
                    {
                        "key": key,
                        "column": col,
                        "df": df_value,
                        "rdd": rdd_value,
                    }
                )

    if mismatches:
        errors.append(f"Valori diversi trovati: {len(mismatches)}")

    if check_order:
        same_order, df_order, rdd_order = compare_order(df_rows, rdd_rows, key_columns)

        if not same_order:
            errors.append("Ordine delle righe diverso tra DataFrame e RDD")

    print("Righe DataFrame:", len(df_rows))
    print("Righe RDD:      ", len(rdd_rows))
    print("Chiavi comuni:  ", len(common_keys))
    print()

    if mismatches:
        print("Prime differenze sui valori:")
        for m in mismatches[:20]:
            print(
                f"  key={m['key']} | column={m['column']} | "
                f"df={m['df']} | rdd={m['rdd']}"
            )
        print()

    if check_order and errors:
        if "Ordine delle righe diverso tra DataFrame e RDD" in errors:
            print("Prime differenze di ordine:")
            for i, (df_key, rdd_key) in enumerate(zip(df_order, rdd_order)):
                if df_key != rdd_key:
                    print(f"  posizione {i}: df={df_key}, rdd={rdd_key}")
                    break
            print()

    if errors:
        print("[✗] Output NON equivalenti")
        for err in errors:
            print(f"  - {err}")
        return False

    print("[✓] Output equivalenti")
    return True


def resolve_config(query, part):
    if query not in COMPARISONS:
        raise ValueError(
            f"Query non supportata: {query}. "
            f"Valori ammessi: {sorted(COMPARISONS.keys())}"
        )

    query_config = COMPARISONS[query]

    if part is None:
        part = query_config["default_part"]

    if part not in query_config["parts"]:
        raise ValueError(
            f"Parte non supportata per {query}: {part}. "
            f"Valori ammessi: {sorted(query_config['parts'].keys())}"
        )

    return query_config["parts"][part], part


def main():
    parser = argparse.ArgumentParser(
        description="Confronta output CSV DataFrame vs RDD."
    )

    parser.add_argument(
        "--query",
        required=True,
        choices=sorted(COMPARISONS.keys()),
        help="Query da confrontare: q1, q2, q3.",
    )

    parser.add_argument(
        "--part",
        default=None,
        help="Parte della query da confrontare. Esempi: all, top10, percentiles, minmax.",
    )

    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Cartella contenente i CSV locali.",
    )

    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.0001,
        help="Tolleranza per confronto valori numerici.",
    )

    parser.add_argument(
        "--df-path",
        default=None,
        help="Path CSV DataFrame custom.",
    )

    parser.add_argument(
        "--rdd-path",
        default=None,
        help="Path CSV RDD custom.",
    )

    parser.add_argument(
        "--check-order",
        action="store_true",
        help="Forza il controllo dell'ordine righe.",
    )

    args = parser.parse_args()

    config, selected_part = resolve_config(args.query, args.part)

    df_path = args.df_path or os.path.join(args.output_dir, config["df"])
    rdd_path = args.rdd_path or os.path.join(args.output_dir, config["rdd"])

    check_order = args.check_order or config.get("check_order", False)

    print(f"[INFO] Query: {args.query}")
    print(f"[INFO] Part:  {selected_part}")

    ok = compare_outputs(
        df_path=df_path,
        rdd_path=rdd_path,
        key_columns=config["keys"],
        value_columns=config["columns"],
        tolerance=args.tolerance,
        check_order=check_order,
    )

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()