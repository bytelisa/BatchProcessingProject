from pathlib import Path
from collections import Counter, defaultdict
import pandas as pd
import time


DATA_DIR = Path(r"C:\Users\elisa\IdeaProjects\BatchProcessingProject\data")
CHUNKSIZE = 200_000

DELAY_CAUSE_COLS = [
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
]

DUPLICATE_KEY_COLS = [
    "YEAR",
    "MONTH",
    "DAY_OF_MONTH",
    "OP_UNIQUE_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN_AIRPORT_ID",
    "DEST_AIRPORT_ID",
    "CRS_DEP_TIME",
]

REQUIRED_COLS = sorted(set(
    [
        "YEAR",
        "MONTH",
        "DAY_OF_MONTH",
        "OP_UNIQUE_CARRIER",
        "OP_CARRIER_FL_NUM",
        "ORIGIN_AIRPORT_ID",
        "DEST_AIRPORT_ID",
        "CRS_DEP_TIME",
        "CANCELLED",
        "DIVERTED",
        "DEP_DELAY",
        "ARR_DELAY",
    ]
    + DELAY_CAUSE_COLS
    + DUPLICATE_KEY_COLS
))


def format_int(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def format_float(x: float) -> str:
    return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def status_distribution(series: pd.Series) -> dict:
    numeric = safe_numeric(series)

    count_0 = int((numeric == 0).sum())
    count_1 = int((numeric == 1).sum())
    count_null = int(numeric.isna().sum())
    count_other = int((numeric.notna() & ~numeric.isin([0, 1])).sum())

    return {
        "0": count_0,
        "1": count_1,
        "null": count_null,
        "other": count_other,
    }


def update_min_max(current_min, current_max, series: pd.Series):
    non_null = series.dropna()

    if non_null.empty:
        return current_min, current_max

    local_min = float(non_null.min())
    local_max = float(non_null.max())

    if current_min is None or local_min < current_min:
        current_min = local_min

    if current_max is None or local_max > current_max:
        current_max = local_max

    return current_min, current_max


def init_delay_category_stats():
    return {
        "le_0": {
            "total": 0,
            "all_causes_null": 0,
            "any_cause_null": 0,
        },
        "gt_0_lt_15": {
            "total": 0,
            "all_causes_null": 0,
            "any_cause_null": 0,
        },
        "ge_15": {
            "total": 0,
            "all_causes_null": 0,
            "any_cause_null": 0,
        },
    }


def update_delay_category_stats(stats: dict, delay_series: pd.Series, causes_df: pd.DataFrame):
    """
    Aggiorna le statistiche per:
    - delay <= 0
    - 0 < delay < 15
    - delay >= 15

    Considera solo righe dove delay non è null.
    """

    valid_delay_mask = delay_series.notna()

    causes_all_null_mask = causes_df.isna().all(axis=1)
    causes_any_null_mask = causes_df.isna().any(axis=1)

    categories = {
        "le_0": valid_delay_mask & (delay_series <= 0),
        "gt_0_lt_15": valid_delay_mask & (delay_series > 0) & (delay_series < 15),
        "ge_15": valid_delay_mask & (delay_series >= 15),
    }

    for category_name, mask in categories.items():
        stats[category_name]["total"] += int(mask.sum())
        stats[category_name]["all_causes_null"] += int((mask & causes_all_null_mask).sum())
        stats[category_name]["any_cause_null"] += int((mask & causes_any_null_mask).sum())


def print_delay_category_stats(title: str, stats: dict):
    labels = {
        "le_0": "delay <= 0, voli in anticipo o in orario",
        "gt_0_lt_15": "0 < delay < 15, ritardo lieve non ufficiale BTS",
        "ge_15": "delay >= 15, ritardo ufficiale BTS",
    }

    print(title)

    for key, label in labels.items():
        total = stats[key]["total"]
        all_null = stats[key]["all_causes_null"]
        any_null = stats[key]["any_cause_null"]

        all_null_perc = all_null / total * 100 if total > 0 else 0
        any_null_perc = any_null / total * 100 if total > 0 else 0

        print(f"\n  {label}:")
        print(f"    Totale voli: {format_int(total)}")
        print(
            f"    Voli con tutte le cause null: "
            f"{format_int(all_null)} ({format_float(all_null_perc)}%)"
        )
        print(
            f"    Voli con almeno una causa null: "
            f"{format_int(any_null)} ({format_float(any_null_perc)}%)"
        )



def main():
    start_time = time.perf_counter()

    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"Nessun file CSV trovato in: {DATA_DIR}")

    print("=" * 100)
    print("DATASET QUALITY CHECK")
    print("=" * 100)
    print(f"Cartella analizzata: {DATA_DIR}")
    print(f"File CSV trovati: {len(csv_files)}")
    for f in csv_files:
        print(f"  - {f.name}")
    print("=" * 100)

    # 1. Controlli generali
    total_rows = 0
    rows_by_month = Counter()
    rows_by_carrier = Counter()

    # Duplicati
    duplicate_key_counter = Counter()
    duplicate_rows_total = 0

    # 2. Stato volo
    cancelled_dist = Counter()
    diverted_dist = Counter()
    cancelled_and_diverted = 0

    # 3. Delay
    dep_delay_null_non_cancelled = 0
    arr_delay_null_non_cancelled_non_diverted = 0

    dep_delay_min = None
    dep_delay_max = None
    arr_delay_min = None
    arr_delay_max = None

    dep_delay_negative_count = 0
    arr_delay_negative_count = 0

    # Fasce di ritardo DEP_DELAY / ARR_DELAY
    dep_delay_category_stats = init_delay_category_stats()
    arr_delay_category_stats = init_delay_category_stats()

    # 4. Cause ritardo
    cause_null_counts = Counter()
    cause_negative_counts = Counter()

    arr_delay_positive_all_causes_null = 0
    arr_delay_15_all_causes_null = 0

    # Somma cause vs ARR_DELAY
    arr_delay_15_rows = 0
    arr_delay_15_causes_all_not_null = 0
    arr_delay_15_total_cause_equals_arr_delay = 0
    arr_delay_15_total_cause_differs_arr_delay = 0
    arr_delay_15_total_cause_less_arr_delay = 0
    arr_delay_15_total_cause_greater_arr_delay = 0

    arr_delay_15_diff_abs_sum = 0.0
    arr_delay_15_diff_abs_max = None

    arr_delay_positive_rows = 0
    arr_delay_positive_causes_all_not_null = 0
    arr_delay_positive_total_cause_equals_arr_delay = 0
    arr_delay_positive_total_cause_differs_arr_delay = 0

    # 5. CRS_DEP_TIME
    crs_dep_time_null = 0
    crs_dep_time_lt_0 = 0
    crs_dep_time_gt_2400 = 0
    crs_dep_time_invalid_minutes = 0

    # Per mostrare qualche esempio utile
    examples = {
        "cancelled_and_diverted": [],
        "arr_delay_15_all_causes_null": [],
        "negative_causes": [],
        "invalid_crs_dep_time": [],
        "cause_sum_differs_arr_delay": [],
    }

    for csv_file in csv_files:
        print(f"\nAnalisi file: {csv_file.name}")

        file_start = time.perf_counter()
        file_rows = 0

        for chunk in pd.read_csv(
            csv_file,
            usecols=REQUIRED_COLS,
            chunksize=CHUNKSIZE,
            low_memory=False,
        ):
            file_rows += len(chunk)
            total_rows += len(chunk)

            # Conversioni numeriche robuste
            numeric_cols = [
                "YEAR",
                "MONTH",
                "DAY_OF_MONTH",
                "OP_CARRIER_FL_NUM",
                "ORIGIN_AIRPORT_ID",
                "DEST_AIRPORT_ID",
                "CRS_DEP_TIME",
                "CANCELLED",
                "DIVERTED",
                "DEP_DELAY",
                "ARR_DELAY",
            ] + DELAY_CAUSE_COLS

            for col in numeric_cols:
                chunk[col] = safe_numeric(chunk[col])

            # ------------------------------------------------------------------
            # 1. Controlli generali minimi
            # ------------------------------------------------------------------
            rows_by_month.update(chunk["MONTH"].dropna().astype(int).tolist())
            rows_by_carrier.update(chunk["OP_UNIQUE_CARRIER"].fillna("NULL").astype(str).tolist())

            # Duplicati su chiave ragionevole
            # Nota: calcolo globale su tutti i file.
            # Ogni chiave viene trasformata in tupla.
            key_df = chunk[DUPLICATE_KEY_COLS].copy()
            key_df = key_df.astype("string").fillna("NULL")

            keys = map(tuple, key_df.to_numpy())
            duplicate_key_counter.update(keys)

            # ------------------------------------------------------------------
            # 2. Stato volo
            # ------------------------------------------------------------------
            c_dist = status_distribution(chunk["CANCELLED"])
            d_dist = status_distribution(chunk["DIVERTED"])

            cancelled_dist.update(c_dist)
            diverted_dist.update(d_dist)

            cancelled = chunk["CANCELLED"]
            diverted = chunk["DIVERTED"]

            cancelled_and_diverted_mask = (cancelled == 1) & (diverted == 1)
            cancelled_and_diverted += int(cancelled_and_diverted_mask.sum())

            if len(examples["cancelled_and_diverted"]) < 5:
                sample = chunk.loc[
                    cancelled_and_diverted_mask,
                    [
                        "YEAR",
                        "MONTH",
                        "DAY_OF_MONTH",
                        "OP_UNIQUE_CARRIER",
                        "OP_CARRIER_FL_NUM",
                        "ORIGIN_AIRPORT_ID",
                        "DEST_AIRPORT_ID",
                        "CRS_DEP_TIME",
                        "CANCELLED",
                        "DIVERTED",
                    ],
                ].head(5 - len(examples["cancelled_and_diverted"]))
                examples["cancelled_and_diverted"].extend(sample.to_dict(orient="records"))

            # ------------------------------------------------------------------
            # 3. DEP_DELAY e ARR_DELAY
            # ------------------------------------------------------------------
            non_cancelled_mask = chunk["CANCELLED"] == 0
            non_cancelled_non_diverted_mask = (chunk["CANCELLED"] == 0) & (chunk["DIVERTED"] == 0)

            dep_delay_null_non_cancelled += int(
                (non_cancelled_mask & chunk["DEP_DELAY"].isna()).sum()
            )

            arr_delay_null_non_cancelled_non_diverted += int(
                (non_cancelled_non_diverted_mask & chunk["ARR_DELAY"].isna()).sum()
            )

            dep_delay_min, dep_delay_max = update_min_max(
                dep_delay_min,
                dep_delay_max,
                chunk["DEP_DELAY"],
            )

            arr_delay_min, arr_delay_max = update_min_max(
                arr_delay_min,
                arr_delay_max,
                chunk["ARR_DELAY"],
            )

            dep_delay_negative_count += int((chunk["DEP_DELAY"] < 0).sum())
            arr_delay_negative_count += int((chunk["ARR_DELAY"] < 0).sum())

            # Fasce di ritardo per DEP_DELAY e ARR_DELAY
            # Nota: qui consideriamo solo righe con delay non-null.
            # Le cause vengono controllate indipendentemente dallo stato cancelled/diverted,
            # perché vogliamo capire il comportamento dei campi causa rispetto ai delay.
            update_delay_category_stats(
                dep_delay_category_stats,
                chunk["DEP_DELAY"],
                chunk[DELAY_CAUSE_COLS],
            )

            update_delay_category_stats(
                arr_delay_category_stats,
                chunk["ARR_DELAY"],
                chunk[DELAY_CAUSE_COLS],
            )

            # ------------------------------------------------------------------
            # 4. Cause di ritardo
            # ------------------------------------------------------------------
            for cause_col in DELAY_CAUSE_COLS:
                cause_null_counts[cause_col] += int(chunk[cause_col].isna().sum())
                cause_negative_counts[cause_col] += int((chunk[cause_col] < 0).sum())

            causes_all_null_mask = chunk[DELAY_CAUSE_COLS].isna().all(axis=1)
            causes_all_not_null_mask = chunk[DELAY_CAUSE_COLS].notna().all(axis=1)

            arr_delay_positive_mask = chunk["ARR_DELAY"] > 0
            arr_delay_15_mask = chunk["ARR_DELAY"] >= 15

            arr_delay_positive_rows += int(arr_delay_positive_mask.sum())
            arr_delay_15_rows += int(arr_delay_15_mask.sum())

            arr_delay_positive_all_causes_null += int(
                (arr_delay_positive_mask & causes_all_null_mask).sum()
            )

            arr_delay_15_all_causes_null += int(
                (arr_delay_15_mask & causes_all_null_mask).sum()
            )

            if len(examples["arr_delay_15_all_causes_null"]) < 5:
                sample = chunk.loc[
                    arr_delay_15_mask & causes_all_null_mask,
                    [
                        "YEAR",
                        "MONTH",
                        "DAY_OF_MONTH",
                        "OP_UNIQUE_CARRIER",
                        "ARR_DELAY",
                    ] + DELAY_CAUSE_COLS,
                ].head(5 - len(examples["arr_delay_15_all_causes_null"]))
                examples["arr_delay_15_all_causes_null"].extend(sample.to_dict(orient="records"))

            negative_causes_mask = pd.Series(False, index=chunk.index)
            for cause_col in DELAY_CAUSE_COLS:
                negative_causes_mask = negative_causes_mask | (chunk[cause_col] < 0)

            if len(examples["negative_causes"]) < 5:
                sample = chunk.loc[
                    negative_causes_mask,
                    [
                        "YEAR",
                        "MONTH",
                        "DAY_OF_MONTH",
                        "OP_UNIQUE_CARRIER",
                        "ARR_DELAY",
                    ] + DELAY_CAUSE_COLS,
                ].head(5 - len(examples["negative_causes"]))
                examples["negative_causes"].extend(sample.to_dict(orient="records"))

            # Somma cause vs ARR_DELAY.
            # Per il confronto principale uso solo righe con tutte le cause non null.
            # Questo evita di confondere missing values e valori realmente 0.
            total_cause_delay = chunk[DELAY_CAUSE_COLS].sum(axis=1, skipna=False)

            valid_positive_compare_mask = arr_delay_positive_mask & causes_all_not_null_mask
            valid_15_compare_mask = arr_delay_15_mask & causes_all_not_null_mask

            arr_delay_positive_causes_all_not_null += int(valid_positive_compare_mask.sum())
            arr_delay_15_causes_all_not_null += int(valid_15_compare_mask.sum())

            positive_diff = total_cause_delay - chunk["ARR_DELAY"]
            diff_abs = positive_diff.abs()

            # Confronto con tolleranza minima, utile se ci sono float.
            tolerance = 0.000001

            positive_equal_mask = valid_positive_compare_mask & (diff_abs <= tolerance)
            positive_diff_mask = valid_positive_compare_mask & (diff_abs > tolerance)

            arr_delay_positive_total_cause_equals_arr_delay += int(positive_equal_mask.sum())
            arr_delay_positive_total_cause_differs_arr_delay += int(positive_diff_mask.sum())

            arr_15_equal_mask = valid_15_compare_mask & (diff_abs <= tolerance)
            arr_15_diff_mask = valid_15_compare_mask & (diff_abs > tolerance)

            arr_delay_15_total_cause_equals_arr_delay += int(arr_15_equal_mask.sum())
            arr_delay_15_total_cause_differs_arr_delay += int(arr_15_diff_mask.sum())
            arr_delay_15_total_cause_less_arr_delay += int(
                (valid_15_compare_mask & (total_cause_delay < chunk["ARR_DELAY"] - tolerance)).sum()
            )
            arr_delay_15_total_cause_greater_arr_delay += int(
                (valid_15_compare_mask & (total_cause_delay > chunk["ARR_DELAY"] + tolerance)).sum()
            )

            arr_15_diff_abs_values = diff_abs.loc[arr_15_diff_mask].dropna()
            if not arr_15_diff_abs_values.empty:
                arr_delay_15_diff_abs_sum += float(arr_15_diff_abs_values.sum())
                local_max_diff = float(arr_15_diff_abs_values.max())
                if arr_delay_15_diff_abs_max is None or local_max_diff > arr_delay_15_diff_abs_max:
                    arr_delay_15_diff_abs_max = local_max_diff

            if len(examples["cause_sum_differs_arr_delay"]) < 5:
                sample_cols = [
                    "YEAR",
                    "MONTH",
                    "DAY_OF_MONTH",
                    "OP_UNIQUE_CARRIER",
                    "ARR_DELAY",
                ] + DELAY_CAUSE_COLS

                sample = chunk.loc[arr_15_diff_mask, sample_cols].copy()
                sample["TOTAL_CAUSE_DELAY"] = total_cause_delay.loc[arr_15_diff_mask]
                sample["DIFF_TOTAL_CAUSES_MINUS_ARR_DELAY"] = positive_diff.loc[arr_15_diff_mask]

                sample = sample.head(5 - len(examples["cause_sum_differs_arr_delay"]))
                examples["cause_sum_differs_arr_delay"].extend(sample.to_dict(orient="records"))

            # ------------------------------------------------------------------
            # 5. CRS_DEP_TIME
            # ------------------------------------------------------------------
            crs = chunk["CRS_DEP_TIME"]

            crs_dep_time_null += int(crs.isna().sum())
            crs_dep_time_lt_0 += int((crs < 0).sum())
            crs_dep_time_gt_2400 += int((crs > 2400).sum())

            # Formato HHMM:
            # ora = CRS_DEP_TIME // 100
            # minuti = CRS_DEP_TIME % 100
            # minuti validi: 0..59
            # 2400 lo segnalo come fuori formato operativo per fasce 0..23,
            # ma qui viene già contato nel controllo > 2400 solo se maggiore.
            crs_int = crs.dropna().astype(int)
            invalid_minutes_index = crs_int[(crs_int % 100) >= 60].index
            invalid_minutes_mask = chunk.index.isin(invalid_minutes_index)

            crs_dep_time_invalid_minutes += int(invalid_minutes_mask.sum())

            if len(examples["invalid_crs_dep_time"]) < 5:
                invalid_crs_mask = (
                    crs.isna()
                    | (crs < 0)
                    | (crs > 2400)
                    | invalid_minutes_mask
                )

                sample = chunk.loc[
                    invalid_crs_mask,
                    [
                        "YEAR",
                        "MONTH",
                        "DAY_OF_MONTH",
                        "OP_UNIQUE_CARRIER",
                        "CRS_DEP_TIME",
                    ],
                ].head(5 - len(examples["invalid_crs_dep_time"]))

                examples["invalid_crs_dep_time"].extend(sample.to_dict(orient="records"))

        file_elapsed = time.perf_counter() - file_start
        print(f"  Righe lette: {format_int(file_rows)}")
        print(f"  Tempo file: {format_float(file_elapsed)} secondi")

    # Calcolo duplicati globali
    duplicate_groups = 0
    duplicate_rows_total = 0

    for count in duplicate_key_counter.values():
        if count > 1:
            duplicate_groups += 1
            duplicate_rows_total += count

    elapsed = time.perf_counter() - start_time

    print("\n\n" + "=" * 100)
    print("1. CONTROLLI GENERALI MINIMI")
    print("=" * 100)

    print(f"Numero righe totale: {format_int(total_rows)}")

    print("\nRighe per mese:")
    for month, count in sorted(rows_by_month.items()):
        print(f"  Mese {month}: {format_int(count)}")

    unexpected_months = sorted([m for m in rows_by_month if m not in [1, 2, 3, 4]])
    if unexpected_months:
        print(f"  ATTENZIONE: trovati mesi inattesi: {unexpected_months}")
    else:
        print("  OK: presenti solo mesi 1, 2, 3, 4.")

    print("\nRighe per compagnia:")
    for carrier, count in rows_by_carrier.most_common():
        print(f"  {carrier}: {format_int(count)}")

    print("\nDuplicati su chiave ragionevole:")
    print(f"  Gruppi chiave duplicati: {format_int(duplicate_groups)}")
    print(f"  Righe appartenenti a gruppi duplicati: {format_int(duplicate_rows_total)}")
    print(
        "  Nota: se questo valore è > 0, conviene ispezionare esempi specifici "
        "prima di decidere se deduplicare."
    )

    print("\n\n" + "=" * 100)
    print("2. CONTROLLI SU STATO VOLO")
    print("=" * 100)

    print("Distribuzione CANCELLED:")
    print(f"  0: {format_int(cancelled_dist['0'])}")
    print(f"  1: {format_int(cancelled_dist['1'])}")
    print(f"  null: {format_int(cancelled_dist['null'])}")
    print(f"  valori diversi da 0/1: {format_int(cancelled_dist['other'])}")

    print("\nDistribuzione DIVERTED:")
    print(f"  0: {format_int(diverted_dist['0'])}")
    print(f"  1: {format_int(diverted_dist['1'])}")
    print(f"  null: {format_int(diverted_dist['null'])}")
    print(f"  valori diversi da 0/1: {format_int(diverted_dist['other'])}")

    print(f"\nRighe con CANCELLED = 1 AND DIVERTED = 1: {format_int(cancelled_and_diverted)}")

    print("\n\n" + "=" * 100)
    print("3. CONTROLLI SU DEP_DELAY E ARR_DELAY")
    print("=" * 100)

    print(
        "DEP_DELAY null tra voli non cancellati: "
        f"{format_int(dep_delay_null_non_cancelled)}"
    )

    print(
        "ARR_DELAY null tra voli non cancellati e non deviati: "
        f"{format_int(arr_delay_null_non_cancelled_non_diverted)}"
    )

    print("\nDEP_DELAY:")
    print(f"  min: {dep_delay_min}")
    print(f"  max: {dep_delay_max}")
    print(f"  count valori negativi: {format_int(dep_delay_negative_count)}")

    print("\nARR_DELAY:")
    print(f"  min: {arr_delay_min}")
    print(f"  max: {arr_delay_max}")
    print(f"  count valori negativi: {format_int(arr_delay_negative_count)}")

    print("\n\nFasce di ritardo DEP_DELAY:")
    print_delay_category_stats(
        "Distribuzione DEP_DELAY per fasce e presenza di cause null:",
        dep_delay_category_stats,
    )

    print("\n\nFasce di ritardo ARR_DELAY:")
    print_delay_category_stats(
        "Distribuzione ARR_DELAY per fasce e presenza di cause null:",
        arr_delay_category_stats,
    )

    print("\n\n" + "=" * 100)
    print("4. CONTROLLI SULLE CAUSE DI RITARDO")
    print("=" * 100)

    print("Null count per ogni causa:")
    for col in DELAY_CAUSE_COLS:
        print(f"  {col}: {format_int(cause_null_counts[col])}")

    print("\nVoli con ARR_DELAY > 0 e tutte le cause null:")
    print(f"  {format_int(arr_delay_positive_all_causes_null)} su {format_int(arr_delay_positive_rows)} voli con ARR_DELAY > 0")
    if arr_delay_positive_rows > 0:
        percentage = arr_delay_positive_all_causes_null / arr_delay_positive_rows * 100
        print(f"  Percentuale: {format_float(percentage)}%")

    print("\nVoli con ARR_DELAY >= 15 e tutte le cause null:")
    print(f"  {format_int(arr_delay_15_all_causes_null)} su {format_int(arr_delay_15_rows)} voli con ARR_DELAY >= 15")
    if arr_delay_15_rows > 0:
        percentage = arr_delay_15_all_causes_null / arr_delay_15_rows * 100
        print(f"  Percentuale: {format_float(percentage)}%")

    print("\nCause negative:")
    for col in DELAY_CAUSE_COLS:
        print(f"  {col}: {format_int(cause_negative_counts[col])}")

    print("\nSomma cause vs ARR_DELAY, considerando solo righe con cause tutte non-null:")
    print("\nCaso ARR_DELAY > 0:")
    print(f"  Righe confrontabili: {format_int(arr_delay_positive_causes_all_not_null)}")
    print(f"  TOTAL_CAUSE_DELAY == ARR_DELAY: {format_int(arr_delay_positive_total_cause_equals_arr_delay)}")
    print(f"  TOTAL_CAUSE_DELAY != ARR_DELAY: {format_int(arr_delay_positive_total_cause_differs_arr_delay)}")

    print("\nCaso ARR_DELAY >= 15:")
    print(f"  Righe confrontabili: {format_int(arr_delay_15_causes_all_not_null)}")
    print(f"  TOTAL_CAUSE_DELAY == ARR_DELAY: {format_int(arr_delay_15_total_cause_equals_arr_delay)}")
    print(f"  TOTAL_CAUSE_DELAY != ARR_DELAY: {format_int(arr_delay_15_total_cause_differs_arr_delay)}")
    print(f"  TOTAL_CAUSE_DELAY < ARR_DELAY: {format_int(arr_delay_15_total_cause_less_arr_delay)}")
    print(f"  TOTAL_CAUSE_DELAY > ARR_DELAY: {format_int(arr_delay_15_total_cause_greater_arr_delay)}")

    if arr_delay_15_total_cause_differs_arr_delay > 0:
        mean_abs_diff = arr_delay_15_diff_abs_sum / arr_delay_15_total_cause_differs_arr_delay
        print(f"  Differenza assoluta media, solo righe diverse: {format_float(mean_abs_diff)}")
        print(f"  Differenza assoluta massima: {format_float(arr_delay_15_diff_abs_max)}")

    print(
        "\nInterpretazione utile: se per ARR_DELAY >= 15 quasi tutte le righe hanno cause valorizzate "
        "e la somma cause coincide con ARR_DELAY, allora trattare i null delle cause come 0 in Q2 "
        "è ragionevole solo per voli senza ritardo rilevante. Se invece molti ARR_DELAY >= 15 hanno cause null, "
        "va motivato con attenzione."
    )

    print("\n\n" + "=" * 100)
    print("5. CONTROLLI SU CRS_DEP_TIME")
    print("=" * 100)

    print(f"CRS_DEP_TIME null: {format_int(crs_dep_time_null)}")
    print(f"CRS_DEP_TIME < 0: {format_int(crs_dep_time_lt_0)}")
    print(f"CRS_DEP_TIME > 2400: {format_int(crs_dep_time_gt_2400)}")
    print(f"CRS_DEP_TIME con minuti invalidi: {format_int(crs_dep_time_invalid_minutes)}")
    print("Nota: esempio invalido: 1267, perché 67 non è un minuto valido.")

    print("\n\n" + "=" * 100)
    print("ESEMPI UTILI")
    print("=" * 100)

    for name, rows in examples.items():
        print(f"\n{name}:")
        if not rows:
            print("  Nessun esempio trovato.")
        else:
            for row in rows:
                print(f"  {row}")

    print("\n\n" + "=" * 100)
    print("TEMPO TOTALE")
    print("=" * 100)
    print(f"Tempo totale analisi: {format_float(elapsed)} secondi")

    if elapsed > 300:
        print(
            "\nATTENZIONE: l'analisi ha superato 5 minuti. "
            "Per analisi ripetute conviene convertire il dataset in Parquet."
        )
    else:
        print(
            "\nOK: sotto i 5 minuti. Per questi controlli esplorativi il CSV è ancora gestibile. "
            "Parquet resta consigliato per le query Spark ripetute."
        )


if __name__ == "__main__":
    main()