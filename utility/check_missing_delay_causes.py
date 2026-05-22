from pathlib import Path
import pandas as pd
import time

# Cartella contenente i 4 CSV
DATA_DIR = Path(r"C:\Users\elisa\IdeaProjects\BatchProcessingProject\data")

# Colonna di ritardo da analizzare.
# Per le cause di ritardo BTS di solito ha più senso ARR_DELAY, perché le cause spiegano il ritardo in arrivo.
# Se vuoi controllare il ritardo in partenza, cambia in "DEP_DELAY".
DELAY_COL = "ARR_DELAY"

DELAY_COMPONENTS = [
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
]

USE_COLS = [DELAY_COL] + DELAY_COMPONENTS

CHUNKSIZE = 200_000


def analyze_file(csv_path: Path) -> dict:
    total_rows = 0
    positive_delay_rows = 0
    positive_delay_all_causes_null = 0
    positive_delay_at_least_one_cause = 0

    examples = []

    for chunk in pd.read_csv(
        csv_path,
        usecols=USE_COLS,
        chunksize=CHUNKSIZE,
        low_memory=False,
    ):
        total_rows += len(chunk)

        # Converte a numerico: stringhe vuote o valori non validi diventano NaN
        for col in USE_COLS:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        positive_delay_mask = chunk[DELAY_COL] > 0
        causes_all_null_mask = chunk[DELAY_COMPONENTS].isna().all(axis=1)

        target_mask = positive_delay_mask & causes_all_null_mask

        positive_delay_rows += int(positive_delay_mask.sum())
        positive_delay_all_causes_null += int(target_mask.sum())
        positive_delay_at_least_one_cause += int((positive_delay_mask & ~causes_all_null_mask).sum())

        # Salva qualche esempio, utile per verificare manualmente
        if len(examples) < 10:
            sample = chunk.loc[target_mask, USE_COLS].head(10 - len(examples))
            examples.extend(sample.to_dict(orient="records"))

    percentage = (
        positive_delay_all_causes_null / positive_delay_rows * 100
        if positive_delay_rows > 0
        else 0.0
    )

    return {
        "file": csv_path.name,
        "total_rows": total_rows,
        "positive_delay_rows": positive_delay_rows,
        "positive_delay_all_causes_null": positive_delay_all_causes_null,
        "positive_delay_at_least_one_cause": positive_delay_at_least_one_cause,
        "percentage_positive_delay_with_all_causes_null": percentage,
        "examples": examples,
    }


def main():
    start = time.perf_counter()

    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"Nessun CSV trovato in: {DATA_DIR}")

    print(f"Trovati {len(csv_files)} file CSV.")
    print(f"Analisi su colonna ritardo: {DELAY_COL}")
    print("-" * 80)

    global_total_rows = 0
    global_positive_delay_rows = 0
    global_positive_delay_all_causes_null = 0
    global_positive_delay_at_least_one_cause = 0

    all_results = []

    for csv_file in csv_files:
        file_start = time.perf_counter()

        result = analyze_file(csv_file)
        all_results.append(result)

        file_elapsed = time.perf_counter() - file_start

        global_total_rows += result["total_rows"]
        global_positive_delay_rows += result["positive_delay_rows"]
        global_positive_delay_all_causes_null += result["positive_delay_all_causes_null"]
        global_positive_delay_at_least_one_cause += result["positive_delay_at_least_one_cause"]

        print(f"File: {result['file']}")
        print(f"  Righe totali: {result['total_rows']:,}")
        print(f"  Righe con {DELAY_COL} > 0: {result['positive_delay_rows']:,}")
        print(
            f"  Righe con {DELAY_COL} > 0 e tutte le cause NULL: "
            f"{result['positive_delay_all_causes_null']:,}"
        )
        print(
            f"  Percentuale sui ritardi positivi: "
            f"{result['percentage_positive_delay_with_all_causes_null']:.2f}%"
        )
        print(f"  Tempo file: {file_elapsed:.2f} secondi")
        print("-" * 80)

    global_percentage = (
        global_positive_delay_all_causes_null / global_positive_delay_rows * 100
        if global_positive_delay_rows > 0
        else 0.0
    )

    elapsed = time.perf_counter() - start

    print("\nRISULTATO GLOBALE")
    print("=" * 80)
    print(f"Righe totali analizzate: {global_total_rows:,}")
    print(f"Righe con {DELAY_COL} > 0: {global_positive_delay_rows:,}")
    print(
        f"Righe con {DELAY_COL} > 0 e tutte le cause NULL: "
        f"{global_positive_delay_all_causes_null:,}"
    )
    print(
        f"Righe con {DELAY_COL} > 0 e almeno una causa registrata: "
        f"{global_positive_delay_at_least_one_cause:,}"
    )
    print(
        f"Percentuale ritardi positivi con cause tutte NULL: "
        f"{global_percentage:.2f}%"
    )
    print(f"Tempo totale: {elapsed:.2f} secondi")

    print("\nESEMPI TROVATI")
    print("=" * 80)

    printed_any = False
    for result in all_results:
        if result["examples"]:
            printed_any = True
            print(f"\nFile: {result['file']}")
            for row in result["examples"]:
                print(row)

    if not printed_any:
        print("Nessun esempio trovato: non ci sono righe con ritardo positivo e tutte le cause NULL.")


if __name__ == "__main__":
    main()