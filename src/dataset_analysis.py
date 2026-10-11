"""Analisi esplorativa, limitata alla struttura del dataset e ai valori nulli.

Questo modulo dopo il download, legge il CSV a blocchi, misura la quantità di dati e valuta la completezza delle colonne.
Non svolge attività di pulizia: i valori mancanti non vengono eliminati.

File prodotti (quando save_outputs=True):
* results/dataset_summary.json: statistiche complessive e nomi delle colonne;
* results/missing_values.csv: conteggi e percentuali per colonna;
* results/charts/missing_values.png: grafico dei valori mancanti;
* results/charts/dataset_completeness.png: grafico delle celle presenti/assenti.

Esecuzione da terminale: ``python src/dataset_analysis.py``.
Uso in Jupyter: ``from src.dataset_analysis import analyze_dataset``.
Dipendenze: pandas, matplotlib.
"""

from pathlib import Path
import json

import matplotlib
# Backend non interattivo: permette di creare PNG anche nei container Docker
# senza server grafico. Va impostato PRIMA di importare pyplot.
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

# Percorsi assoluti ricavati dal file, non dalla directory di esecuzione.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_PATH = PROJECT_ROOT / "data" / "original" / "Citywide_Payroll_Data__Fiscal_Year_.csv"
RESULTS_DIR = PROJECT_ROOT / "results"
CHARTS_DIR = RESULTS_DIR / "charts"
# Numero massimo di righe trattate per blocco. Limita l'uso della RAM.
CHUNK_SIZE = 50_000


def generate_charts(missing_values: pd.Series, total_rows: int, total_columns: int, charts_dir: Path = CHARTS_DIR) -> list[Path]:
    """Genera due grafici PNG e restituisce i rispettivi percorsi.

    Parametri
    ---------
    missing_values : pandas.Series
        Numero di valori nulli per ciascuna colonna del CSV.
    total_rows : int
        Numero complessivo di record analizzati.
    total_columns : int
        Numero di colonne del dataset.
    charts_dir : pathlib.Path
        Cartella nella quale salvare i grafici.

    Restituisce
    -----------
    list[pathlib.Path]
        Percorsi dei PNG generati, nell'ordine: distribuzione dei null e
        completezza complessiva.

    La completezza aggregata è riferita alle *celle*, non alla percentuale
    di righe che non presentano valori mancanti.
    """
    charts_dir.mkdir(parents=True, exist_ok=True)

    # Percentuale di righe prive di valore per ogni singola colonna.
    missing_percent = (missing_values / total_rows * 100).sort_values()
    # Nel grafico vengono mostrate solo le colonne che presentano almeno un null.
    missing_percent = missing_percent[missing_percent > 0]

    fig, ax = plt.subplots(figsize=(11, 6))
    if not missing_percent.empty:
        bars = ax.barh(missing_percent.index, missing_percent.values, color="steelblue")
        max_value = float(missing_percent.max())
        ax.set_xlim(0, max_value * 1.2)
        for bar, value in zip(bars, missing_percent.values):
            ax.text(bar.get_width() + max_value * 0.012,
                    bar.get_y() + bar.get_height() / 2,
                    f"{value:.3f}%", va="center", fontsize=9)
    else:
        ax.text(0.5, 0.5, "Nessun valore mancante", ha="center", va="center", transform=ax.transAxes)
    ax.set_title("Valori mancanti per colonna")
    ax.set_xlabel("Percentuale di record (%)")
    ax.set_ylabel("")
    fig.tight_layout()
    fig.savefig(charts_dir / "missing_values.png", dpi=200)
    plt.close(fig)

    # Ogni intersezione riga-colonna è una cella; il denominatore non è
    # quindi il numero delle righe ma il numero totale delle celle.
    total_cells = total_rows * total_columns
    total_missing = int(missing_values.sum())
    valid_cells = total_cells - total_missing

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(
        [valid_cells, total_missing],
        labels=["Valori presenti", "Valori mancanti"],
        autopct="%1.2f%%",
        startangle=90,
        colors=["seagreen", "indianred"],
    )
    ax.set_title("Completezza complessiva del dataset")
    fig.tight_layout()
    fig.savefig(charts_dir / "dataset_completeness.png", dpi=200)
    plt.close(fig)

    print(f"\nGrafici salvati in: {charts_dir}")
    return [charts_dir / "missing_values.png", charts_dir / "dataset_completeness.png"]


def analyze_dataset(dataset_path: Path = DATASET_PATH, results_dir: Path = RESULTS_DIR, *, save_outputs: bool = True) -> dict:
    """Analizza la struttura del CSV e, opzionalmente, salva i risultati.

    Parametri
    ---------
    dataset_path : pathlib.Path
        Percorso del CSV da esaminare.
    results_dir : pathlib.Path
        Directory destinata ai riepiloghi e ai grafici.
    save_outputs : bool
        Se True, salva CSV, JSON e PNG. Se False, restituisce soltanto il
        dizionario delle statistiche (e stampa il riepilogo nel terminale).

    Restituisce
    -----------
    dict
        Dimensione, numero di righe, colonne, valori nulli e percentuale
        di completezza; utilizzabile direttamente nelle celle Jupyter.

    Lancia eccezioni
    -------
    FileNotFoundError
        Se il CSV non è disponibile nel percorso indicato.
    ValueError
        Se il file non contiene record utili per l'analisi.

    Attenzione
    ----------
    ``dtype=str`` evita conversioni numeriche automatiche, ma Pandas
    continua a riconoscere come nulli i valori mancanti secondo le sue
    impostazioni predefinite. La stessa convenzione è usata dal converter.
    """
    dataset_path = Path(dataset_path)
    results_dir = Path(results_dir)
    if not dataset_path.is_file():
        raise FileNotFoundError(f"Dataset non trovato: {dataset_path}")

    if save_outputs:
        results_dir.mkdir(parents=True, exist_ok=True)
    size_bytes = dataset_path.stat().st_size
    total_rows = 0
    missing_values = None
    columns = None

    print("=== DATASET ANALYSIS ===")
    print(f"File: {dataset_path.name}")
    print(f"Dimensione: {size_bytes / 1024**2:.2f} MiB")

    # Scansione incrementale: non vengono accumulati i blocchi in memoria.
    for chunk in pd.read_csv(
        dataset_path,
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False,
    ):
        # Lo schema si acquisisce una sola volta, dal primo blocco.
        if columns is None:
            columns = chunk.columns.tolist()
            missing_values = pd.Series(0, index=columns, dtype="int64")
            print("\nColonne:")
            for column in columns:
                print(f" - {column}")

        total_rows += len(chunk)
        # Vengono sommati i conteggi parziali dei valori nulli di ogni blocco.
        missing_values = missing_values.add(chunk.isna().sum(), fill_value=0).astype("int64")

    if columns is None or missing_values is None or total_rows == 0:
        raise ValueError("Il CSV non contiene record da analizzare.")

    total_columns = len(columns)
    total_cells = total_rows * total_columns
    total_missing = int(missing_values.sum())
    completeness = 100 * (total_cells - total_missing) / total_cells

    print(f"\nNumero righe: {total_rows:,}")
    print(f"Numero colonne: {total_columns}")
    print("\nValori mancanti per colonna:")
    print(missing_values.to_string())
    print(f"\nCompletezza complessiva: {completeness:.2f}%")

    # Tabella in forma lunga, comoda per grafici e successive elaborazioni.
    summary = pd.DataFrame({
        "column": columns,
        "missing_count": [int(missing_values[c]) for c in columns],
        "missing_percent": [float(missing_values[c]) / total_rows * 100 for c in columns],
    })
    if save_outputs:
        summary.to_csv(results_dir / "missing_values.csv", index=False)

    # Dizionario serializzabile in JSON e restituito al chiamante Jupyter.
    metadata = {
        "dataset_file": dataset_path.name,
        "size_bytes": size_bytes,
        "size_mib": round(size_bytes / 1024**2, 2),
        "row_count": total_rows,
        "column_count": total_columns,
        "total_missing_cells": total_missing,
        "completeness_percent": round(completeness, 4),
        "columns": columns,
    }
    metadata["missing_values"] = {col: int(missing_values[col]) for col in columns}
    if save_outputs:
        (results_dir / "dataset_summary.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        generate_charts(missing_values, total_rows, total_columns, results_dir / "charts")
        print(f"Riepiloghi salvati in: {results_dir}")
    return metadata


# L'analisi parte solo quando il file viene eseguito direttamente.
if __name__ == "__main__":
    analyze_dataset()
