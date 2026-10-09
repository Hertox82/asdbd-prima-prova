"""Conversione del CSV Payroll di New York in JSON, Parquet e Avro.

Questo modulo implementa il confronto dell'occupazione di spazio richiesto
nel Task 2. Produce tre file equivalenti dal punto di vista dei record e
un riepilogo delle dimensioni rispetto al CSV originale.

SCELTE METODOLOGICHE
-------------------
* Il CSV originale non viene modificato.
* Ogni colonna è trattata come stringa nullable, senza interpretazioni
  automatiche di date o importi monetari.
* I null riconosciuti da Pandas vengono rappresentati come null nei
  formati di destinazione.
* JSON viene prodotto come unico array JSON valido, scritto progressivamente
  senza caricare l’intero dataset in memoria.
* Parquet usa compression="NONE"; il suo encoding interno può comunque
  ridurre lo spazio. Avro usa codec="null" (nessuna compressione).
* I dati vengono processati a blocchi per limitare il consumo di memoria.

Lo script verifica il numero di record, ma NON confronta singolarmente
ogni valore tra il CSV e tutti i formati generati: il limite è dichiarato
perché rilevante nella discussione metodologica dell'esperimento.

Esecuzione: ``python src/converters.py``.
Uso Jupyter: ``from src.converters import run_conversions``.
Dipendenze: pandas, pyarrow, fastavro, matplotlib, ijson.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import fastavro
import ijson
import matplotlib
# Backend Matplotlib non interattivo, compatibile con container senza display.
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Directory e file risolti dalla posizione dello script, indipendenti dal cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = PROJECT_ROOT / "data" / "original" / "Citywide_Payroll_Data__Fiscal_Year_.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "converted"
RESULTS_DIR = PROJECT_ROOT / "results"
CHARTS_DIR = RESULTS_DIR / "charts"
# Scansioni incrementali di 50.000 righe per controllare la RAM impiegata.
CHUNK_SIZE = 50_000

JSON_PATH = OUTPUT_DIR / "payroll.json"
PARQUET_PATH = OUTPUT_DIR / "payroll.parquet"
AVRO_PATH = OUTPUT_DIR / "payroll.avro"


def read_csv_chunks():
    """Restituisce un iteratore di blocchi CSV con le convenzioni concordate.

    La lettura è differita: i dati vengono caricati quando si itera sul
    risultato. Il comportamento dei valori nulli coincide con quello
    utilizzato da ``analyze_dataset``.
    """
    return pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, dtype=str, low_memory=False)


def convert_all(csv_path: Path = CSV_PATH, output_dir: Path = OUTPUT_DIR, chunk_size: int = CHUNK_SIZE) -> tuple[int, list[str], dict[str, int]]:
    """Converte il CSV in tre formati utilizzando una sola scansione.

    Parametri
    ---------
    csv_path : pathlib.Path
        File CSV di origine, che rimane invariato.
    output_dir : pathlib.Path
        Directory di destinazione dei tre file.
    chunk_size : int
        Numero di righe da elaborare in ciascun blocco.

    Restituisce
    -----------
    tuple[int, list[str], dict[str, int]]
        Numero di record convertiti, elenco dei nomi delle colonne
        originali e dizionario dei null per colonna.

    Lancia eccezioni
    ----------------
    FileNotFoundError
        Se manca il CSV di origine.
    ValueError
        Se il CSV è privo di record o presenta colonne duplicate.
    RuntimeError
        Se i file generati non hanno lo stesso numero di record.

    Nota: i file di output esistenti vengono sovrascritti. In caso di
    interruzione può rimanere un file incompleto, da rigenerare.
    """
    csv_path = Path(csv_path)
    output_dir = Path(output_dir)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV non trovato: {csv_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "payroll.json"
    parquet_path = output_dir / "payroll.parquet"
    avro_path = output_dir / "payroll.avro"

    # Il primo blocco serve anche per ricavare i nomi delle colonne.
    chunks = iter(pd.read_csv(csv_path, chunksize=chunk_size, dtype=str, low_memory=False))
    try:
        first_chunk = next(chunks)
    except StopIteration as exc:
        raise ValueError("CSV senza record: conversione non eseguita") from exc

    columns = first_chunk.columns.tolist()
    if len(columns) != len(set(columns)):
        raise ValueError("Nomi di colonna duplicati; serve una gestione esplicita")

    # Schema esplicito di Parquet: stessa sequenza di campi del CSV,
    # ciascuno con tipo string e possibilità di essere nullo.
    arrow_schema = pa.schema([pa.field(col, pa.string(), nullable=True) for col in columns])
    # I nomi dei campi Avro non possono contenere spazi: creiamo nomi validi
    # conservando quello originale come metadato dello schema.
    avro_names = []
    for i, col in enumerate(columns):
        base = re.sub(r"[^A-Za-z0-9_]", "_", col).strip("_")
        if not base or base[0].isdigit():
            base = f"field_{base}"
        avro_names.append(f"field_{i:02d}_{base}")
    # In Avro sono ammessi nomi di campo limitati: viene associato quindi un nome
    # stabile e valido a ogni colonna. Viene conservato il nome CSV nell'attributo
    # custom "original_column" dello schema per renderlo rintracciabile.
    avro_schema = {
        "type": "record",
        "name": "PayrollRecord",
        "fields": [
            {"name": safe, "type": ["null", "string"], "default": None,
             "original_column": original}
            for safe, original in zip(avro_names, columns)
        ],
    }
    parsed_avro_schema = fastavro.parse_schema(avro_schema)
    row_count = 0
    first_json_record = True
    null_counts = {col: 0 for col in columns}

    def record_stream(json_file, parquet_writer):
        """Produce record Avro mentre scrive lo stesso blocco in JSON/Parquet.

        Il generatore evita di creare tre copie complete del dataset in RAM.
        Viene consumato da fastavro.writer, che guida quindi la scansione.
        """
        nonlocal row_count, first_json_record
        from itertools import chain

        for chunk in chain([first_chunk], chunks):
            # dtype=str preserva i valori come testo e mantiene i NA come null.
            if chunk.columns.tolist() != columns:
                raise ValueError("Le colonne del CSV sono cambiate durante la lettura")

            nulls = chunk.isna().sum()
            for col in columns:
                null_counts[col] += int(nulls[col])

            # Pandas serializza ogni blocco come un piccolo array JSON.
            # Ne rimuoviamo solo le parentesi esterne e uniamo i blocchi
            # tramite virgole: il risultato è UN SOLO array JSON valido.
            # L'operazione è incrementale e non richiede di tenere in RAM
            # tutti i 2,2 milioni di record contemporaneamente.
            json_chunk = chunk.to_json(
                orient="records", force_ascii=False
            )
            if len(chunk) > 0:
                if not first_json_record:
                    json_file.write(",\n")
                json_file.write(json_chunk[1:-1])
                first_json_record = False
            # PyArrow converte il DataFrame secondo lo schema esplicito.
            parquet_writer.write_table(
                pa.Table.from_pandas(chunk, schema=arrow_schema, preserve_index=False)
            )

            # Converte NaN/NA in None per il formato Avro.
            for values in chunk.itertuples(index=False, name=None):
                yield {
                    safe: None if pd.isna(value) else value
                    for safe, value in zip(avro_names, values)
                }

            row_count += len(chunk)
            print(f"Record convertiti: {row_count:,}", flush=True)

    # La scrittura Avro consuma lo stream e, nello stesso ciclo, genera JSON e Parquet.
    with (
        json_path.open("w", encoding="utf-8", newline="\n") as json_file,
        pq.ParquetWriter(parquet_path, arrow_schema, compression="NONE") as parquet_writer,
        avro_path.open("wb") as avro_file,
    ):
        # Le parentesi vengono scritte una sola volta all'inizio e alla fine.
        # In mezzo si inseriscono tutti i record JSON, separati da virgole.
        json_file.write("[\n")
        fastavro.writer(
            avro_file,
            parsed_avro_schema,
            record_stream(json_file, parquet_writer),
            codec="null",
        )
        json_file.write("\n]\n")

    # Controllo quantitativo dei file effettivamente scritti su disco.
    # Non è un controllo esaustivo dell'uguaglianza di ogni singolo campo.
    print("\nVerifica dei record scritti...")
    parquet_rows = pq.ParquetFile(parquet_path).metadata.num_rows
    # JSON tradizionale non può essere contato per linee: le righe NON
    # corrispondono ai record. ijson analizza l'array in streaming, valida
    # la sintassi JSON e conta gli oggetti senza caricare 1 GiB in RAM.
    with json_path.open("rb") as fh:
        json_rows = sum(1 for _ in ijson.items(fh, "item"))
    with avro_path.open("rb") as fh:
        avro_rows = sum(1 for _ in fastavro.reader(fh))

    if not (row_count == parquet_rows == json_rows == avro_rows):
        raise RuntimeError(
            "Numeri di record diversi: "
            f"CSV={row_count}, JSON={json_rows}, Parquet={parquet_rows}, Avro={avro_rows}"
        )
    print(f"Verifica OK: {row_count:,} record in ogni formato (JSON valido)")
    return row_count, columns, null_counts


def save_results(row_count: int, columns: list[str], null_counts: dict[str, int], csv_path: Path = CSV_PATH, output_dir: Path = OUTPUT_DIR, results_dir: Path = RESULTS_DIR) -> dict:
    """Misura le dimensioni dei file e genera tabelle e grafico comparativi.

    Parametri
    ---------
    row_count : int
        Numero di record validato nella fase di conversione.
    columns : list[str]
        Nomi originali delle colonne, nell'ordine di partenza.
    null_counts : dict[str, int]
        Numero di valori nulli per colonna durante la lettura CSV.
    csv_path, output_dir, results_dir : pathlib.Path
        Percorsi dei dati originali, dei dati convertiti e dei risultati.

    Restituisce
    -----------
    dict
        Riepilogo serializzabile con dimensioni, metodologia e percorsi.

    I risultati sono salvati in ``results/format_comparison.csv``,
    ``results/format_comparison.json`` e nel relativo grafico PNG.
    Una differenza percentuale negativa significa risparmio rispetto al CSV.
    """
    csv_path, output_dir, results_dir = Path(csv_path), Path(output_dir), Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    charts_dir = results_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "CSV": csv_path,
        "JSON": output_dir / "payroll.json",
        "Parquet": output_dir / "payroll.parquet",
        "Avro": output_dir / "payroll.avro",
    }
    # Per confrontare i formati usiamo i byte sul disco: MiB = byte / 2^20.
    csv_bytes = csv_path.stat().st_size
    sizes = {name: path.stat().st_size for name, path in paths.items()}
    rows = [
        {
            "format": name,
            "file": str(paths[name].relative_to(PROJECT_ROOT)) if paths[name].is_relative_to(PROJECT_ROOT) else str(paths[name]),
            "size_bytes": size,
            "size_mib": round(size / 1024**2, 3),
            "difference_vs_csv_percent": round((size / csv_bytes - 1) * 100, 2),
        }
        for name, size in sizes.items()
    ]

    with (results_dir / "format_comparison.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    # Viene registrato nel report le configurazioni dell'esperimento: sono
    # indispensabili per interpretare i numeri durante la discussione.
    report = {
        "record_count": row_count,
        "columns": columns,
        "null_counts": null_counts,
        "methodology": {
            "text_fields": "string nullable per tutte le colonne",
            "json": "Array JSON UTF-8 non compresso, senza indentazione",
            "parquet": "Parquet compression=NONE (encoding interno ammesso)",
            "avro": "Avro codec=null; nomi dei campi normalizzati e nomi originali negli attributi dello schema",
            "csv": "file originale invariato",
            "pandas_na": "valori vuoti e token NA riconosciuti con impostazioni default di pandas",
        },
        "files": rows,
    }
    (results_dir / "format_comparison.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Visualizzazione delle dimensioni: la scala è in MiB, non MB decimali.
    fig, ax = plt.subplots(figsize=(9, 5))
    labels = list(sizes)
    values = [sizes[name] / 1024**2 for name in labels]
    bars = ax.bar(labels, values)
    ax.bar_label(bars, fmt="%.1f MiB", padding=3)
    ax.set_ylim(0, max(values) * 1.15)
    ax.set_ylabel("Dimensione (MiB)")
    ax.set_title("Dimensione del dataset per formato — senza compressione esterna")
    fig.tight_layout()
    fig.savefig(charts_dir / "format_comparison.png", dpi=200)
    plt.close(fig)

    print("\n=== CONFRONTO DIMENSIONI ===")
    for item in rows:
        print(f"{item['format']:<12} {item['size_mib']:>10.3f} MiB   {item['difference_vs_csv_percent']:>+8.2f}% vs CSV")
    print(f"\nRisultati in: {results_dir}")
    return report


def run_conversions(csv_path: Path = CSV_PATH, output_dir: Path = OUTPUT_DIR, results_dir: Path = RESULTS_DIR) -> dict:
    """Coordina conversione, verifica e report in un'unica chiamata.

    È la funzione da richiamare nel notebook: restituisce il dizionario
    finale senza richiedere la lettura manuale dei file di risultati.
    L'esecuzione ripetuta rigenera tutti gli output della conversione.
    """
    row_count, columns, null_counts = convert_all(csv_path, output_dir)
    return save_results(row_count, columns, null_counts, csv_path, output_dir, results_dir)


def main() -> None:
    """Punto di ingresso per chi usa lo script direttamente da terminale."""
    run_conversions()


# Evita che la conversione parta automaticamente durante un import Jupyter.
if __name__ == "__main__":
    main()
