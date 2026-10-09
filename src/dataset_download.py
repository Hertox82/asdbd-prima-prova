"""Acquisizione riproducibile del dataset NYC Citywide Payroll Data.
------------------
Scaricare la versione 1 del dataset pubblico da Kaggle, identificarne i
file CSV e visualizzarne la dimensione prima delle successive analisi.

Lo script può essere utilizzato in due modi:
* da terminale: ``python src/dataset_download.py``;
* dal notebook: ``from src.dataset_download import download_dataset``.

La cartella ``data/original`` è esterna al controllo versione: viene
popolata da questo programma e può essere montata come volume Docker.
La versione del dataset è fissata per evitare cambiamenti non controllati
tra un'esecuzione e l'altra dell'esperimento.

Dipendenza esterna: kagglehub.
"""
from pathlib import Path
import kagglehub

# Risalgo da src/dataset_download.py alla directory radice del progetto.
# In questo modo i percorsi non dipendono dalla cartella da cui si lancia Python.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Destinazione dei file scaricati (persistente se montata come volume Docker).
DATASET_DIR = PROJECT_ROOT / "data" / "original"
# Identificativo del dataset con versione esplicita: NON "latest".
DATASET_ID = "new-york-city/nyc-citywide-payroll-data/versions/1"


def download_dataset(dataset_id: str = DATASET_ID, output_dir: Path = DATASET_DIR) -> list[Path]:
    """Scarica il dataset e restituisce i percorsi dei file CSV individuati.

    Parametri
    ---------
    dataset_id : str
        Identificativo Kaggle, comprensivo della versione da utilizzare.
    output_dir : pathlib.Path
        Directory locale di destinazione. Viene creata, se assente.

    Restituisce
    -----------
    list[pathlib.Path]
        Elenco ordinato dei file CSV presenti nella directory scaricata.

    Lancia eccezioni
    -------
    FileNotFoundError
        Se l'archivio scaricato non contiene alcun file con estensione .csv.

    Note
    ----
    KaggleHub può utilizzare una copia già presente localmente: per questo
    l'output descrive un dataset *disponibile*, non garantisce che sia stato
    effettuato un nuovo download dalla rete.
    """
    # Viene accettato sia un oggetto Path sia una stringa con il percorso.
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    # Il download e l'eventuale estrazione sono gestiti da KaggleHub.
    # Il percorso restituito potrebbe essere diverso da output_dir.
    dataset_path = Path(kagglehub.dataset_download(dataset_id, output_dir=str(output_dir)))
    # Ricerca ricorsiva: il dataset potrebbe contenere sottocartelle o più CSV.
    csv_files = sorted(dataset_path.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Nessun CSV trovato in {dataset_path}")
    print(f"Dataset disponibile in: {dataset_path}")
    for csv_file in csv_files:
        print(f"{csv_file.name}: {csv_file.stat().st_size / 1024**2:.2f} MiB")
    return csv_files


def main() -> None:
    """Punto di ingresso per l'esecuzione dello script da terminale."""
    download_dataset()


# L'importazione dal notebook NON deve avviare automaticamente il download.
if __name__ == "__main__":
    main()
