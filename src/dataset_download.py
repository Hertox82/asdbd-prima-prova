from pathlib import Path
import kagglehub

# Path per il dataset di New York City Citywide Payroll Data
DATASET_ID = (
    "new-york-city/"
    "nyc-citywide-payroll-data/"
    "versions/1"
)

# Creazione della directory per il dataset
DATASET_DIR = Path("data/original")

# Creazione della directory se non esiste
DATASET_DIR.mkdir(parents=True, exist_ok=True)

# Download del dataset da Kaggle
dataset_path = Path(
    kagglehub.dataset_download(
        DATASET_ID,
        output_dir=str(DATASET_DIR)
    )
)

# Verifica se il dataset è stato scaricato correttamente
print(f"Dataset disponibile in: {dataset_path}")
# Elenco dei file CSV presenti nel dataset
csv_files = sorted(dataset_path.rglob("*.csv"))

# Verifica se sono stati trovati file CSV
if not csv_files:
    raise FileNotFoundError(
        "Nessun file CSV trovato nel dataset"
    )

# Stampa delle informazioni sui file CSV trovati
for csv_file in csv_files:
    size_mb = csv_file.stat().st_size / (1024 ** 2)
    print(f"{csv_file.name}: {size_mb:.2f} MiB")