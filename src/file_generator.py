"""Generazione riproducibile dei file per il benchmark HDFS e Apache Ozone.

Task 3 – Algoritmi e Strutture Dati per i Big Data.

Il modulo crea file binari di dimensione esatta, con contenuto pseudo-casuale
riproducibile, destinati alle misurazioni di upload e download.

SCELTE METODOLOGICHE
-------------------
* Le dimensioni sono espresse esplicitamente in unità binarie (KiB, MiB, GiB).
* Il contenuto viene generato in blocchi; non si alloca un file da 1 GiB in RAM.
* Si usa una sorgente pseudo-casuale deterministica, così ogni esecuzione con
  lo stesso seed, la stessa dimensione e lo stesso algoritmo produce gli stessi byte.
* I file esistenti vengono riutilizzati solo se dimensione e SHA-256 coincidono
  con quanto atteso. Se non coincidono, sono rigenerati.
* La generazione e il calcolo del checksum sono separati dalla misurazione dei
  tempi di upload/download, per non contaminare i benchmark.
* I file vengono scritti prima su un file temporaneo e sostituiti al termine,
  così un'interruzione non lascia un file finale apparentemente valido.

ATTENZIONE
----------
Questi file servono solo per test prestazionali. Il generatore non è progettato
per produrre casualità crittograficamente sicura. Il contenuto non è costituito
da soli zeri, che potrebbero alterare le prove in presenza di compressione.

Esecuzione da terminale: ``python src/file_generator.py``
Uso da notebook: ``from src.file_generator import generate_benchmark_files``
Dipendenze: solo libreria standard Python.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from pathlib import Path
from typing import Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
OUTPUT_DIR: Final[Path] = PROJECT_ROOT / "data" / "generated"
RESULTS_DIR: Final[Path] = PROJECT_ROOT / "results"

# Unitá binarie: 1 KiB = 1024 byte; 1 GiB = 1024^3 byte.
KIB: Final[int] = 1024
MIB: Final[int] = 1024**2
GIB: Final[int] = 1024**3

# Quattro ordini di grandezza richiesti dalla traccia.
FILE_SIZES: Final[dict[str, int]] = {
    "10KiB": 10 * KIB,
    "50MiB": 50 * MIB,
    "250MiB": 250 * MIB,
    "1GiB": GIB,
}

# Un blocco da 1 MiB limita la memoria e riduce il numero di operazioni di I/O.
CHUNK_SIZE: Final[int] = MIB
DEFAULT_SEED: Final[int] = 20261013


def _sha256(path: Path, chunk_size: int = CHUNK_SIZE) -> str:
    """Calcola l'hash SHA-256 del file leggendolo progressivamente.

    Il digest consentirà, nel successivo benchmark, di confrontare il file
    scaricato da HDFS/Ozone con l'originale locale.
    """
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(chunk_size), b""):
            digest.update(block)
    return digest.hexdigest()


def generate_file(
    path: Path,
    size_bytes: int,
    *,
    seed: int = DEFAULT_SEED,
    chunk_size: int = CHUNK_SIZE,
    overwrite: bool = True,
) -> dict[str, str | int]:
    """Genera un file binario di dimensione esatta.

    Parametri
    ---------
    path : Path
        Percorso del file da produrre.
    size_bytes : int
        Dimensione desiderata, espressa in byte.
    seed : int
        Valore per inizializzare il generatore pseudo-casuale.
    chunk_size : int
        Numero massimo di byte prodotti in memoria per iterazione.
    overwrite : bool
        Se False e il file esiste, viene sollevato FileExistsError.

    Restituisce
    -----------
    dict
        Percorso, dimensione esatta e checksum SHA-256.

    Note
    ----
    ``random.Random.randbytes`` è deterministico per la stessa versione e
    implementazione di Python; per riprodurre gli stessi byte su macchine
    diverse, nella relazione va registrata la versione di Python.
    """
    if size_bytes <= 0:
        raise ValueError("La dimensione del file deve essere positiva")
    if chunk_size <= 0:
        raise ValueError("La dimensione del blocco deve essere positiva")

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(f"File già esistente: {path}")

    generator = random.Random(seed)
    digest = hashlib.sha256()
    temporary = path.with_name(path.name + ".part")

    try:
        with temporary.open("wb") as destination:
            remaining = size_bytes
            while remaining:
                block = generator.randbytes(min(chunk_size, remaining))
                destination.write(block)
                digest.update(block)
                remaining -= len(block)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()

    actual_size = path.stat().st_size
    if actual_size != size_bytes:
        raise RuntimeError(f"Dimensione non corretta per {path}: {actual_size} != {size_bytes}")

    return {"path": str(path), "size_bytes": actual_size, "sha256": digest.hexdigest()}


def generate_benchmark_files(
    output_dir: Path = OUTPUT_DIR,
    results_dir: Path = RESULTS_DIR,
    *,
    sizes: dict[str, int] | None = None,
    seed: int = DEFAULT_SEED,
    force: bool = False,
) -> list[dict[str, str | int]]:
    """Crea i file di test e salva un manifest JSON di verifica.

    Ogni file viene identificato da una dimensione esplicita. Se ``force`` è
    False e il file esiste, il modulo verifica dimensione e SHA-256 rispetto
    al manifest precedente; lo riutilizza solo quando entrambi coincidono.
    In assenza di una verifica attendibile, rigenera il file.

    Il manifest contiene i checksum da usare nel futuro benchmark per la
    verifica dell'integrità dei file scaricati.
    """
    output_dir = Path(output_dir)
    results_dir = Path(results_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    sizes = dict(FILE_SIZES if sizes is None else sizes)

    manifest_path = results_dir / "generated_files.json"
    previous = {}
    if manifest_path.is_file() and not force:
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
            if existing.get("seed") == seed:
                previous = {entry["name"]: entry for entry in existing.get("files", [])}
        except (ValueError, KeyError, TypeError):
            previous = {}

    generated: list[dict[str, str | int]] = []
    for name, size in sizes.items():
        if not name or "/" in name or "\\" in name or name in (".", ".."):
            raise ValueError(f"Nome file non valido: {name!r}")
        if size <= 0:
            raise ValueError(f"Dimensione non valida per {name}: {size}")
        target = output_dir / f"benchmark_{name}.bin"
        old = previous.get(name)
        reusable = (
            not force
            and target.is_file()
            and old is not None
            and old.get("size_bytes") == size
            and target.stat().st_size == size
            and _sha256(target) == old.get("sha256")
        )
        if reusable:
            print(f"[RIUTILIZZATO] {target.name}")
            checksum = old["sha256"]
        else:
            print(f"[GENERAZIONE] {target.name} ({size:,} byte)", flush=True)
            checksum = generate_file(target, size, seed=seed)["sha256"]

        generated.append({
            "name": name,
            "path": str(target.resolve()),
            "size_bytes": size,
            "sha256": str(checksum),
        })

    # Il manifest è un risultato sperimentale: come gli altri risultati può
    # essere rigenerato e rimanere escluso da Git.
    manifest = {"seed": seed, "files": generated}
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nManifest salvato in: {manifest_path}")
    return generated


def main() -> None:
    """Punto di ingresso per l'esecuzione da terminale."""
    files = generate_benchmark_files()
    for item in files:
        print(f"{item['name']:<7} {item['size_bytes']:>12,} byte  SHA-256: {item['sha256']}")


if __name__ == "__main__":
    main()
