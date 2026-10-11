"""Benchmark riproducibile di upload/download HDFS e Apache Ozone.

Eseguire nel container Jupyter, non sull'host: `python -m src.benchmark`.
I client WebHDFS e S3 Gateway sono quelli di `src.storage` già collaudati.
Il controllo di integrità SHA-256 avviene FUORI dalle finestre cronometrate.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .file_generator import FILE_SIZES, generate_benchmark_files
from .storage import HDFSClient, OzoneClient
from .storage.smoke_test import sha256_file
from .storage_analysis import snapshot, compare, HDFS_DATA, HDFS_META, OZONE_DATA, OZONE_META, _sum_allocated

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"
COLUMNS = ["run_id", "timestamp_utc", "size_label", "size_bytes", "system", "repeat", "upload_seconds", "download_seconds", "upload_mib_s", "download_mib_s", "sha256", "verified", "remote_path", "storage_before_bytes", "storage_after_bytes", "storage_delta_bytes", "storage_cumulative_bytes", "storage_data_delta_bytes", "storage_metadata_delta_bytes", "storage_overhead_pct"]
SUMMARY_COLUMNS = ["run_id", "size_label", "size_bytes", "system", "completed", "upload_mean_s", "upload_median_s", "upload_stdev_s", "download_mean_s", "download_median_s", "download_stdev_s", "upload_mean_mib_s", "download_mean_mib_s"]


@dataclass(frozen=True)
class BenchmarkConfig:
    """Parametri di misura: le quattro dimensioni sono sempre quelle previste."""
    repetitions: int = 5
    results_dir: Path = RESULTS_DIR
    hdfs_prefix: str = "/benchmark/performance"
    ozone_prefix: str = "performance"
    ozone_bucket: str = "asdbd-benchmark"
    clean_remote: bool = False
    settle_seconds: float = 2.0


def _save_csv_row(path: Path, columns: list[str], row: dict) -> None:
    """Aggiunge una misurazione e scarica il buffer per limitare perdite da crash."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        if stream.tell() == 0:
            writer.writeheader()
        writer.writerow(row)
        stream.flush()


def _load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def _check_inputs() -> dict[str, dict]:
    """Verifica/genera i file originali con il modulo già sviluppato."""
    entries = generate_benchmark_files()
    indexed = {str(e["name"]): e for e in entries}
    if set(indexed) != set(FILE_SIZES):
        raise ValueError("Manifest generatore incompleto")
    for label, expected_size in FILE_SIZES.items():
        item = indexed[label]
        local = Path(str(item["path"]))
        if local.stat().st_size != expected_size:
            raise ValueError(f"Dimensione non valida: {local}")
        if sha256_file(local) != item["sha256"]:
            raise ValueError(f"SHA-256 origine errato: {local}")
    return indexed


def _stats(values: list[float]) -> tuple[float, float, float]:
    return (statistics.mean(values), statistics.median(values), statistics.stdev(values) if len(values) > 1 else 0.0)


def summarize_results(run_id: str, *, results_dir: Path = RESULTS_DIR) -> list[dict]:
    """Salva una riga per ciascuna combinazione sistema/dimensione."""
    raw_file = Path(results_dir) / f"benchmark_{run_id}_raw.csv"
    rows = [r for r in _load_rows(raw_file) if r["run_id"] == run_id and r["verified"] == "True"]
    report: list[dict] = []
    for label, size in FILE_SIZES.items():
        for system in ("HDFS", "Ozone"):
            items = [r for r in rows if r["size_label"] == label and r["system"] == system]
            if not items:
                continue
            ups = [float(r["upload_seconds"]) for r in items]
            downs = [float(r["download_seconds"]) for r in items]
            up_mean, up_median, up_std = _stats(ups)
            down_mean, down_median, down_std = _stats(downs)
            report.append({
                "run_id": run_id, "size_label": label, "size_bytes": size,
                "system": system, "completed": len(items),
                "upload_mean_s": round(up_mean, 6), "upload_median_s": round(up_median, 6), "upload_stdev_s": round(up_std, 6),
                "download_mean_s": round(down_mean, 6), "download_median_s": round(down_median, 6), "download_stdev_s": round(down_std, 6),
                "upload_mean_mib_s": round(sum(float(r["upload_mib_s"]) for r in items) / len(items), 4),
                "download_mean_mib_s": round(sum(float(r["download_mib_s"]) for r in items) / len(items), 4),
            })
    destination = Path(results_dir) / f"benchmark_{run_id}_summary.csv"
    with destination.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(report)
    return report


def plot_results(run_id: str, *, results_dir: Path = RESULTS_DIR) -> list[Path]:
    """Genera due grafici PNG senza modificare lo stile globale di matplotlib."""
    import matplotlib.pyplot as plt

    summary = summarize_results(run_id, results_dir=results_dir)
    destinations: list[Path] = []
    for metric, title, suffix in (
        ("upload_mean_s", "Upload: tempo medio", "upload"),
        ("download_mean_s", "Download: tempo medio", "download"),
    ):
        fig, ax = plt.subplots(figsize=(9, 5))
        x = list(range(len(FILE_SIZES)))
        width = 0.36
        for system, shift in (("HDFS", -width / 2), ("Ozone", width / 2)):
            selected = {(r["size_label"], r["system"]): r for r in summary}
            values = [float(selected.get((label, system), {}).get(metric, math.nan)) for label in FILE_SIZES]
            ax.bar([i + shift for i in x], values, width, label=system)
        ax.set_xticks(x, list(FILE_SIZES))
        ax.set_ylabel("Secondi (media)")
        ax.set_xlabel("Dimensione file")
        ax.set_title(title)
        ax.legend()
        ax.grid(axis="y", alpha=0.25)
        fig.tight_layout()
        destination = Path(results_dir) / f"benchmark_{run_id}_{suffix}.png"
        fig.savefig(destination, dpi=150)
        plt.close(fig)
        destinations.append(destination)
    return destinations


def run_benchmark(config: BenchmarkConfig | None = None, *, run_id: str | None = None) -> dict:
    """Esegue l'intero benchmark 4×2×N e produce CSV, JSON e grafici.

    Passare lo stesso run_id per riprendere una sessione interrotta. In tal caso
    non ripete le misure completate e verificate; la configurazione è controllata.
    I file remoti sono univoci per esecuzione/dimensione/ripetizione.
    """
    config = config or BenchmarkConfig()
    if config.clean_remote:
        raise ValueError("Per l’analisi cumulativa usare clean_remote=False: i file devono restare nello storage")
    if config.settle_seconds < 0:
        raise ValueError("settle_seconds deve essere >= 0")
    if config.repetitions < 1:
        raise ValueError("Le ripetizioni devono essere >= 1")
    results_dir = Path(config.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    if not all(ch.isalnum() or ch in "-_" for ch in run_id):
        raise ValueError("run_id ammette soltanto caratteri alfanumerici, - e _")
    metadata_file = results_dir / f"benchmark_{run_id}_config.json"
    raw_file = results_dir / f"benchmark_{run_id}_raw.csv"
    identity = {"run_id": run_id, "repetitions": config.repetitions, "file_sizes": FILE_SIZES,
                "hdfs_prefix": config.hdfs_prefix, "ozone_prefix": config.ozone_prefix,
                "ozone_bucket": config.ozone_bucket, "clean_remote": config.clean_remote,
                "settle_seconds": config.settle_seconds, "schema_version": 2}
    if metadata_file.exists():
        if json.loads(metadata_file.read_text(encoding="utf-8")) != identity:
            raise ValueError("Configurazione diversa da quella del run salvato; scegliere un altro run_id")
    elif raw_file.exists():
        raise ValueError("CSV esistente senza metadati di configurazione: uso del run_id non sicuro")
    else:
        metadata_file.write_text(json.dumps(identity, indent=2), encoding="utf-8")

    print("Controllo/generazione dei file originali (fuori dal benchmark)...", flush=True)
    inputs = _check_inputs()
    previous = _load_rows(raw_file)
    # Snapshot complessivo prima di nuove scritture, fuori dalle finestre cronometrate.
    # Per riprendere una sessione, la baseline è conservata nel file JSON dedicato.
    baseline_file = results_dir / f"benchmark_{run_id}_storage_baseline.json"
    if baseline_file.exists():
        baseline = json.loads(baseline_file.read_text(encoding="utf-8"))
    else:
        if previous:
            raise RuntimeError("Baseline storage mancante per una sessione già avviata: impossibile ricostruire il cumulativo")
        baseline = snapshot()
        baseline_file.write_text(json.dumps(baseline, indent=2), encoding="utf-8")
    # Tutti i volumi sono già suddivisi per sistema; il delta cumulativo
    # sottrae la baseline della stessa piattaforma.
    def used(snap: dict, system: str) -> int:
        names = HDFS_DATA + HDFS_META if system == "HDFS" else OZONE_DATA + OZONE_META
        return _sum_allocated(snap, names)
    completed = {(r["size_label"], r["system"], int(r["repeat"]))
                 for r in previous if r.get("run_id") == run_id and r.get("verified") == "True"}
    hdfs = HDFSClient()
    ozone = OzoneClient(bucket=config.ozone_bucket)
    try:
        ozone.ensure_bucket()  # fuori dal cronometro
        hdfs.mkdir(config.hdfs_prefix)
        total = len(FILE_SIZES) * 2 * config.repetitions
        for label, size_bytes in FILE_SIZES.items():
            item = inputs[label]
            source = Path(str(item["path"]))
            expected_hash = str(item["sha256"])
            for repeat in range(1, config.repetitions + 1):
                # Alternanza delle piattaforme per mitigare un bias d'ordine fisso.
                systems = ("HDFS", "Ozone") if repeat % 2 else ("Ozone", "HDFS")
                for system in systems:
                    if (label, system, repeat) in completed:
                        print(f"[RIPRESA] {system} {label} rip. {repeat}: già completata")
                        continue
                    remote_name = f"{run_id}/{label}/rip_{repeat}.bin"
                    remote = (f"{config.hdfs_prefix.rstrip('/')}/{remote_name}" if system == "HDFS"
                              else f"{config.ozone_prefix.strip('/')}/{remote_name}")
                    local_copy = results_dir / "downloads" / run_id / f"{system}_{label}_{repeat}.bin"
                    local_copy.parent.mkdir(parents=True, exist_ok=True)
                    client = hdfs if system == "HDFS" else ozone
                    print(f"[TEST] {system:5} {label:6} rip. {repeat}/{config.repetitions}", flush=True)
                    try:
                        # Non sovrascriviamo oggetti esistenti, neanche in caso di ripresa.
                        # Un upload concluso ma senza riga CSV va segnalato: non può
                        # diventare una nuova misurazione di occupazione per lo stesso oggetto.
                        if client.exists(remote):
                            raise RuntimeError(f"Oggetto già presente ma ciclo non registrato: {remote}. Non sovrascrivere; usare un nuovo run_id o verificare il run interrotto.")
                        before = snapshot()
                        # Include la richiesta e il trasferimento; esclude l'hashing.
                        start = time.perf_counter()
                        client.upload(source, remote)
                        upload_s = time.perf_counter() - start
                        # Le misure di allocazione non sono incluse nei tempi.
                        # Attesa fissa per limitare l'effetto degli aggiornamenti asincroni.
                        if config.settle_seconds:
                            time.sleep(config.settle_seconds)
                        after = snapshot()
                        measured = compare(before, after, system, size_bytes)
                        cumulative = used(after, system) - used(baseline, system)
                        start = time.perf_counter()
                        client.download(remote, local_copy)
                        download_s = time.perf_counter() - start
                        if local_copy.stat().st_size != size_bytes or sha256_file(local_copy) != expected_hash:
                            raise RuntimeError(f"Integrità non verificata: {system} {label} ripetizione {repeat}")
                        row = {
                            "run_id": run_id, "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                            "size_label": label, "size_bytes": size_bytes, "system": system, "repeat": repeat,
                            "upload_seconds": round(upload_s, 6), "download_seconds": round(download_s, 6),
                            "upload_mib_s": round(size_bytes / (1024**2) / upload_s, 4),
                            "download_mib_s": round(size_bytes / (1024**2) / download_s, 4),
                            "sha256": expected_hash, "verified": True,
                            "remote_path": remote,
                            "storage_before_bytes": used(before, system),
                            "storage_after_bytes": used(after, system),
                            "storage_delta_bytes": measured["total_delta_allocated_bytes"],
                            "storage_cumulative_bytes": cumulative,
                            "storage_data_delta_bytes": measured["data_delta_allocated_bytes"],
                            "storage_metadata_delta_bytes": measured["metadata_delta_allocated_bytes"],
                            "storage_overhead_pct": measured["overhead_estimate_pct"],
                        }
                        _save_csv_row(raw_file, COLUMNS, row)
                        completed.add((label, system, repeat))
                        print(f"       upload={upload_s:.3f}s download={download_s:.3f}s checksum=OK ({len(completed)}/{total})", flush=True)
                    finally:
                        # Il cleanup non deve mascherare un eventuale errore sperimentale.
                        local_copy.unlink(missing_ok=True)
                        local_copy.with_name(local_copy.name + ".part").unlink(missing_ok=True)
                        # I file remoti restano conservati per il confronto dello spazio.
                        # Non cancellare in questa fase: verrebbero alterati i cumulativi.
        summary = summarize_results(run_id, results_dir=results_dir)
        plots = plot_results(run_id, results_dir=results_dir)
        plots.extend(plot_storage_results(run_id, results_dir=results_dir))
        return {"run_id": run_id, "raw_csv": str(raw_file),
                "summary_csv": str(results_dir / f"benchmark_{run_id}_summary.csv"),
                "plots": [str(p) for p in plots], "summary": summary}
    finally:
        hdfs.close()


def plot_storage_results(run_id: str, *, results_dir: Path = RESULTS_DIR) -> list[Path]:
    """Grafici dell'occupazione cumulativa osservata, distinti per sistema.

    I valori comprendono dati + metadati sui rispettivi volumi, rispetto
    alla baseline iniziale. Non sono una misura fisica esatta del disco host.
    """
    import matplotlib.pyplot as plt
    rows = [r for r in _load_rows(Path(results_dir) / f"benchmark_{run_id}_raw.csv")
            if r.get("verified") == "True" and r.get("run_id") == run_id]
    output = []
    for system in ("HDFS", "Ozone"):
        selected = [r for r in rows if r["system"] == system]
        if not selected:
            continue
        x = list(range(1, len(selected) + 1))
        y = [int(r["storage_cumulative_bytes"]) / 1024**2 for r in selected]
        fig, ax = plt.subplots(figsize=(10, 4.5))
        ax.plot(x, y, marker="o")
        ax.set_xticks(x)
        ax.set_xlabel("Upload completati (ordine del benchmark)")
        ax.set_ylabel("Incremento cumulativo stimato (MiB allocati)")
        ax.set_title(f"{system}: occupazione cumulativa vs baseline")
        ax.grid(alpha=.25)
        fig.tight_layout()
        destination = Path(results_dir) / f"benchmark_{run_id}_storage_{system.lower()}.png"
        fig.savefig(destination, dpi=150)
        plt.close(fig)
        output.append(destination)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark completo HDFS/Ozone: 4 file × 2 sistemi × ripetizioni")
    parser.add_argument("--repetitions", type=int, default=5, help="Ripetizioni per file e sistema (predefinito: 5)")
    parser.add_argument("--run-id", help="Riprende un run con lo stesso identificativo")
    args = parser.parse_args()
    result = run_benchmark(BenchmarkConfig(repetitions=args.repetitions), run_id=args.run_id)
    print("\nCOMPLETATO:", json.dumps({k: v for k, v in result.items() if k != "summary"}, indent=2))


if __name__ == "__main__":
    main()
