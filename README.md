# Sistemi di gestione dei dati e storage distribuito

**Elaborato sperimentale — Algoritmi e Strutture Dati per i Big Data, Modulo 2**

## Obiettivo

Il repository contiene un laboratorio riproducibile per l'analisi di un dataset pubblico, il confronto tra formati di serializzazione e la valutazione sperimentale delle operazioni di caricamento e lettura su **Apache Hadoop HDFS** e **Apache Ozone**.

L'esperimento comprende la generazione di file di dimensioni differenti, misurazioni ripetute dei tempi di trasferimento, verifiche di integrità, lettura dei metadati e osservazione dell'incremento di spazio occupato nei volumi Docker. L'ambiente è destinato a una singola macchina con risorse limitate e non rappresenta un cluster di produzione.

## Organizzazione del repository

La struttura logica principale è la seguente:

```text
.
├── docker-compose.yml             # Definizione dei servizi del laboratorio
├── docker/                        # Configurazioni e immagini di supporto
│   ├── hdfs/                      # Configurazione Hadoop/HDFS
│   ├── jupyter/                   # Ambiente Python/Jupyter
│   └── storage-metrics/           # Servizio di rilevazione dei volumi, se integrato
├── requirements.txt              # Dipendenze Python
├── notebooks/
│   ├── 00_PROVA_COMPLETA.ipynb    # Notebook principale di presentazione
│   └── test/
│       ├── benchmark_storage.ipynb                      # Benchmark tecnico: 5 ripetizioni
│       ├── benchmark_storage_3_ripetizioni_esempio.ipynb # Benchmark isolato: 3 ripetizioni
│       └── test_storage.ipynb                          # Test diagnostici dello storage
├── src/
│   ├── dataset_download.py        # Acquisizione dataset
│   ├── dataset_analysis.py        # Analisi esplorativa
│   ├── converters.py              # Conversione CSV/JSON/Parquet/Avro
│   ├── file_generator.py          # Generazione file e SHA-256
│   ├── benchmark.py               # Esecuzione e aggregazione misure
│   ├── storage_analysis.py        # Snapshot e variazioni di occupazione
│   ├── storage_metadata.py        # Metadati HDFS e Ozone
│   └── storage/                    # Client WebHDFS/S3 e controlli funzionali
├── data/                          # Dataset e file generati in esecuzione
└── results/                       # CSV, JSON e grafici prodotti in esecuzione
```

Le directory `data/` e `results/` sono popolate dagli script; i relativi contenuti non sono necessari per il funzionamento iniziale del progetto e possono essere esclusi dal versionamento Git. Devono invece essere disponibili i moduli `src/`, i notebook, le configurazioni Docker e le dipendenze.

## Ambiente sperimentale

| Componente | Configurazione |
|---|---|
| Hadoop HDFS | Versione 3.5.0; 1 NameNode e 3 DataNode |
| Apache Ozone | Versione 2.2.1; SCM, OM, 1 DataNode, S3 Gateway e Recon |
| Replica | 1 per entrambi i sistemi |
| Accesso a HDFS | WebHDFS tramite HTTP |
| Accesso a Ozone | API compatibili S3 tramite `boto3` |
| Orchestrazione | Docker Compose |
| Ambiente di analisi | JupyterLab in un container dedicato |
| Persistenza | Volumi Docker distinti per i componenti di storage |

La scelta di una configurazione contenuta deriva dai vincoli di CPU, memoria e disco di una postazione individuale. I tre DataNode HDFS consentono di osservare la collocazione dei blocchi su nodi distinti, mentre la configurazione minimale di Ozone limita il consumo complessivo di risorse. Il fattore di replica unitario riduce lo spazio richiesto e rende omogeneo il numero di copie mantenute, **senza rendere identiche le topologie**.

I benchmark misurano pertanto il comportamento delle **specifiche configurazioni sperimentali**, includendo le differenze tra WebHDFS e S3 Gateway; non stabiliscono una superiorità prestazionale generale di una tecnologia.

## Requisiti

- Docker Desktop con Docker Compose (su Windows è utilizzabile il backend WSL2), oppure Docker Engine con plugin Compose.
- Risorse sufficienti per i container Hadoop, Ozone e Jupyter; per il laboratorio è stata utilizzata una postazione con **16 GB di RAM fisica**.
- Spazio libero nell'ordine di diverse decine di GB per file locali, volumi e ripetizioni del benchmark.
- Connessione Internet al primo avvio per scaricare immagini, dipendenze e dataset pubblico.

Non è richiesta un'installazione locale separata di Hadoop, Ozone o Jupyter.

## Avvio dell'ambiente

I comandi devono essere eseguiti **dalla directory principale del repository**, dove si trova `docker-compose.yml`.

**1. Verifica della configurazione**

```powershell
docker compose config
```

**2. Creazione e avvio dei servizi**

```powershell
docker compose up -d --build
```

Al primo avvio possono essere necessari alcuni minuti per lo scaricamento delle immagini e l'inizializzazione dei servizi. Occorre attendere che i cluster siano operativi prima di eseguire le misure.

**3. Controllo dei container e dei DataNode**

```powershell
docker compose ps -a
docker compose exec namenode hdfs dfsadmin -report
docker compose exec scm ozone admin datanode list
```

Nel report HDFS sono attesi **3 DataNode attivi**; per Ozone è atteso **1 DataNode in stato HEALTHY**. È inoltre necessario che sia disponibile il servizio interno di misurazione dello storage previsto dal benchmark (`storage-metrics`), qualora sia definito nel Compose corrente.

**4. Accesso a JupyterLab**

Aprire nel browser:

**<http://localhost:8888/lab>**

Nel file browser di Jupyter selezionare:

`notebooks/00_PROVA_COMPLETA.ipynb`

La directory del repository è montata nel container Jupyter in `/home/jovyan/work`; l'URL `localhost` è riferito al browser della macchina host, mentre tra container si utilizzano i nomi dei servizi Docker.

**5. Esecuzione della prova**

Nel notebook selezionare **Restart Kernel and Run All Cells**. Il notebook principale esegue, nell'ordine, acquisizione e analisi del dataset, conversioni, generazione dei file, benchmark, verifica dei metadati e visualizzazione dei risultati. Le misure vengono prodotte durante la sessione corrente e non richiedono risultati salvati da prove precedenti.

Il download del dataset richiede l'accesso a Internet alla prima esecuzione. L'intero esperimento può richiedere diversi minuti, in particolare per i trasferimenti da 1 GiB.

### Interfacce di consultazione

| Servizio | Indirizzo sul computer host | Funzione |
|---|---|---|
| **JupyterLab** | <http://localhost:8888/lab> | Esecuzione del notebook principale |
| HDFS NameNode | <http://localhost:9870> | Stato HDFS e informazioni sui file |
| Ozone Manager | <http://localhost:9874> | Interfaccia di gestione Ozone |
| Ozone Recon | <http://localhost:9888> | Monitoraggio Ozone |
| Ozone S3 Gateway | `http://localhost:9878` | Endpoint API S3, non interfaccia grafica |

Le porte del laboratorio sono pubblicate sull'interfaccia locale del computer. Eventuali credenziali dimostrative sono destinate esclusivamente a questo ambiente non produttivo.

## Notebook ed esperimenti

Il repository contiene **quattro notebook**, suddivisi tra la prova principale e le verifiche tecniche di supporto.

| Notebook | Scopo | Modalità |
|---|---|---|
| `notebooks/00_PROVA_COMPLETA.ipynb` | **Presentazione integrale**: acquisizione e analisi del dataset, conversioni, generazione dei file, benchmark HDFS/Ozone, verifica SHA-256, occupazione e metadati | **3 ripetizioni**, 24 cicli di upload/download |
| `notebooks/test/benchmark_storage.ipynb` | Benchmark tecnico isolato, finalizzato al confronto dei tempi di trasferimento e delle misure dello storage | **5 ripetizioni**, 40 cicli di upload/download |
| `notebooks/test/benchmark_storage_3_ripetizioni_esempio.ipynb` | Variante del benchmark isolato con durata ridotta, utile per il collaudo della procedura | **3 ripetizioni**, 24 cicli di upload/download |
| `notebooks/test/test_storage.ipynb` | Notebook diagnostico dedicato alle verifiche dei sistemi di storage | Test di supporto; non richiesto per la prova principale |

Per l'esecuzione dell'elaborato è sufficiente avviare **`notebooks/00_PROVA_COMPLETA.ipynb`**. I notebook in `notebooks/test/` sono facoltativi e non devono essere lanciati contemporaneamente al benchmark principale: l'esecuzione separata evita interferenze nelle misurazioni e un ulteriore consumo di spazio nei volumi.

### Dataset e conversioni

Viene utilizzato il dataset pubblico **NYC Citywide Payroll Data**, ottenuto tramite il modulo di download. L'analisi comprende numerosità dei record, struttura tabellare e valori mancanti. A partire dal CSV originale vengono prodotti i formati **JSON, Parquet e Avro**; le dimensioni vengono confrontate adottando le opzioni di codifica documentate nel codice.

### Test di trasferimento e integrità

Per ciascun sistema sono previsti file da **10 KiB, 50 MiB, 250 MiB e 1 GiB**. Il benchmark principale esegue **3 ripetizioni per dimensione e sistema** (4 × 2 × 3 = **24 cicli**). Ogni ciclo misura separatamente upload e download e verifica tramite **SHA-256** l'integrità del contenuto recuperato. I file remoti ricevono nomi univoci per esecuzione e ripetizione.

I tempi misurati non includono le operazioni di interrogazione dei metadati e di campionamento dell'occupazione dei volumi. I risultati vengono aggregati per dimensione e sistema.

### Verifica dei metadati

Per **HDFS** vengono esaminati esistenza del file, dimensione, fattore di replica, dimensione dei blocchi e localizzazione effettiva dei blocchi sui DataNode. Per **Ozone** vengono verificati bucket, key, presenza dell'oggetto, dimensione e metadati restituiti da S3. L'ETag non è utilizzato come garanzia generale di integrità: tale controllo è affidato a SHA-256.

La mappatura fisica dei container e dei chunk Ozone non viene dedotta dai metadati S3, che non espongono direttamente queste informazioni.

## Risultati prodotti

Le elaborazioni generano, tra gli altri, i seguenti artefatti:

```text
results/
├── dataset_summary.json
├── missing_values.csv
├── format_comparison.csv
├── format_comparison.json
├── generated_files.json
├── benchmark_<run_id>_config.json
├── benchmark_<run_id>_raw.csv
├── benchmark_<run_id>_summary.csv
├── benchmark_<run_id>_storage_baseline.json
├── benchmark_<run_id>_metadata.json
├── benchmark_<run_id>_upload.png
├── benchmark_<run_id>_download.png
├── benchmark_<run_id>_storage_hdfs.png
├── benchmark_<run_id>_storage_ozone.png
└── benchmark_<run_id>_hdfs_block_distribution.png
```

`<run_id>` identifica la singola esecuzione; gli artefatti non devono essere preparati in anticipo. Ulteriori grafici esplorativi possono essere generati dai moduli di analisi. Il dataset originale e i file convertiti sono conservati sotto `data/`.

Le misure di occupazione rappresentano la **variazione dei byte allocati nei volumi Docker rispetto a una fotografia iniziale**. Possono comprendere allocazioni e attività interne dei servizi e non devono essere interpretate automaticamente come dimensione fisica attribuibile al singolo oggetto.

## Arresto e ripristino

Per arrestare il laboratorio conservando i dati:

```powershell
docker compose down
```

Per eliminare **anche i volumi gestiti dal progetto** e ripartire con uno storage inizialmente vuoto:

```powershell
docker compose down -v --remove-orphans
```

**Attenzione:** il secondo comando cancella definitivamente i dati di HDFS e Ozone presenti nei volumi Compose. Non elimina normalmente i file locali `data/` e `results/` presenti nella cartella del repository.

## Limiti e riproducibilità

Le misurazioni sono influenzate dalle risorse condivise della macchina ospitante, dalle diverse topologie (tre DataNode HDFS e uno Ozone), dalle interfacce WebHDFS e S3 e dallo stato iniziale dei sistemi. Per contenere tali influenze, i trasferimenti vengono eseguiti sequenzialmente, il contenuto dei file viene mantenuto identico nei due sistemi e ogni nuova sessione utilizza identificativi distinti.

La riproducibilità riguarda **la procedura e le verifiche**, non l'identità assoluta dei tempi: risultati numerici diversi possono emergere in esecuzioni successive a causa dello stato del sistema e della variabilità delle risorse disponibili.
