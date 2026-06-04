# Batch Processing Pipeline — US Flight Data Analysis

**Progetto 1 — SABD 2025/26 | Università Tor Vergata**
Valentina Jin · Elisa Marzioli

---

## Descrizione

Questo progetto presenta una pipeline di batch processing per l’analisi di circa 2,2 milioni di record sui voli aerei 
statunitensi (gennaio–aprile 2025), forniti dal BTS. L’architettura containerizzata tramite Docker Compose orchestra i 
componenti Apache NiFi, HDFS, Apache Spark (PySpark), Redis e Grafana, con Apache Airflow come strumento di orchestrazione. 
Vengono implementate tre query analitiche sui ritardi e le cancellazioni dei voli.

Tre query analitiche sono implementate sia con l'API **DataFrame** che con l'API **RDD** di Spark, per confronto di performance:

| Query | Descrizione                                                                                                                               |
|-------|-------------------------------------------------------------------------------------------------------------------------------------------|
| **Q1** | Statistiche mensili per compagnie aeree AA e DL (ritardi medi/min/max, cancellazioni medi).                                               |
| **Q2** | Top-10 compagnie per ritardo medio, con breakdown delle cause.                                                                            |
| **Q3** | Percentili di ritardo alla partenza (P25/P50/P75/P90) per compagnia e fascia oraria, per AA, DL, UA, WN. Min/Max per ritardo di partenza. |

---

## Architettura

```
URL BTS → NiFi → HDFS (CSV raw)
                     ↓
              Spark Preprocessing → Parquet 
                     ↓
         ┌───────────┼───────────┐
        Q1          Q2          Q3
         └───────────┼───────────┘
                     ↓
                   Redis
                     ↓
                  Grafana (dashboard)
```

**Stack tecnologico:**

| Componente | Versione | Porta |
|---|---|---|
| Apache Spark | 3.5.3 | 8080 (UI), 7077 |
| Apache Hadoop HDFS | 3.2.1 | 9870 (UI), 9000 |
| Apache NiFi | 1.23.2 | 9090 |
| Apache Airflow | 2.9.0 | 8081 |
| Redis | 7.2.0 | 6379 |
| Grafana | 10.4.0 | 3000 |

---

## Prerequisiti

- Docker
- Docker Compose
- WSL2 (se su Windows)

---

## Avvio

### 1. Build e avvio dei container

```bash
docker compose up -d
```

Per avviare solo i servizi Spark (utile per esecuzione manuale degli script):

```bash
docker compose up -d spark-master spark-worker
docker compose ps   # verifica che siano pronti
```

### 2. Inizializzazione HDFS

Da eseguire una sola volta dopo il primo avvio:

```bash
# Struttura delle directory
docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights/archive
docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights/csv
docker compose exec namenode hdfs dfs -mkdir -p /data/processed/flights/parquet
docker compose exec namenode hdfs dfs -mkdir -p /data/output/flights

# Permessi per NiFi
docker compose exec namenode hdfs dfs -chown -R nifi:supergroup /data/raw/flights
docker compose exec namenode hdfs dfs -chmod -R 755 /data/raw/flights

# Permessi per Spark
docker compose exec namenode hdfs dfs -chmod -R 777 /data/processed
docker compose exec namenode hdfs dfs -chmod -R 777 /data/output
docker compose exec namenode hdfs dfs -chmod -R 777 /data/raw/flights/csv
```

Per verificare il contenuto di una directory:

```bash
docker compose exec namenode hdfs dfs -ls /data/raw/flights/csv
```

### 3. NiFi — Ingestione dati

Aprire la UI NiFi nel browser:

```
http://localhost:9090/nifi/
```

Caricare il flow `nifi/flows/sabd-ingest-flights-to-hdfs.json` e avviare il Process Group per scaricare i file `.tar.gz` dal BTS e depositarli su HDFS come CSV.

### 4. Airflow — Pipeline end-to-end

Airflow orchestra l'intera pipeline (NiFi → Spark preprocessing → Query → Redis export).

```bash
chmod +x airflow/start.sh
docker compose up -d airflow
```

La UI Airflow è disponibile su `http://localhost:8081`.

**Comandi utili:**

```bash
# Accedere al container
docker exec -it batchprocessingproject-airflow-1 bash

# Verificare i DAG run
airflow dags list-runs -d sabd_project1_pipeline

# Sospendere il DAG
airflow dags pause sabd_project1_pipeline
```


### 5. Grafana — Visualizzazione

Aprire la UI Grafana su `http://localhost:3000`.

I dashboard per Q1, Q2, Q3 e i benchmark sono pre-configurati e si alimentano da Redis.

---

## Esecuzione manuale degli script Spark

Il file `run.sh` è una shortcut per `spark-submit` nel container del master:

```bash
./run.sh <script.py>
```

**Preprocessing:**

```bash
./run.sh preprocess.py
```

**Query individuali (DataFrame API):**

```bash
./run.sh query1.py
./run.sh query2.py
./run.sh query3.py
```

**Query con RDD API:**

```bash
./run.sh query1_rdd.py
./run.sh query2_rdd.py
./run.sh query3_rdd.py
```

In alternativa, il comando completo `spark-submit`:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/scripts/<script.py>
```

---

## Benchmarking

NOTA: Il `benchmark_rdd_vs_df` misura le performance di DataFrame vs RDD su 20 iterazioni (5 warm-up + 15 valide), variando anche il numero di worker (1, 2, 3, 4).

```bash
# Benchmark DataFrame per Q1, Q2, Q3 con warm-up
./run.sh benchmark_warmup.py

# Benchmark DataFrame vs RDD per Q1, Q2, Q3 con warm-up
./run.sh benchmark_rdd_vs_df.py

# Benchmark specifico per Q3 (percentile_approx vs t-digest) con warm-up
./run.sh benchmark_q3.py

# Benchmark con cold start (15 iterazioni valide)
./run.sh benchmark_coldstart.py
```

Le fasi misurate sono: `loading_s`, `computation_s`, `output_csv_local_s`, `output_csv_hdfs_s`, `end_to_end_s`.

I risultati vengono salvati in `/opt/output/benchmarks/` e poi esportati su Redis per la visualizzazione in Grafana.

**Plot dei risultati:**

```bash
python tools/plot_benchmark_boxplots.py
python tools/plot_benchmark_scaling.py
```

---

## Struttura del progetto

```
BatchProcessingProject/
├── airflow/
│   ├── dags/
│   │   └── sabd_pipeline.py              # DAG Airflow principale
│   ├── logs/                             # Log delle esecuzioni
│   └── start.sh                          # Script di avvio Airflow
├── grafana/
│   ├── dashboards/                       # JSON dei dashboard (Q1, Q2, Q3, benchmark)
│   └── provisioning/                     # Configurazione automatica datasource Redis
├── nifi/
│   ├── flows/                            # Flow NiFi esportati
│   └── hadoop-conf/                      # core-site.xml, hdfs-site.xml per NiFi
├── output/
│   ├── benchmarks/                       # Risultati benchmark in CSV
│   └── query/                            # Risultati delle query in CSV
├── plots/
│   ├── benchmark_plots/                  # Boxplot comparativi DF vs RDD
│   ├── benchmark_tables/                 # Tabelle riassuntive benchmark
│   └── scaling_plots/                    # Grafici di scaling per numero di worker
├── src/
│   ├── benchmark/
│   │   ├── benchmark_coldstart.py        # Benchmark con cold start
│   │   ├── benchmark_config.json         # Parametri del benchmark (JSON)
│   │   ├── benchmark_config.py           # Parametri del benchmark (Python)
│   │   ├── benchmark_q3.py               # Benchmark Q3 (percentile_approx vs t-digest)
│   │   ├── benchmark_rdd_vs_df.py        # Benchmark DataFrame vs RDD
│   │   └── benchmark_warmup.py           # Benchmark con warm-up
│   ├── export/
│   │   ├── export_benchmark_to_redis.py  # Esportazione risultati benchmark → Redis
│   │   └── export_output_to_redis.py     # Esportazione risultati query → Redis
│   ├── query/
│   │   ├── query1.py                     # Q1: statistiche mensili (DataFrame)
│   │   ├── query1_rdd.py                 # Q1: statistiche mensili (RDD)
│   │   ├── query2.py                     # Q2: top-10 airline per ritardo (DataFrame)
│   │   ├── query2_rdd.py                 # Q2: top-10 airline per ritardo (RDD)
│   │   ├── query3.py                     # Q3: percentili orari (DataFrame)
│   │   └── query3_rdd.py                 # Q3: percentili orari (RDD, t-digest)
│   ├── config.py                         # Path HDFS e output centralizzati
│   ├── preprocess.py                     # Preprocessing CSV → Parquet
│   └── utils.py                          # Utilità condivise (SparkSession, costanti)
├── tools/
│   ├── plot_benchmark_boxplots.py        # Boxplot comparativi DF vs RDD
│   ├── plot_benchmark_scaling.py         # Grafici di scaling per numero di worker
│   └── run_worker_benchmarks.py          # Esecuzione benchmark al variare dei worker
├── utility/
│   ├── dataset_quality_check.py          # Analisi qualità dataset BTS
│   ├── check_missing_delay_causes.py
│   ├── check_wn_night_flights.py
│   └── compare_query_outputs.py          # Confronto output DF vs RDD
├── docker-compose.yml
├── Dockerfile                            # Immagine Spark custom (+ tdigest, pandas, redis)
├── hadoop.env                            # Variabili d'ambiente Hadoop
├── run.sh                                # Shortcut spark-submit
└── README.md
```

---

## Note implementative

**Preprocessing:** la colonna `HOUR` viene derivata da `CRS_DEP_TIME` tramite divisione intera per 100. Il Parquet è partizionato per `OP_UNIQUE_CARRIER` per abilitare il partition pruning su tutte le query. I valori NULL nelle colonne di causa ritardo sono trattati come 0 (il BTS li registra solo quando `ARR_DELAY > 15`).

**Q3 — Scelta dell'algoritmo:**
- DataFrame API: `percentile_approx` (algoritmo Greenwald-Khanna, nativo Spark, ottimizzato da Catalyst)
- RDD API: t-digest (`tdigest==0.5.2.2`) via `mapPartitions` + merge manuale con `update_centroids_from_list`

I due metodi producono valori numericamente vicini (differenza < 1 minuto), ma semanticamente diversi: `percentile_approx` restituisce sempre un valore osservato nel dataset, mentre t-digest interpola tra i centroidi.

