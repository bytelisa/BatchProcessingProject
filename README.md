# Batch Processing Project - American Flights Analysis
Un progetto di Valentina Jin ed Elisa Marzioli.

Requisiti:
- Docker
- Docker Compose

Avvio dei container:
## 1. Launch the containers

```bash
docker-compose up -d
docker-compose up -d namenode datanode nifi # per vale (solo nodi nifi, hadoop)
docker compose up -d namenode datanode nifi spark-master spark-worker
```

## 2. Initialize HDFS directories and permits for NiFi
todo: automatizzare questi step con Docker compose!!

```bash

docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights
docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights/archive
docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights/csv
docker compose exec namenode hdfs dfs -mkdir -p /data/processed/flights/parquet
```

Per controllare il contenuto delle cartelle
```bash
 docker compose exec namenode hdfs dfs -ls /data/raw/flights/csv
```

```bash
docker compose exec namenode hdfs dfs -chown -R nifi:supergroup /data/raw/flights
```

```bash
docker compose exec namenode hdfs dfs -chmod -R 755 /data/raw/flights
```
And for Spark as well:
```bash
docker compose exec namenode hdfs dfs -mkdir -p /data/processed/flights
docker compose exec namenode hdfs dfs -chmod -R 777 /data/processed
```

## 3. Open NiFi web UI
```bash
http://localhost:9090/nifi/
```

## 4. Spark
Attivo due nodi Spark
```bash
docker compose up -d spark-master spark-worker # per vale 
docker compose ps # per verificare se sono pronti
```

Comando per avviare utilis.py
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/scripts/utils.py
```
oppure
```bash
./run.sh utils.py
```

## 5. Airflow (pipeline end-to-end)
Comando per avviare airflow
```bash
chmod +x airflow/start.sh
docker compose up -d airflow
```
Comando per dare i permessi per usare socket docker senza fermare esecuzione (non persistente)
La versione persistente è risolto con start.sh
```bash
docker compose exec --user root airflow chmod 666 /var/run/docker.sock
```

# Entra nel container
```bash
docker exec -it batchprocessingproject-airflow-1 bash
```

# Lista i DAG run attivi
```bash
airflow dags list-runs -d sabd_project1_pipeline
```

# Ferma un DAG run specifico
```bash
airflow dags pause sabd_project1_pipeline
```

# Oppure marca come failed un task specifico
```bash
airflow tasks failed sabd_project1_pipeline spark_preprocessing <execution_date>
```
## 6. Esecuzione preprocess manuale
```bash
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/scripts/preprocess.py
```

## 7. Esecuzione query manuale
```bash
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/scripts/query1.py
```

