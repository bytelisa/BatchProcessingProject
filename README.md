# Batch Processing Project - American Flights Analysis
A project by Valentina Jin and Elisa Marzioli.

## 1. Launch the containers

```bash
docker-compose up -d
docker-compose up -d namenode datanode nifi # per vale (solo nodi nifi e hadoop)
```

## 2. Initialize HDFS directories and permits for NiFi
todo: automatizzare questi step con Docker compose!!

```bash

docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights
docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights/archive
docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights/csv
docker compose exec namenode hdfs dfs -mkdir -p /data/processed/flights/parquet
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

