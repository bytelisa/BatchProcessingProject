# Batch Processing Project - American Flights Analysis
A project by Valentina Jin and Elisa Marzioli.

## 1. Launch the containers

```bash
docker-compose up -d
```

## 2. Initialize HDFS permits
```bash

docker compose exec namenode hdfs dfs -mkdir -p /data/raw/flights
```

```bash
docker compose exec namenode hdfs dfs -chown -R nifi:supergroup /data/raw/flights
```

```bash
docker compose exec namenode hdfs dfs -chmod -R 755 /data/raw/flights
```

## 3. Open NiFi web UI
```bash
http://localhost:9090/nifi/
```

