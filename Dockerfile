# Dockerfile
# Base: stessa immagine ufficiale usata nel docker-compose.yml
FROM apache/spark:3.5.3

# Passa a root per installare pacchetti
USER root

# Installa dipendenze Python usate dagli script Spark/export
# - tdigest: eventuale supporto per percentili/sketch
# - pandas: utility locali/analisi output
# - redis: necessario per export_output_to_redis.py verso Redis Stack
RUN pip install --no-cache-dir \
    tdigest==0.5.2.2 \
    "pandas>=1.4.0" \
    redis==5.0.8

# Torna all'utente spark (default dell'immagine base)
USER spark