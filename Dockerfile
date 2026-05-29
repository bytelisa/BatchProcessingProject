# Dockerfile
# Base: stessa immagine ufficiale usata nel docker-compose.yml
FROM apache/spark:3.5.3

# Passa a root per installare pacchetti
USER root

# Installa tdigest (e pandas, già presente ma lo forziamo aggiornato)
# --break-system-packages è necessario su immagini con Python gestito dal sistema
RUN pip install --no-cache-dir \
    tdigest==0.5.2.2 \
    pandas>=1.4.0

# Torna all'utente spark (default dell'immagine base)
USER spark