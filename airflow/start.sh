#!/bin/bash
set -e

echo "=== Fix permessi Docker socket ==="
chmod 666 /var/run/docker.sock || true

echo "=== DB migrate ==="
airflow db migrate

echo "=== Creazione utente admin ==="
airflow users create -u admin -p admin -f Admin -l Admin -r Admin -e admin@example.com 2>/dev/null || echo "Utente gia esistente, continuo..."

echo "=== Avvio scheduler ==="
airflow scheduler &

echo "=== Avvio webserver ==="
exec airflow webserver --port 8080
