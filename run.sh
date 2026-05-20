#!/bin/bash
# Uso: ./run.sh utils.py
#      ./run.sh preprocess.py
#      ./run.sh query1.py

SCRIPT=$1

docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/scripts/$SCRIPT
