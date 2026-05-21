# Project Decisions

## D1 - Ingestion framework
Options considered: NiFi, Kafka, Pulsar, script.
Chosen option: Nifi
Motivation:
Trade-offs:

## D2 - Storage format
Options considered: CSV, Parquet, ORC, Avro.
Chosen option: Parquet
Motivation:
Trade-offs:

## D3 - Data partitioning
Options considered: single dataset, monthly partitioning, daily partitioning.
Chosen option:
Motivation:
Trade-offs:

## D4 - Spark API
Options considered: DataFrame, RDD, Spark SQL optional.
Chosen option: Dataframe, eventualmente confronto con RDD se abbastanza tempo
Motivation:
Trade-offs:

## D5 - Missing values
Fields affected: carrier
Chosen policy: riga scartata
Motivation: invalida
Impact on results:

Fields affected: 
Chosen policy:  
Motivation: 
Impact on results:


## D6 - Percentile computation
Options considered: exact sort, percentile_approx, t-digest, KLL.
Chosen option:
Motivation:
Trade-offs:

## D7 - Performance methodology
Runs:
Metrics:
Cold/warm cache policy:
Spark configuration: