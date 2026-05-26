## Giustificazione della differenza dei risultati delle due query che riguarda al calcolo di percentile:
I due metodi producono risultati numericamente vicini ma semanticamente diversi. percentile_approx implementa 
un quantile empirico discreto e restituisce sempre un valore osservato nel dataset, comportamento appropriato 
per variabili come DEP_DELAY espresso in minuti interi. t-digest effettua interpolazione lineare tra i centroidi 
dello sketch, restituendo valori decimali che non corrispondono a osservazioni reali. Dal punto di vista della 
precisione, la differenza massima osservata tra i due metodi è inferiore a 1 minuto su tutti i quantili analizzati, 
confermando che entrambi gli approcci sono adeguati per questo tipo di analisi.

## Calcolo percentili: confronto spark approximation e t-digest
Spiegare tradeoff: per un dataset piccolo come il nostro, l'overhead dell'ordinamento di spark approximation è 
sopportabile, rendendo spark approximation una soluzione migliore rispetto a t-digest.
Per un dataset di grandi dimensioni, dove l'ordinamento introdurrebbe un overhead molto elevato, t-digest è 
probabilmente una soluzione più efficiente.

## Grafici e tempo
Il tempo di output deve considerare:
- il tempo di scrittura dei file 
- il tempo di caricamento su hdfs 
- tempo di caricamento da hdfs a redis? -> calcolabile con airflow
- tempo di generazione grafici con grafana? -> calcolabile con airflow

## Export CSV su Redis
Spark produce gli output analitici richiesti e li salva in HDFS come CSV. L’esportazione verso Redis è gestita da un
secondo flow NiFi, coerentemente con il ruolo di NiFi come strumento di data movement tra sistemi. Poiché la versione
NiFi usata non include un processor Redis record-native compatibile, la scrittura su Redis è delegata a uno script 
Python invocato tramite ExecuteStreamCommand. NiFi mantiene il controllo del flow, delle dipendenze e della gestione 
degli errori; lo script implementa solo il mapping specifico dal CSV aggregato alle strutture Redis usate da Grafana.