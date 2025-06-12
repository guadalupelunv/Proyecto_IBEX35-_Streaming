# Big Data Aplicado
## Proyecto Final

### Objetivo general

Desarrollar un sistema completo Big Data que permita monitorizar, procesar y analizar la evolución diaria del índice bursátil IBEX 35, correlacionando sus fluctuaciones con el análisis de sentimiento económico en noticias relevantes, con el fin de generar insights sobre la influencia mediática en los mercados.

### Origen de los datos

| Fuente | Tipo de dato | Frecuencia |
| ------ | ------------ | ---------- |
| Datos sintéticos | Cotizaciones diarias de las 35 empresas del IBEX 35 por segundo | Streaming |
| Datos sintéticos | Noticias económicas o relevantes para las empresas con scoring | Streaming |

### Flujo general del sistema
1. Ingesta
- Se inicia el programa de generación de los datos sintéticos.
- Se recogen noticias económicas usando la API GDELT.
- Los datos se publican en Kafka para permitir procesamiento posterior batch o streaming.

2. Procesamiento ETL IBEX 35
Aplica lógica de transformación:
- Cálculo de variación por segundo o por ventana.
- Agrupaciones por empresa.
- Cálculo de media móvil, máximos/mínimos, etc.
- Se genera una tabla combinada por empresa y día con:
    - Variación bursátil
    - Número de noticias
    - Sentimiento medio (–1 a +1)
- Toda la información procesada se guarda en Parquet sobre HDFS.

3. Business Intelligence / Visualización
Se construyen dashboards en Grafana, mostrando:
- Fluctuaciones del IBEX 35 por empresa
- Sentimiento medio de las noticias diarias
- Relación entre sentimiento y comportamiento bursátil (correlaciones, tendencias)
- Alertas cuando sentimiento negativo precede caídas del mercado


### Gráficos

- Línea de variación % vs. sentimiento medio
- Histograma de noticias positivas/negativas por empresa

Ejemplo:

| Empresa | Variación diaria | Sentimiento medio | Noticias |
| ------- | ---------------- | ----------------- | -------- |
| BBVA    | +1.2 %           | Positivo (0.7)    | 15       |
| Telefónica | –0.4 %	     | Negativo (–0.2)   | 8        |
| Iberdrola	| +0.8 %	     | Neutral (0.1)     | 6        |



### PRODUCER IBEX35

### TOPICS

1. Crear los topics necesarios.

```
bin/kafka-topics.sh --create --topic ibex35 --bootstrap-server 192.168.67.10:9094 --replication-factor 2 --partitions 2
bin/kafka-topics.sh --create --topic ibex35_ETL --bootstrap-server 192.168.67.10:9094 --replication-factor 2 --partitions 2
bin/kafka-topics.sh --create --topic ibex35-news --bootstrap-server 192.168.67.10:9094 --replication-factor 2 --partitions 2
bin/kafka-topics.sh --create --topic ibex35-news_ETL --bootstrap-server 192.168.67.10:9094 --replication-factor 2 --partitions 2


bin/kafka-topics.sh --delete --topic ibex35_ETL --bootstrap-server 192.168.67.10:9094

bin/kafka-topics.sh --list --bootstrap-server 192.168.67.10:9094
```



### PASOS

1. Generar los IDs.

```
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
CONTROLLER_1_UUID="$(bin/kafka-storage.sh random-uuid)"
```

2. Borramos los logs si es necesario.

```
rm -rf /opt/kafka/practica1_GLV/logs/controller1/*
rm -rf /opt/kafka/practica1_GLV/logs/broker1/*
rm -rf /opt/kafka/practica1_GLV/logs/broker2/*
```

3. Damos formato al controller y los 2 brokers.

```
bin/kafka-storage.sh format --cluster-id ${KAFKA_CLUSTER_ID} \
                     --initial-controllers "1@192.168.67.10:9093:${CONTROLLER_1_UUID}" \
                     --config /opt/kafka/proyecto_final_GLV/config/controller1.properties

bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /opt/kafka/proyecto_final_GLV/config/broker1.properties

bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /opt/kafka/proyecto_final_GLV/config/broker2.properties
```

4. Iniciamos el controller y brokers en ventanas diferentes.

```
bin/kafka-server-start_ejemplo2_mon_kafka.sh /opt/kafka/proyecto_final_GLV/config/controller1.properties
bin/kafka-server-start_ejemplo2_mon_kafka.sh /opt/kafka/proyecto_final_GLV/config/broker1.properties
bin/kafka-server-start_ejemplo2_mon_kafka.sh /opt/kafka/proyecto_final_GLV/config/broker2.properties
bin/connect-distributed.sh /opt/kafka/proyecto_final_GLV/config/worker1.properties
```

5. En /opt/prometheus iniciamos prometheus.
```
./prometheus --config.file=prometheus_ejemplo2_mon_kafka.yml
```

6. Iniciamos Grafana.

```
systemctl start grafana-server
```

7. Iniciamos el generador de datos.

```
python3 consumer.py
```

8. Ejecutamos la orden para que spark empiece a procesar los datos.
```
$SPARK_HOME/bin/spark-submit \
  --master spark://192.168.67.10:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  IBEX35ETL.py

$SPARK_HOME/bin/spark-submit \
  --master spark://192.168.67.10:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  news_ETL.py


  $SPARK_HOME/bin/spark-submit \
  --master spark://192.168.67.10:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  combined_ETL.py

```

Para conectarse a la base de datos PostgreSQL desde la terminal, puedes usar el siguiente comando:

 ```
 curl -X POST -H "Content-Type: application/json" --data @/opt/kafka/proyecto_final_GLV/config/postgre-sink.json http://192.168.67.10:8083/connectors

 curl -X POST -H "Content-Type: application/json" --data @/opt/kafka/proyecto_final_GLV/config/postgre-sink_news.json http://192.168.67.10:8083/connectors


 curl -X DELETE http://192.168.67.10:8083/connectors/postgres-sink-ibex-points
 curl -X DELETE http://192.168.67.10:8083/connectors/postgres-sink-ibex-news

 ```

 bin/kafka-console-consumer.sh --topic ibex35 --from-beginning --bootstrap-server 192.168.67.10:9094

 psql -h 192.168.67.10 -U hadoop -d ibex_db