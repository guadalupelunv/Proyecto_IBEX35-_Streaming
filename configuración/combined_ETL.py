from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, lit, concat, expr, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import json

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("Combined_ETL_IBEX35_and_NEWS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ========================== IBEX35 ETL ==========================
def run_ibex35_etl():
    input_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("company", StringType(), True),
        StructField("points", FloatType(), True)
    ])

    schema_json = {
        "type": "struct",
        "fields": [
            {"field": "timestamp", "type": "int64", "optional": True},
            {"field": "company", "type": "string", "optional": True},
            {"field": "points", "type": "float", "optional": True}
        ],
        "optional": False,
        "name": "IbexPoints"
    }
    schema_str = json.dumps(schema_json)

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.67.10:9094") \
        .option("subscribe", "ibex35") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.select(from_json(col("value").cast("string"), input_schema).alias("data")).select("data.*")

    datos = parsed_df \
        .withColumn("timestamp_trimmed", expr("substring(timestamp, 1, 19)")) \
        .withColumn("timestamp", (unix_timestamp("timestamp_trimmed", "yyyy-MM-dd HH:mm:ss") * 1000).cast("long")) \
        .drop("timestamp_trimmed")

    # Escritura a Kafka
    payload_struct = struct("timestamp", "company", "points")
    json_payload = to_json(payload_struct)

    mensaje_final = datos.select(
        concat(lit('"'), col("company"), lit('"')).alias("key"),
        concat(
            lit('{"schema": '), lit(schema_str), lit(', "payload": '), json_payload, lit('}')
        ).alias("value")
    )

    kafka_query = mensaje_final.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.67.10:9094") \
        .option("topic", "ibex35_ETL") \
        .option("checkpointLocation", "/tmp/checkpoints/ibex35_etl_kafka") \
        .outputMode("append") \
        .start()

    # Escritura a HDFS
    hdfs_query = datos.writeStream \
        .format("parquet") \
        .option("path", "/bda/proyecto/ibex35") \
        .option("checkpointLocation", "/tmp/checkpoints/ibex35_etl_hdfs") \
        .outputMode("append") \
        .start()

    return [kafka_query, hdfs_query]

# ========================== NEWS ETL ==========================
def run_news_etl():
    input_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("company", StringType(), True),
        StructField("content", StringType(), True),
        StructField("scoring", FloatType(), True)
    ])

    schema_json = {
        "type": "struct",
        "fields": [
            {"field": "timestamp", "type": "int64", "optional": True},
            {"field": "company", "type": "string", "optional": True},
            {"field": "content", "type": "string", "optional": True},
            {"field": "scoring", "type": "float", "optional": True}
        ],
        "optional": False,
        "name": "IbexNews"
    }
    schema_str = json.dumps(schema_json)

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.67.10:9094") \
        .option("subscribe", "ibex35-news") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.select(from_json(col("value").cast("string"), input_schema).alias("data")).select("data.*")

    datos = parsed_df \
        .withColumn("timestamp_trimmed", expr("substring(timestamp, 1, 19)")) \
        .withColumn("timestamp", (unix_timestamp("timestamp_trimmed", "yyyy-MM-dd HH:mm:ss") * 1000).cast("long")) \
        .drop("timestamp_trimmed")

    # Escritura a Kafka
    payload_struct = struct("timestamp", "company", "content", "scoring")
    json_payload = to_json(payload_struct)

    mensaje_final = datos.select(
        concat(lit('"'), col("company"), lit('"')).alias("key"),
        concat(
            lit('{"schema": '), lit(schema_str), lit(', "payload": '), json_payload, lit('}')
        ).alias("value")
    )

    kafka_query = mensaje_final.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.67.10:9094") \
        .option("topic", "ibex35-news_ETL") \
        .option("checkpointLocation", "/tmp/checkpoints/ibex35_news_etl_kafka") \
        .outputMode("append") \
        .start()

    # Escritura a HDFS
    hdfs_query = datos.writeStream \
        .format("parquet") \
        .option("path", "/bda/proyecto/news") \
        .option("checkpointLocation", "/tmp/checkpoints/ibex35_news_etl_hdfs") \
        .outputMode("append") \
        .start()

    return [kafka_query, hdfs_query]

# Ejecutar ambos ETLs y esperar su finalización
queries = run_ibex35_etl() + run_news_etl()
for q in queries:
    q.awaitTermination()
