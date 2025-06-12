from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, lit, concat, expr, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import json

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("ETL_IBEX35_NEWS_to_Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Definir esquema de entrada (noticias)
input_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("company", StringType(), True),
    StructField("content", StringType(), True),
    StructField("scoring", FloatType(), True)
])

# 3. Definir el esquema Kafka Connect para las noticias
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

# 4. Leer datos del topic original
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.67.10:9094") \
    .option("subscribe", "ibex35-news") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Parsear JSON y transformar timestamp a Unix timestamp (en ms)
parsed_df = df.select(from_json(col("value").cast("string"), input_schema).alias("data")) \
              .select("data.*")

datos = parsed_df \
    .withColumn("timestamp_trimmed", expr("substring(timestamp, 1, 19)")) \
    .withColumn("timestamp_ms", (unix_timestamp("timestamp_trimmed", "yyyy-MM-dd HH:mm:ss") * 1000).cast("long")) \
    .drop("timestamp") \
    .drop("timestamp_trimmed") \
    .withColumnRenamed("timestamp_ms", "timestamp")

# 6. Construir payload para Kafka Connect
payload_struct = struct("timestamp", "company", "content", "scoring")
json_payload = to_json(payload_struct)

mensaje_final = datos.select(
    concat(lit('"'), col("company"), lit('"')).alias("key"),
    concat(
        lit('{"schema": '), lit(schema_str), lit(', "payload": '), json_payload, lit('}')
    ).alias("value")
)

# 7. Escribir al topic de salida
query = mensaje_final.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.67.10:9094") \
    .option("topic", "ibex35-news_ETL") \
    .option("checkpointLocation", "/tmp/checkpoints/ibex35_news_etl") \
    .outputMode("append") \
    .start()

query.awaitTermination()
