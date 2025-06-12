from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, lit, concat, expr, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import json

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("ETL_IBEX35_to_Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Definir esquema para parsear JSON original
input_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("company", StringType(), True),
    StructField("points", FloatType(), True)
])

# 3. Definir esquema Kafka Connect para el mensaje transformado
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

# 4. Leer datos desde Kafka topic original
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.67.10:9094") \
    .option("subscribe", "ibex35") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Parsear JSON y transformar el timestamp
parsed_df = df.select(from_json(col("value").cast("string"), input_schema).alias("data")) \
              .select("data.*")

datos = parsed_df \
    .withColumn("timestamp_trimmed", expr("substring(timestamp, 1, 19)")) \
    .withColumn("timestamp_ms", (unix_timestamp("timestamp_trimmed", "yyyy-MM-dd HH:mm:ss") * 1000).cast("long")) \
    .drop("timestamp") \
    .drop("timestamp_trimmed") \
    .withColumnRenamed("timestamp_ms", "timestamp")

# 6. Construir payload con esquema Kafka Connect
payload_struct = struct("timestamp", "company", "points")
json_payload = to_json(payload_struct)

mensaje_final = datos.select(
    concat(lit('"'), col("company"), lit('"')).alias("key"), # Se mantiene la key como un string JSON
    concat(
        lit('{"schema": '), lit(schema_str), lit(', "payload": '), json_payload, lit('}')
    ).alias("value")
)

# 7. Escribir al topic Kafka transformado
query = mensaje_final.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.67.10:9094") \
    .option("topic", "ibex35_ETL") \
    .option("checkpointLocation", "/tmp/checkpoints/ibex35_etl") \
    .outputMode("append") \
    .start()

query.awaitTermination()