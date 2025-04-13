# UNIVERSIDAD NACIONAL ABIERTA Y A DISTANCIA UNAD

#  Curso: BIGDATA
# Estudiantes: Adrian Armero, Jose Portilla, Leonardo Ramirez

#Este código permite consumir los datos que llegan desde el kafka_generator.py al topic, simulando datos en tiempo real


#Se importan las funciones y estructuras necesarias para leer, transformar y procesar los datos en Spark.

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType
 
#Se crea una sesión de Spark llamada SecurityEventsProcessor.
#Se establece el nivel de log a “WARN” para evitar demasiada salida en consola.
 
spark = SparkSession.builder \
    .appName("SecurityEventsProcessor") \
	.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
 
#Se define el esquema del JSON que llega desde Kafka, para poder interpretarlo correctamente.
schema = StructType() \
    .add("timestamp", StringType()) \
	.add("ip", StringType()) \
	.add("event_type", StringType()) \
	.add("username", StringType()) \
	.add("device", StringType())
 
#Se crea un DataFrame de streaming que se conecta a Kafka y escucha el topic seguridad_logs.
df = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9092") \
	.option("subscribe", "seguridad_logs") \
    .load()
 
#Se convierte el valor recibido (JSON en formato texto) en columnas utilizando el esquema definido.
json_df = df.selectExpr("CAST(value AS STRING)") \
	.select(from_json(col("value"), schema).alias("data")) \
	.select("data.*")
 
#Se configuran los resultados para que se impriman en consola a medida que llegan.
#outputMode("append"): se añaden nuevas filas (no se actualizan datos antiguos).
#.awaitTermination(): mantiene el proceso en ejecución mientras llegan nuevos eventos.
 
datos = json_df.writeStream \
	.outputMode("append") \
	.format("console") \
	.option("truncate", False) \
	.start()
datos.awaitTermination()
