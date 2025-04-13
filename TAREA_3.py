# UNIVERSIDAD NACIONAL ABIERTA Y A DISTANCIA UNAD

#  Curso: BIGDATA
# Estudiantes: Adrian Armero, Jose Portilla, Leonardo Ramirez

#Este código es desarrollado para tratar, anlizar datos de un dataset relacionado con el ciima en Medellin
#Se realizan tratamiento de valores nulos, transformación de columnas al igual que el EDA, por último se almacena los nuevo datos



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, expr
# Crear sesión de Spark
spark = SparkSession.builder \
	.appName("ClimaMedellinEDA") \
	.getOrCreate()
# Cargar CSV
df = spark.read.csv("historical-weather-medellin.csv", header=True, inferSchema=True)
# Renombrar columnas
df_clean = df \
	.withColumnRenamed("Date (yyyy-mm-dd)", "Fecha") \
	.withColumnRenamed("Max_temperature (°C)", "TemperaturaMaxima") \
	.withColumnRenamed("Min_temperature (°C)", "TemperaturaMinima") \
	.withColumnRenamed("Rain (mm)", "Lluvia") \
	.withColumnRenamed("Wind (km/h)", "Viento") \
	.withColumnRenamed("Description", "Descripcion") \
    .withColumnRenamed("City", "Ciudad")
# Eliminar filas con valores nulos
df_clean = df_clean.na.drop()
# Transformar y agregar columnas
df_transformed = df_clean \
    .withColumn("Fecha", to_date(col("Fecha"), "yyyy-MM-dd")) \
    .withColumn("Año", year("Fecha")) \
    .withColumn("Mes", month("Fecha")) \
    .withColumn("Día", dayofmonth("Fecha")) \
    .withColumn("TemperaturaMedia", expr("(TemperaturaMaxima + TemperaturaMinima) / 2"))
 
# =====================
# ====== EDA ==========
# =====================
 
print(" Temperatura media por mes:")
df_transformed.groupBy("Mes").avg("TemperaturaMedia").orderBy("Mes").show()
 
print(" Promedio de lluvia por año:")
df_transformed.groupBy("Año").avg("Lluvia").orderBy("Año").show()
 
print(" Temperatura máxima y mínima por año:")
df_transformed.groupBy("Año").agg({
    "TemperaturaMaxima": "max",
    "TemperaturaMinima": "min"
}).orderBy("Año").show()
# ===============================
# === Guardar los resultados ===
# ===============================
df_transformed.write.csv("datos_procesados", header=True, mode="overwrite")
# Finalizar Spark
spark.stop()
