# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "75be783d-5778-4383-b087-378d019d9c5e",
# META       "default_lakehouse_name": "lh_flota_silver",
# META       "default_lakehouse_workspace_id": "43c901bd-0717-46ee-96f7-562666215b0a",
# META       "known_lakehouses": [
# META         {
# META           "id": "75be783d-5778-4383-b087-378d019d9c5e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# 1. INGESTA Y CREACIÓN DE DIM_PROVEEDORES
# Leemos desde el Lakehouse de Bronce usando la ruta ABFS
ruta_bronce = "abfss://WS_Fabric_Flota_NRT@onelake.dfs.fabric.microsoft.com/lh_flota_bronce.Lakehouse/Files/"

df_proveedores = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ruta_bronce + "dim_proveedores.csv")
df_proveedores.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_proveedores")

# 2. INGESTA Y CREACIÓN DE FACT_MANTENIMIENTOS
df_mantenimientos = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ruta_bronce + "fact_mantenimientos_45vehiculos.csv")
from pyspark.sql.functions import col
# Calculamos el coste total
df_mantenimientos = df_mantenimientos.withColumn("Coste_Total_Intervencion", col("Coste_Materiales_EUR") + col("Coste_ManoObra_EUR"))
df_mantenimientos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_mantenimientos")

# 3. INGESTA Y CREACIÓN DE FACT_CONTROL_JORNADA
df_jornada = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ruta_bronce + "fact_control_jornada.csv")
df_jornada.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_control_jornada")

# 4. INGESTA Y CREACIÓN DE FACT_INCIDENCIAS
df_incidencias = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ruta_bronce + "fact_incidencias.csv")
df_incidencias.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_incidencias")

print("4 Tablas Delta creadas con éxito en la Capa Plata.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr

# 1. Cargamos la tabla de mantenimientos actual
df_mantenimientos = spark.table("fact_mantenimientos")

# 2. Creamos la Fecha de Salida sumando los 'Dias_Inmovilizado' a la 'Fecha_Entrada_Taller'
df_mantenimientos_actualizada = df_mantenimientos.withColumn(
    "Fecha_Salida_Taller", 
    expr("date_add(Fecha_Entrada_Taller, Dias_Inmovilizado)")
)

# 3. Sobrescribimos la tabla para guardar la nueva columna
(
    df_mantenimientos_actualizada.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fact_mantenimientos")
)

print("¡Columna Fecha_Salida_Taller calculada y añadida con éxito a fact_mantenimientos!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
