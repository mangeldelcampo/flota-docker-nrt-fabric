# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9a85df46-663e-493c-b411-f0941ce7b377",
# META       "default_lakehouse_name": "lh_flota_bronce",
# META       "default_lakehouse_workspace_id": "43c901bd-0717-46ee-96f7-562666215b0a",
# META       "known_lakehouses": [
# META         {
# META           "id": "9a85df46-663e-493c-b411-f0941ce7b377"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import ast
import json
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

# 1. Esquema tabular final de cada parada individual
stop_output_schema = ArrayType(StructType([
    StructField("id_parada", IntegerType(), True),
    StructField("nombre_parada", StringType(), True),
    StructField("distancia_parada", DoubleType(), True),
    StructField("latitud_parada", DoubleType(), True),
    StructField("longitud_parada", DoubleType(), True),
    StructField("sentido", StringType(), True)
]))

# 2. Función de extracción que lee los campos reales de la EMT (stopNum, stopName) en toA y toB
def extraer_paradas_emt(raw_str):
    if not raw_str:
        return []
    
    data = None
    try:
        data = json.loads(raw_str)
    except Exception:
        try:
            data = ast.literal_eval(raw_str)
        except Exception:
            return []
            
    if not data or not isinstance(data, dict):
        return []

    paradas = []
    
    # Extraer paradas de ambos sentidos (toA y toB)
    for sentido_clave in ["toA", "toB"]:
        bloque = data.get(sentido_clave)
        if isinstance(bloque, dict):
            features = bloque.get("features", [])
            for f in features:
                props = f.get("properties", {}) if isinstance(f, dict) else {}
                geom = f.get("geometry", {}) if isinstance(f, dict) else {}
                coords = geom.get("coordinates", []) if isinstance(geom, dict) else []
                
                # Campos reales identificados en la inspección
                stop_id = props.get("stopNum") or props.get("id") or props.get("stopId")
                stop_name = props.get("stopName") or props.get("name")
                distance = props.get("distance")
                
                lon = coords[0] if len(coords) >= 2 else None
                lat = coords[1] if len(coords) >= 2 else None
                
                if stop_id is not None:
                    try:
                        paradas.append((
                            int(stop_id),
                            str(stop_name) if stop_name else None,
                            float(distance) if distance is not None else None,
                            float(lat) if lat is not None else None,
                            float(lon) if lon is not None else None,
                            sentido_clave
                        ))
                    except Exception:
                        continue
                        
    return paradas

# Registrar como UDF
parse_paradas_udf = udf(extraer_paradas_emt, stop_output_schema)

# 3. Lectura del CSV desde Files
path_csv = "Files/raw_batch/lines_detail geojson.csv"

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .load(path_csv)

# 4. Transformar y desanidar paradas
df_parsed = df_raw.filter(col("stops").isNotNull()) \
    .withColumn("paradas_lista", parse_paradas_udf(col("stops")))

df_paradas = df_parsed.select(
    col("label").alias("linea_comercial"),
    col("line").alias("linea_id"),
    col("nameSectionA").alias("cabecera_origen"),
    col("nameSectionB").alias("cabecera_destino"),
    explode(col("paradas_lista")).alias("parada")
).select(
    col("linea_comercial"),
    col("linea_id"),
    col("cabecera_origen"),
    col("cabecera_destino"),
    col("parada.id_parada").alias("id_parada"),
    col("parada.nombre_parada").alias("nombre_parada"),
    col("parada.distancia_parada").alias("distancia_parada"),
    col("parada.latitud_parada").alias("latitud_parada"),
    col("parada.longitud_parada").alias("longitud_parada"),
    col("parada.sentido").alias("sentido")
).dropDuplicates(["linea_comercial", "id_parada", "sentido"])

# 5. Guardar en la tabla Delta administrada de Bronze (Forzando el cambio de esquema)
df_paradas.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_paradas_emt")

print("Tabla Delta 'dim_paradas_emt' generada correctamente.")
display(spark.table("dim_paradas_emt").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("dim_paradas_emt").limit(500))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("dim_paradas_emt").filter("id_parada = 3355").limit(10).show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("dim_paradas_emt").filter("id_parada = 3355").limit(10))  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
