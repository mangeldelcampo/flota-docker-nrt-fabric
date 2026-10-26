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

import pandas as pd

dtc_data = [
    {"DTC_Code": "P0420", "Sistema": "Emisiones/Escape", "Descripcion_Averia": "Eficiencia del Catalizador por debajo del umbral", "Gravedad": "Media", "Accion_Requerida": "Revisión en Taller (Fin de Turno)"},
    {"DTC_Code": "P0300", "Sistema": "Motor", "Descripcion_Averia": "Fallo de encendido múltiple/aleatorio", "Gravedad": "Crítica", "Accion_Requerida": "Inmovilización Inmediata"},
    {"DTC_Code": "P0171", "Sistema": "Combustible", "Descripcion_Averia": "Sistema demasiado pobre", "Gravedad": "Alta", "Accion_Requerida": "Paso por taller"},
    {"DTC_Code": "", "Sistema": "OK", "Descripcion_Averia": "Sin averías reportadas", "Gravedad": "OK", "Accion_Requerida": "Operativa Normal"}
]

df_dtc = spark.createDataFrame(pd.DataFrame(dtc_data))
df_dtc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.dim_dtc_codigos")
print("✅ Dimensión de Averías DTC creada con éxito.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
