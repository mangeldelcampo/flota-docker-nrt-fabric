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

# MARKDOWN ********************

# print("--- MANTENIMIENTO PUNTUAL: TABLAS ESTÁTICAS DE BRONCE ---")
# 
# # 1. Consolidar histórico de telemetría previa
# spark.sql("OPTIMIZE lh_flota_bronce.telemetria_cruda_nrt")
# spark.sql("VACUUM lh_flota_bronce.telemetria_cruda_nrt RETAIN 168 HOURS")
# 
# # 2. Consolidar catálogo estático de paradas EMT
# spark.sql("OPTIMIZE lh_flota_bronce.dim_paradas_emt")
# spark.sql("VACUUM lh_flota_bronce.dim_paradas_emt RETAIN 168 HOURS")
# 
# print("✅ Tablas estáticas de Bronce compactadas y limpiadas definitivamente.")

# CELL ********************

print("--- MANTENIMIENTO NOCTURNO: TABLA ACTIVA STREAMING ---")
# 1. Compactar la tabla histórica (la que originó el aviso)
#spark.sql("OPTIMIZE lh_flota_bronce.telemetria_cruda_nrt")
#spark.sql("VACUUM lh_flota_bronce.telemetria_cruda_nrt RETAIN 168 HOURS")

# 1. Compactar micro-archivos de la ingesta continua
spark.sql("OPTIMIZE lh_flota_bronce.telemetria_nrt_v2")

# 2. Purgar archivos de versiones con más de 7 días
spark.sql("VACUUM lh_flota_bronce.telemetria_nrt_v2 RETAIN 168 HOURS")

print("✅ Tabla 'telemetria_nrt_v2' optimizada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
