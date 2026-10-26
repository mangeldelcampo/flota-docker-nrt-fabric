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

# MARKDOWN ********************

# print("--- CONSOLIDACIÓN PUNTUAL: TABLAS BATCH EN CAPA PLATA ---")
# 
# tablas_batch = [
#     "lh_flota_silver.dim_conductores",
#     "lh_flota_silver.dim_vehiculos_tecnica",
#     "lh_flota_silver.dim_proveedores",
#     "lh_flota_silver.dim_calendario",
#     "lh_flota_silver.fact_mantenimientos",
#     "lh_flota_silver.fact_control_jornada",
#     "lh_flota_silver.fact_incidencias",
#     "lh_flota_silver.fact_checklist_diario",
# ]
# 
# for tabla in tablas_batch:
#   print(f"Optimizando {tabla}...")
#   spark.sql(f"OPTIMIZE {tabla}")
#   spark.sql(f"VACUUM {tabla} RETAIN 168 HOURS")
# 
# print("✅ Todas las tablas estáticas de la Capa Plata han sido consolidadas.")

# MARKDOWN ********************

# spark.sql("OPTIMIZE lh_flota_silver.dim_conductores")
# spark.sql("VACUUM lh_flota_silver.dim_conductores RETAIN 168 HOURS")
# 
# spark.sql("OPTIMIZE lh_flota_silver.dim_vehiculos_tecnica")
# spark.sql("VACUUM lh_flota_silver.dim_vehiculos_tecnica RETAIN 168 HOURS")
# 
# spark.sql("OPTIMIZE lh_flota_silver.dim_proveedores")
# spark.sql("VACUUM lh_flota_silver.dim_proveedores RETAIN 168 HOURS")
# 
# spark.sql("OPTIMIZE lh_flota_silver.dim_calendario")
# spark.sql("VACUUM lh_flota_silver.dim_calendario RETAIN 168 HOURS")
# 
# spark.sql("OPTIMIZE lh_flota_silver.fact_mantenimientos")
# spark.sql("VACUUM lh_flota_silver.fact_mantenimientos RETAIN 168 HOURS")
# 
# spark.sql("OPTIMIZE lh_flota_silver.fact_control_jornada")
# spark.sql("VACUUM lh_flota_silver.fact_control_jornada RETAIN 168 HOURS")
# 
# spark.sql("OPTIMIZE lh_flota_silver.fact_incidencias")
# spark.sql("VACUUM lh_flota_silver.fact_incidencias RETAIN 168 HOURS")
# 
# spark.sql("OPTIMIZE lh_flota_silver.fact_checklist_diario")
# spark.sql("VACUUM lh_flota_silver.fact_checklist_diario RETAIN 168 HOURS")


# CELL ********************

print("--- MANTENIMIENTO NOCTURNO: CAPA PLATA ---")

# 1. Compactar y organizar archivos Parquet para acelerar Direct Lake
spark.sql("OPTIMIZE lh_flota_silver.telemetria_enriquecida")

# 2. Eliminar físicamente los archivos Parquet de sobrescrituras anteriores a 7 días
spark.sql("VACUUM lh_flota_silver.telemetria_enriquecida RETAIN 168 HOURS")

print("✅ Mantenimiento nocturno completado en telemetria_enriquecida.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
