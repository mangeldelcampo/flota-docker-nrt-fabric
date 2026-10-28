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

from datetime import datetime, timedelta
from pyspark.sql.functions import (
    col,
    current_timestamp,
    hour,
    max as spark_max,
    to_date,
    to_timestamp,
)

print("1. Leyendo datos desde Capa Bronce...")
df_historico = (
    spark.read.format("delta")
    .table("lh_flota_bronce.telemetria_cruda_nrt")
    .withColumn("Timestamp", to_timestamp(col("Timestamp")))
)

df_nuevo = (
    spark.read.format("delta")
    .table("lh_flota_bronce.telemetria_nrt_v2")
    .withColumn("Timestamp", col("Timestamp").cast("timestamp"))
)

print("2. Unificando históricos y recientes...")
df_unificado = df_historico.unionByName(df_nuevo, allowMissingColumns=True)

print("3. Cruzando datos con RRHH...")
df_conductores = spark.read.format("delta").table(
    "lh_flota_silver.dim_conductores"
)

df_enriquecido = df_unificado.join(
    df_conductores,
    df_unificado.VehicleID == df_conductores.vehiculo_asignado,
    "left",
)

print(
    "4. Seleccionando columnas finales (con Fecha_Evento, Hora e id_empleado)..."
)
df_final = df_enriquecido.select(
    col("VehicleID"),
    col("Timestamp").alias("Hora_Evento"),
    to_date(col("Timestamp")).alias("Fecha_Evento"),
    hour(col("Timestamp")).cast("integer").alias("Hora"),
    col("Latitude"),
    col("Longitude"),
    col("Speed"),
    col("FuelLevel"),
    col("DTC_Code"),
    col("id_empleado"),
    col("nombre_conductor").alias("Conductor_Activo"),
    col("experiencia_anos").alias("Experiencia_Cond"),
    col("turno").alias("Turno_RRHH"),
    current_timestamp().alias("Hora_Procesamiento_Silver"),
)

print("5. Escribiendo en Capa Plata (lh_flota_silver)...")
(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_flota_silver.telemetria_enriquecida")
)

# =========================================================================
# DATA QUALITY GATE: VALIDACIÓN DE VOLUMEN, FRESCURA (SLA < 45 MIN) E INTEGRIDAD
# =========================================================================
print("--- VALIDACIÓN DE CALIDAD DE DATOS (DATA QUALITY GATE) ---")

total_filas = df_final.count()
if total_filas == 0:
  raise ValueError(
      "❌ [SLA CRÍTICO] La tabla 'telemetria_enriquecida' no generó ningún"
      " registro."
  )

max_evento = df_final.select(spark_max(col("Hora_Evento"))).collect()
if max_evento is None:
  raise ValueError(
      "❌ [SLA CRÍTICO] La columna 'Hora_Evento' está vacía o contiene valores"
      " Nulos."
  )

tiempo_actual = datetime.now()
tiempo_limite = tiempo_actual - timedelta(minutes=45)

if max_evento < tiempo_limite:
  minutos_retraso = int((tiempo_actual - max_evento).total_seconds() / 60)
  horas_retraso = round(minutos_retraso / 60, 1)

  mensaje_error = (
      f"\n{'='*70}\n"
      f"🚨 [PIPELINE SLA VIOLATION] TELEMETRÍA ESTANCADA\n"
      f"{'='*70}\n"
      f"• Causa: No se reciben datos en la Capa Bronce desde hace {minutos_retraso} min (~{horas_retraso} h).\n"
      f"• Último evento registrado : {max_evento}\n"
      f"• Hora de ejecución actual : {tiempo_actual.strftime('%Y-%m-%d %H:%M:%S')}\n"
      f"• Umbral máximo permitido : 45 minutos\n\n"
      f"📋 ACCIÓN REQUERIDA (Runbook):\n"
      f"  1. Conectarse al servidor Ubuntu y ejecutar: 'docker compose ps'\n"
      f"  2. Si los contenedores están caídos: 'docker compose up -d'\n"
      f"  3. Comprobar flujo en Eventstream 'es_flota_nrt' antes de reejecutar.\n"
      f"{'='*70}"
  )
  raise ValueError(mensaje_error)

huerfanos = df_final.filter(col("id_empleado").isNull()).count()
if huerfanos > 0:
  print(
      f"⚠️ AVISO: Se detectaron {huerfanos} registros sin 'id_empleado'"
      " asignado."
  )

print(
    f"✅ QUALITY GATE SUPERADO: {total_filas} filas validadas. Último evento:"
    f" {max_evento}."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# from datetime import datetime, timezone, timedelta
# from pyspark.sql.functions import (
#     col,
#     current_timestamp,
#     hour,
#     max as spark_max,
#     to_date,
#     to_timestamp
# )
# 
# print("1. Leyendo datos desde Capa Bronce...")
# df_historico = (
#     spark.read.format("delta")
#     .table("lh_flota_bronce.telemetria_cruda_nrt")
#     .withColumn("Timestamp", to_timestamp(col("Timestamp")))
# )
# 
# df_nuevo = (
#     spark.read.format("delta")
#     .table("lh_flota_bronce.telemetria_nrt_v2")
#     .withColumn("Timestamp", col("Timestamp").cast("timestamp"))
# )
# 
# print("2. Unificando históricos y recientes...")
# df_unificado = df_historico.unionByName(df_nuevo, allowMissingColumns=True)
# 
# print("3. Cruzando datos con RRHH...")
# df_conductores = spark.read.format("delta").table("lh_flota_silver.dim_conductores")
# 
# df_enriquecido = df_unificado.join(
#     df_conductores,
#     df_unificado.VehicleID == df_conductores.vehiculo_asignado,
#     "left"
# )
# 
# print("4. Seleccionando columnas finales...")
# df_final = df_enriquecido.select(
#     col("VehicleID"),
#     col("Timestamp").alias("Hora_Evento"),
#     to_date(col("Timestamp")).alias("Fecha_Evento"),
#     hour(col("Timestamp")).cast("integer").alias("Hora"),
#     col("Latitude"),
#     col("Longitude"),
#     col("Speed"),
#     col("FuelLevel"),
#     col("DTC_Code"),
#     col("id_empleado"),
#     col("nombre_conductor").alias("Conductor_Activo"),
#     col("experiencia_anos").alias("Experiencia_Cond"),
#     col("turno").alias("Turno_RRHH"),
#     current_timestamp().alias("Hora_Procesamiento_Silver")
# )
# 
# print("--- EVALUACIÓN PREVIA: DATA QUALITY GATE ---")
# 
# # 1. Validación de Volumen
# total_filas = df_final.count()
# if total_filas == 0:
#     raise ValueError("❌ [SLA CRÍTICO] El DataFrame final no contiene registros.")
# 
# # 2. Validación de Frescura (SLA NRT < 45 min en referencia UTC)
# row_max = df_final.select(spark_max(col("Hora_Evento"))).collect()
# max_evento = row_max[0][0] if row_max and row_max[0][0] is not None else None
# 
# if max_evento is None:
#     raise ValueError("❌ [SLA CRÍTICO] La columna 'Hora_Evento' está vacía o contiene nulos.")
# 
# # Referencia UTC alineada con el emisor en Docker
# tiempo_actual_utc = datetime.now(timezone.utc).replace(tzinfo=None)
# tiempo_limite = tiempo_actual_utc - timedelta(minutes=45)
# 
# if max_evento < tiempo_limite:
#     minutos_retraso = int((tiempo_actual_utc - max_evento).total_seconds() / 60)
#     raise ValueError(
#         f"🚨 [SLA VIOLATION] Telemetría estancada ({minutos_retraso} min de desfase). "
#         f"Último evento: {max_evento}. Límite UTC: {tiempo_limite}. Abortando escritura en Plata."
#     )
# 
# # 3. Validación de Integridad Relacional
# huerfanos = df_final.filter(col("id_empleado").isNull()).count()
# if huerfanos > 0:
#     print(f"⚠️ AVISO: Se detectaron {huerfanos} registros sin 'id_empleado' asignado.")
# 
# print(f"✅ QUALITY GATE SUPERADO: {total_filas} filas validadas. Último evento: {max_evento}.")
# 
# # 5. Persistencia física: SOLO se ejecuta tras validar la calidad
# print("5. Escribiendo en Capa Plata (lh_flota_silver)...")
# (
#     df_final.write.format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable("lh_flota_silver.telemetria_enriquecida")
# )
# print("✅ Proceso completado exitosamente.")


# CELL ********************

from datetime import datetime, timezone, timedelta
from pyspark.sql.functions import (
    col,
    current_timestamp,
    hour,
    max as spark_max,
    to_date,
    to_timestamp,
    from_utc_timestamp
)

print("1. Leyendo datos desde Capa Bronce...")
df_historico = (
    spark.read.format("delta")
    .table("lh_flota_bronce.telemetria_cruda_nrt")
    .withColumn("Timestamp", to_timestamp(col("Timestamp")))
)

df_nuevo = (
    spark.read.format("delta")
    .table("lh_flota_bronce.telemetria_nrt_v2")
    .withColumn("Timestamp", col("Timestamp").cast("timestamp"))
)

print("2. Unificando históricos y recientes...")
df_unificado = df_historico.unionByName(df_nuevo, allowMissingColumns=True)

print("3. Cruzando datos con RRHH...")
df_conductores = spark.read.format("delta").table("lh_flota_silver.dim_conductores")

df_enriquecido = df_unificado.join(
    df_conductores,
    df_unificado.VehicleID == df_conductores.vehiculo_asignado,
    "left"
)

print("4. Seleccionando columnas finales con conversión horaria (Europe/Madrid)...")
# Convertir el Timestamp UTC original a la hora oficial de Madrid (CET / CEST automático)
df_local = df_enriquecido.withColumn("Timestamp_Local", from_utc_timestamp(col("Timestamp"), "Europe/Madrid"))

df_final = df_local.select(
    col("VehicleID"),
    col("Timestamp_Local").alias("Hora_Evento"),
    to_date(col("Timestamp_Local")).alias("Fecha_Evento"),
    hour(col("Timestamp_Local")).cast("integer").alias("Hora"),
    col("Latitude"),
    col("Longitude"),
    col("Speed"),
    col("FuelLevel"),
    col("DTC_Code"),
    col("id_empleado"),
    col("nombre_conductor").alias("Conductor_Activo"),
    col("experiencia_anos").alias("Experiencia_Cond"),
    col("turno").alias("Turno_RRHH"),
    current_timestamp().alias("Hora_Procesamiento_Silver")
)

# =========================================================================
# DATA QUALITY GATE: VALIDACIÓN DE VOLUMEN, FRESCURA (SLA < 45 MIN) E INTEGRIDAD
# =========================================================================
print("--- EVALUACIÓN PREVIA: DATA QUALITY GATE ---")

# A. Validación de Volumen
total_filas = df_final.count()
if total_filas == 0:
    raise ValueError("❌ [SLA CRÍTICO] El DataFrame final no contiene registros.")

# B. Validación de Frescura (SLA NRT < 45 min)
row_max = df_final.select(spark_max(col("Hora_Evento"))).collect()
max_evento_local = row_max[0][0] if row_max and row_max[0][0] is not None else None

if max_evento_local is None:
    raise ValueError("❌ [SLA CRÍTICO] La columna 'Hora_Evento' está vacía o contiene nulos.")

# Referencia UTC convertida a hora de Madrid para evaluar con exactitud
ahora_utc = datetime.now(timezone.utc)
# En invierno (CET) sumamos 1 hora; en verano (CEST) sumamos 2 horas
# O evaluamos directamente sobre el Timestamp UTC crudo:
row_max_utc = df_unificado.select(spark_max(to_timestamp(col("Timestamp")))).collect()
max_utc = row_max_utc[0][0]

tiempo_limite_utc = ahora_utc.replace(tzinfo=None) - timedelta(minutes=45)

if max_utc < tiempo_limite_utc:
    minutos_retraso = int((ahora_utc.replace(tzinfo=None) - max_utc).total_seconds() / 60)
    raise ValueError(
        f"🚨 [SLA VIOLATION] Telemetría estancada ({minutos_retraso} min de desfase). "
        f"Último evento UTC: {max_utc}. Límite UTC: {tiempo_limite_utc}. Abortando escritura."
    )

print(f"✅ QUALITY GATE SUPERADO: {total_filas} filas validadas. Último evento local: {max_evento_local}.")

# C. Persistencia física SOLO tras superar la puerta de calidad
print("5. Escribiendo en Capa Plata (lh_flota_silver)...")
(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_flota_silver.telemetria_enriquecida")
)
print("✅ Proceso completado exitosamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
