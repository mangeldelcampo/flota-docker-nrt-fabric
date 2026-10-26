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
