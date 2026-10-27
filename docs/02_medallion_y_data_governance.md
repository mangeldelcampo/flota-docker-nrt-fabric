# ⚪ Módulo 2: Arquitectura Medallion en OneLake, Quality Gates y Gobernanza

## 1. Arquitectura Medallion Híbrida (Lambda)

La plataforma analítica implementa una **Arquitectura Medallion** en Microsoft Fabric estructurada en tres capas sobre **OneLake**:

1. **Capa Bronce (`lh_flota_bronce`)**: Ingesta cruda en tiempo real (Streaming AMQP) y por lotes (Batch en formato Delta Parquet).
2. **Capa Plata (`lh_flota_silver`)**: Limpieza, estandarización de tipos, enriquecimiento relacional entre dominios y aplicación de auditorías automáticas de calidad (**Data Quality Gate**).
3. **Capa Oro (`SM_Flota_Enriquecida`)**: Modelo analítico semántico en Estrella operando bajo el motor **Direct Lake**.

```
  [ Streaming IoT (Docker) ]              [ Ingesta Batch (EMT & ERP) ]
               │                                       │
               ▼                                       ▼
 🟤 CAPA BRONCE (lh_flota_bronce)       🟤 CAPA BRONCE (lh_flota_bronce)
   └── telemetria_nrt_v2                  └── dim_paradas_emt (1.007 paradas)
               │                                       │
               └───────────────────┬───────────────────┘
                                   │
                                   ▼ (PySpark ETL: nb_silver_enriquecimiento_nrt)
                         [ Data Quality Gate ] (SLA < 45 min)
                                   │
                                   ▼
 ⚪ CAPA PLATA (lh_flota_silver)
   ├── telemetria_enriquecida (NRT + RRHH + Fecha + Hora)
   ├── dim_conductores (150 empleados, PRL, puntos DGT)
   ├── dim_vehiculos_tecnica (45 autobuses, FinOps, ESG)
   ├── dim_proveedores (5 talleres, SLAs)
   ├── dim_dtc_codigos (Catálogo de averías)
   ├── fact_mantenimientos (Intervenciones en taller)
   ├── fact_multas (Expedientes DGT Point-in-Time)
   ├── fact_checklist_diario (Inspecciones visuales)
   ├── fact_control_jornada (Fichajes y absentismo)
   └── fact_incidencias (Tickets de soporte)
```

---

## 2. Capa Bronce (`lh_flota_bronce`) & Filosofía *Schema-on-Read*

* **Tabla NRT Streaming (`telemetria_nrt_v2`)**: Receptora directa del flujo ingestando eventos desde el Eventstream `es_flota_nrt`.
* **Tabla Batch de Referencia Espacial (`dim_paradas_emt`)**: Contiene las 1.007 paradas oficiales de las líneas comerciales de la EMT de Madrid (cargadas mediante el cuaderno PySpark `nb_bronce_ingesta_paradas_emt` desde el dataset `lines_detail geojson.csv`).
* **Regla de Gobernanza (*Schema-on-Read*)**: Para evitar la interrupción de la ingesta en vivo ante cambios imprevistos en el esquema origen de los sensores telemáticos, la Capa Bronce actúa como un sumidero flexible (catch-all). Las trasformaciones complejas y casteos estrictos de tipo se posponen intencionadamente a la Capa Plata.

---

## 3. Capa Plata (`lh_flota_silver`): Enriquecimiento y Modelo Dimensional

El cuaderno maestro de enriquecimiento **`nb_silver_enriquecimiento_nrt`** unifica el historial telemático, realiza el cruce relacional (*Left Join*) con el catálogo de RRHH y proyecta los identificadores inmutables de negocio.

### Código PySpark Definitivo: `nb_silver_enriquecimiento_nrt`
```python
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
df_conductores = spark.read.format("delta").table("lh_flota_silver.dim_conductores")

df_enriquecido = df_unificado.join(
    df_conductores,
    df_unificado.VehicleID == df_conductores.vehiculo_asignado,
    "left",
)

print("4. Seleccionando columnas finales (con Fecha_Evento, Hora e id_empleado)...")
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
    col("id_empleado"),  # Requisito clave para Direct Lake
    col("nombre_conductor").alias("Conductor_Activo"),
    col("experiencia_anos").alias("Experiencia_Cond"),
    col("turno").alias("Turno_RRHH"),
    current_timestamp().alias("Hora_Procesamiento_Silver"),
)

print("5. Escribiendo en Capa Plata (Silver)...")
(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_flota_silver.telemetria_enriquecida")
)
```

---

## 4. Data Quality Gate y Diagnóstico de SLA de Frescura

Para evitar la propagación de datos corruptos o estancados hacia los cuadros de mando ejecutivos en Power BI, el cuaderno incorpora un bloque de validación funcional denominado **Data Quality Gate**:

```python
# =========================================================================
# DATA QUALITY GATE: VALIDACIÓN DE VOLUMEN, FRESCURA (SLA < 45 MIN) E INTEGRIDAD
# =========================================================================
print("--- VALIDACIÓN DE CALIDAD DE DATOS (DATA QUALITY GATE) ---")

total_filas = df_final.count()
if total_filas == 0:
    raise ValueError("❌ [SLA CRÍTICO] La tabla 'telemetria_enriquecida' no generó ningún registro.")

max_evento = df_final.select(spark_max(col("Hora_Evento"))).collect()[0][0]
if max_evento is None:
    raise ValueError("❌ [SLA CRÍTICO] La columna 'Hora_Evento' está vacía o contiene valores Nulos.")

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
    print(f"⚠️ AVISO: Se detectaron {huerfanos} registros sin 'id_empleado' asignado.")

print(f"✅ QUALITY GATE SUPERADO: {total_filas} filas validadas. Último evento: {max_evento}.")
```

### Reglas Auditoras del Quality Gate:
1. **Validación de Volumen**: Detiene la ejecución si el DataFrame resultante tiene 0 filas.
2. **SLA de Frescura (< 45 minutos)**: Evalúa el evento más reciente (`max_evento`). Si supera los 45 minutos respecto al reloj del sistema, aborta el script imprimiendo el **Runbook operativo de restauración**.
3. **Auditoría de Integridad Referencial**: Registra avisos de advertencia si existen eventos telemáticos sin conductor asignado (`id_empleado IS NULL`).
