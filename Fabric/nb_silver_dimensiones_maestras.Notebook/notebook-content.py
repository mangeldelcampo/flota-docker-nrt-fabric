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

import random
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import expr

print("--- INICIANDO ACTUALIZACIÓN DE DIMENSIONES MAESTRAS ---")

# =====================================================================
# 1. CREAR DIMENSIÓN TÉCNICA DE VEHÍCULOS (Consumo, Seguros e ITV)
# =====================================================================
tipos_vehiculo = [
    {"tipo": "Estándar 12m", "combustible": "GNC", "deposito": 300, "consumo": 45.5, "seguro": 1200},
    {"tipo": "Articulado 18m", "combustible": "Diésel", "deposito": 400, "consumo": 60.2, "seguro": 1800},
    {"tipo": "Minibús Eléctrico", "combustible": "Eléctrico", "deposito": 150, "consumo": 25.0, "seguro": 950},
    {"tipo": "Estándar 12m (Híbrido)", "combustible": "Diésel/Híbrido", "deposito": 250, "consumo": 32.0, "seguro": 1400}
]

random.seed(42)
datos_vehiculos = []
for i in range(1001, 1046):
    veh_id = f"VEH-{i}"
    perfil = tipos_vehiculo[0] if i <= 1015 else (tipos_vehiculo[1] if i <= 1030 else (tipos_vehiculo[3] if i <= 1038 else tipos_vehiculo[2]))
        
    datos_vehiculos.append(Row(
        VehicleID=veh_id, Tipo_Vehiculo=perfil["tipo"], Tipo_Combustible=perfil["combustible"],
        Capacidad_Deposito_Lts=perfil["deposito"], Consumo_Medio_100km=perfil["consumo"],
        Coste_Anual_Seguro_EUR=perfil["seguro"], Kilometros_Adquisicion=random.randint(10000, 150000)
    ))

esquema_veh = StructType([
    StructField("VehicleID", StringType(), False), StructField("Tipo_Vehiculo", StringType(), False),
    StructField("Tipo_Combustible", StringType(), False), StructField("Capacidad_Deposito_Lts", IntegerType(), False),
    StructField("Consumo_Medio_100km", FloatType(), False), StructField("Coste_Anual_Seguro_EUR", IntegerType(), False),
    StructField("Kilometros_Adquisicion", IntegerType(), False)
])

# Guardar base técnica e inyectar fechas de ITV
df_vehiculos = spark.createDataFrame(datos_vehiculos, esquema_veh)
df_vehiculos_compliance = df_vehiculos.withColumn(
    "Ultima_ITV", expr("date_add(current_date(), -cast(rand() * 365 as int))")
).withColumn("Caducidad_ITV", expr("date_add(Ultima_ITV, 365)"))

df_vehiculos_compliance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.dim_vehiculos_tecnica")
print("✅ 1. dim_vehiculos_tecnica: Creada con especificaciones FinOps y caducidad de ITV.")

# =====================================================================
# 2. ACTUALIZAR DIMENSIÓN CONDUCTORES (Caducidad de Permisos)
# =====================================================================
df_conductores = spark.read.format("delta").table("lh_flota_silver.dim_conductores")

df_conductores_compliance = df_conductores.withColumn(
    "Caducidad_Permiso", expr("date_add(current_date(), cast(rand() * 1825 - 60 as int))") # Rango: -60 días a +5 años
).withColumn(
    "Estado_Permiso", expr("""
        CASE WHEN Caducidad_Permiso < current_date() THEN 'CADUCADO'
             WHEN Caducidad_Permiso <= date_add(current_date(), 30) THEN 'RENOVAR < 30 DÍAS'
             ELSE 'EN VIGOR' END
    """)
)

df_conductores_compliance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.dim_conductores")
print("✅ 2. dim_conductores: Actualizada con fechas de permisos y alertas.")

print("--- BLOQUE 1 COMPLETADO ---")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timedelta
from pyspark.sql.functions import col

print("--- INICIANDO BLOQUE 2: SIMULACIÓN DE CHECKLIST VISUAL (POWER APPS) ---")

# 1. Definir posibles incidencias y URLs simuladas (Blob Storage)
incidencias_comunes = [
    ("Arañazo lateral derecho", "https://via.placeholder.com/300x200.png?text=Aranazo+Lateral"),
    ("Faro trasero izquierdo roto", "https://via.placeholder.com/300x200.png?text=Faro+Roto"),
    ("Asiento manchado", "https://via.placeholder.com/300x200.png?text=Asiento+Manchado"),
    ("Espejo retrovisor rajado", "https://via.placeholder.com/300x200.png?text=Espejo+Rajado")
]

# 2. Generar datos (3 turnos para 45 vehículos)
datos_checklist = []
fecha_base = datetime(2026, 10, 21) # Fecha actual de la simulación
turnos = [(6, "Mañana"), (14, "Tarde"), (22, "Noche")]

random.seed(123)

for i in range(1001, 1046):
    veh_id = f"VEH-{i}"
    
    for hora_turno, nombre_turno in turnos:
        # Fichaje aleatorio: entre 5 y 25 minutos antes de la hora en punto
        hora_fichaje = fecha_base + timedelta(hours=hora_turno, minutes=-random.randint(5, 25))
        
        # 10% de probabilidad de encontrar un daño reportable
        es_incidencia = random.random() < 0.10
        
        if es_incidencia:
            estado = "Con Daños Previos"
            incidencia_elegida = random.choice(incidencias_comunes)
            descripcion = incidencia_elegida[0]
            url_foto = incidencia_elegida[1]
        else:
            estado = "OK"
            descripcion = None
            url_foto = None
            
        datos_checklist.append(Row(
            VehicleID=veh_id,
            Turno_Asignado=nombre_turno,
            Hora_Checklist=hora_fichaje,
            Estado_Visual=estado,
            Descripcion_Dano=descripcion,
            URL_Foto_Dano=url_foto
        ))

# 3. Esquema
esquema_checklist = StructType([
    StructField("VehicleID", StringType(), False),
    StructField("Turno_Asignado", StringType(), False),
    StructField("Hora_Checklist", TimestampType(), False),
    StructField("Estado_Visual", StringType(), False),
    StructField("Descripcion_Dano", StringType(), True),
    StructField("URL_Foto_Dano", StringType(), True)
])

# 4. Escribir tabla de hechos en Plata
df_checklist = spark.createDataFrame(datos_checklist, esquema_checklist)
df_checklist.write.format("delta").mode("overwrite").saveAsTable("lh_flota_silver.fact_checklist_diario")

print(f"✅ Éxito: Se han generado {df_checklist.count()} partes de revisión pre-turno.")

# Mostrar únicamente aquellos partes que tienen incidencias (fotos) para validarlo
display(spark.table("lh_flota_silver.fact_checklist_diario").filter(col("Estado_Visual") == "Con Daños Previos").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, concat, substring, rand, round
import uuid

print("--- INICIANDO BLOQUE 3: MOTOR DE MULTAS DGT (POINT-IN-TIME) ---")

# 1. Leer la telemetría real (Capa Plata)
df_telemetria = spark.read.format("delta").table("lh_flota_silver.telemetria_enriquecida")

# 2. Detectar Infracciones Reales: Buscar excesos de velocidad (> 60 km/h)
df_infracciones = df_telemetria.filter(col("Speed") > 60)

# 3. Extraer una muestra aleatoria para simular las notificaciones de la DGT que llegan hoy
# (Cogemos por ejemplo un 1% de las infracciones para no inundar a los conductores de multas)
df_multas_base = df_infracciones.sample(withReplacement=False, fraction=0.01, seed=42).limit(20)

# 4. Enriquecer y formatear la tabla final de Multas (fact_multas)
df_multas = df_multas_base.select(
    col("VehicleID"),
    col("Hora_Evento").alias("Fecha_Hora_Infraccion"),
    col("Speed").alias("Velocidad_Radar_kmh"),
    col("Conductor_Activo").alias("Conductor_Infractor"),
    col("Turno_RRHH").alias("Turno_Infraccion")
).withColumn(
    "ID_Expediente", expr("uuid()") # Generar un ID único de multa
).withColumn(
    "Tipo_Infraccion", lit("Exceso de Velocidad en Vía Urbana")
).withColumn(
    "Importe_EUR", 
    expr("""
        CASE 
            WHEN Velocidad_Radar_kmh > 80 THEN 600
            WHEN Velocidad_Radar_kmh > 70 THEN 300
            ELSE 100
        END
    """)
).withColumn(
    "Puntos_DGT", 
    expr("""
        CASE 
            WHEN Velocidad_Radar_kmh > 80 THEN 6
            WHEN Velocidad_Radar_kmh > 70 THEN 2
            ELSE 0
        END
    """)
)

# Reordenar columnas para que quede bonito
columnas_finales = ["ID_Expediente", "VehicleID", "Conductor_Infractor", "Fecha_Hora_Infraccion", "Tipo_Infraccion", "Velocidad_Radar_kmh", "Importe_EUR", "Puntos_DGT"]
df_multas_final = df_multas.select(columnas_finales)

# 5. Escribir tabla de hechos en Plata
df_multas_final.write.format("delta").mode("overwrite").saveAsTable("lh_flota_silver.fact_multas")

print(f"✅ Éxito: Se han procesado y asignado {df_multas_final.count()} expedientes sancionadores de tráfico.")

# Mostrar el resultado final
display(spark.table("lh_flota_silver.fact_multas").orderBy(col("Importe_EUR").desc()).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr, col

print("Asignando Roles Operativos a la Flota (Titulares vs Reserva)...")

# 1. Leer la dimensión actual
df_vehiculos_actual = spark.read.format("delta").table("lh_flota_silver.dim_vehiculos_tecnica")

# 2. Asignar rol: 85% Titulares, 15% Reservas (aprox)
df_vehiculos_roles = df_vehiculos_actual.withColumn(
    "Rol_Operativo",
    expr("""
        CASE 
            WHEN rand() > 0.85 THEN 'Reserva'
            ELSE 'Titular'
        END
    """)
)

# 3. Sobrescribir en Plata
df_vehiculos_roles.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.dim_vehiculos_tecnica")

print("✅ Éxito: La flota tiene ahora vehículos de retén (reserva).")

# Mostrar cuántos vehículos hay de cada tipo
display(spark.sql("SELECT Rol_Operativo, COUNT(*) as Total_Vehiculos FROM lh_flota_silver.dim_vehiculos_tecnica GROUP BY Rol_Operativo"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr, col

print("Añadiendo Saldo Inicial de Puntos DGT a los conductores...")

# 1. Leer la tabla actual de conductores
df_conductores_actual = spark.read.format("delta").table("lh_flota_silver.dim_conductores")

# 2. Asignar el saldo inicial de puntos basándose en la experiencia
# (Los veteranos suelen tener 15 puntos, los conductores estándar 12, los muy novatos 8)
df_conductores_puntos = df_conductores_actual.withColumn(
    "Saldo_Puntos_Inicial",
    expr("""
        CASE 
            WHEN experiencia_anos >= 10 THEN 15
            WHEN experiencia_anos > 3 THEN 12
            ELSE 8
        END
    """)
)

# 3. Sobrescribir la tabla en Plata
df_conductores_puntos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.dim_conductores")

print("✅ Éxito: La dimensión 'dim_conductores' tiene ahora su saldo de puntos inicial.")

# Previsualizar el cambio
display(spark.table("lh_flota_silver.dim_conductores").select("nombre_conductor", "experiencia_anos", "Saldo_Puntos_Inicial").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, expr
import uuid

print("--- FORZANDO MULTAS PARA PRUEBAS (UMBRAL > 40 KM/H) ---")

df_telemetria = spark.read.format("delta").table("lh_flota_silver.telemetria_enriquecida")

# Bajamos el umbral a 40 km/h para asegurarnos de capturar eventos reales de la telemetría
df_infracciones = df_telemetria.filter(col("Speed") > 40)

# Extraemos 15 registros aleatorios
df_multas_base = df_infracciones.sample(withReplacement=False, fraction=0.05, seed=123).limit(15)

df_multas = df_multas_base.select(
    col("VehicleID"),
    col("Hora_Evento").alias("Fecha_Hora_Infraccion"),
    col("Speed").alias("Velocidad_Radar_kmh"),
    col("Conductor_Activo").alias("Conductor_Infractor"),
    col("Turno_RRHH").alias("Turno_Infraccion")
).withColumn(
    "ID_Expediente", expr("uuid()")
).withColumn(
    "Tipo_Infraccion", lit("Exceso de Velocidad Urbana")
).withColumn(
    "Importe_EUR", 
    expr("""
        CASE 
            WHEN Velocidad_Radar_kmh >= 48 THEN 300
            ELSE 100
        END
    """)
).withColumn(
    "Puntos_DGT_Retirados", 
    expr("""
        CASE 
            WHEN Velocidad_Radar_kmh >= 48 THEN 4
            ELSE 2
        END
    """)
)

columnas_finales = ["ID_Expediente", "VehicleID", "Conductor_Infractor", "Fecha_Hora_Infraccion", "Turno_Infraccion", "Tipo_Infraccion", "Velocidad_Radar_kmh", "Importe_EUR", "Puntos_DGT_Retirados"]
df_multas_final = df_multas.select(columnas_finales)

df_multas_final.write.format("delta").mode("overwrite").saveAsTable("lh_flota_silver.fact_multas")

print(f"✅ Éxito: Expedientes generados con éxito: {df_multas_final.count()}")
display(spark.table("lh_flota_silver.fact_multas").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

print("--- INYECTANDO ID DE EMPLEADO EN LAS TABLAS DE HECHOS ---")

# 1. Leer el catálogo de conductores (solo cogemos Nombre e ID para hacer de diccionario)
df_diccionario_rrhh = spark.read.format("delta").table("lh_flota_silver.dim_conductores").select(
    col("nombre_conductor"), 
    col("id_empleado")
)

# 2. Arreglar la tabla de Telemetría
print("Actualizando telemetria_enriquecida...")
df_telemetria = spark.read.format("delta").table("lh_flota_silver.telemetria_enriquecida")

df_telemetria_arreglada = df_telemetria.join(
    df_diccionario_rrhh, 
    df_telemetria.Conductor_Activo == df_diccionario_rrhh.nombre_conductor, 
    "left"
).drop("nombre_conductor") # Borramos la columna duplicada del join

df_telemetria_arreglada.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("lh_flota_silver.telemetria_enriquecida")


# 3. Arreglar la tabla de Multas
print("Actualizando fact_multas...")
df_multas = spark.read.format("delta").table("lh_flota_silver.fact_multas")

df_multas_arreglada = df_multas.join(
    df_diccionario_rrhh, 
    df_multas.Conductor_Infractor == df_diccionario_rrhh.nombre_conductor, 
    "left"
).drop("nombre_conductor")

df_multas_arreglada.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("lh_flota_silver.fact_multas")

print("✅ Éxito: Las tablas de hechos ahora tienen el campo 'id_empleado'.")
display(spark.table("lh_flota_silver.fact_multas").select("Conductor_Infractor", "id_empleado", "Importe_EUR").limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

print("--- INYECTANDO ID DE EMPLEADO EN EL CHECKLIST DIARIO ---")

# 1. Leer el catálogo de conductores (solo cogemos ID, Vehículo y Turno para el cruce)
df_dim_conductores = spark.read.format("delta").table("lh_flota_silver.dim_conductores").select(
    col("id_empleado"),
    col("vehiculo_asignado"),
    col("turno")
)

# 2. Leer la tabla de Checklist
df_checklist = spark.read.format("delta").table("lh_flota_silver.fact_checklist_diario")

# 3. Cruzar (Join) para obtener el id_empleado
df_checklist_arreglado = df_checklist.join(
    df_dim_conductores,
    (df_checklist.VehicleID == df_dim_conductores.vehiculo_asignado) & 
    (df_checklist.Turno_Asignado == df_dim_conductores.turno),
    "left"
).drop("vehiculo_asignado", "turno") # Borramos las columnas duplicadas usadas para el join

# 4. Sobrescribir la tabla física en la Capa Plata
df_checklist_arreglado.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("lh_flota_silver.fact_checklist_diario")

print("✅ Éxito: La tabla fact_checklist_diario ahora tiene el campo 'id_empleado'.")
display(spark.table("lh_flota_silver.fact_checklist_diario").select("VehicleID", "Turno_Asignado", "id_empleado", "Estado_Visual").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, date_format

print("--- ESTANDARIZANDO FORMATO DE FECHAS ---")

# 1. Arreglar fact_multas
df_multas = spark.read.format("delta").table("lh_flota_silver.fact_multas")
df_multas_limpia = df_multas.withColumn(
    "Fecha_Hora_Infraccion", 
    date_format(col("Fecha_Hora_Infraccion"), "yyyy-MM-dd HH:mm:ss")
)
df_multas_limpia.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.fact_multas")
print("✅ fact_multas actualizada.")

# 2. Arreglar fact_checklist_diario (Corregido a Hora_Checklist)
df_checklist = spark.read.format("delta").table("lh_flota_silver.fact_checklist_diario")
df_checklist_limpia = df_checklist.withColumn(
    "Hora_Checklist", 
    date_format(col("Hora_Checklist"), "yyyy-MM-dd HH:mm:ss")
)
df_checklist_limpia.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.fact_checklist_diario")
print("✅ fact_checklist_diario actualizada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- PROMOVIENDO DIMENSIÓN GEOGRÁFICA DE PARADAS A SILVER ---")

# 1. Leemos la tabla limpia desde la Capa Bronce
df_paradas = spark.read.format("delta").table("lh_flota_bronce.dim_paradas_emt")

# 2. La guardamos exactamente igual en la Capa Plata
df_paradas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.dim_paradas_emt")

print("✅ Tabla 'dim_paradas_emt' promovida a lh_flota_silver con éxito.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, expr

print("--- GENERANDO DIMENSIÓN HORARIA (dim_hora) ---")

# Generar rango de 0 a 23
df_hora_base = spark.range(0, 24).toDF("Hora")

df_dim_hora = (
    df_hora_base.withColumn("Hora_Texto", expr("printf('%02d:00', Hora)"))
    .withColumn(
        "Franja_Horaria",
        expr("printf('%02d:00 - %02d:00', Hora, (Hora + 1) % 24)"),
    )
    .withColumn(
        "Turno_Horario",
        expr("""
        CASE 
            WHEN Hora BETWEEN 6 AND 13 THEN 'Mañana'
            WHEN Hora BETWEEN 14 AND 21 THEN 'Tarde'
            ELSE 'Noche'
        END
    """),
    )
)

# Persistir en la Capa Plata
(
    df_dim_hora.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_flota_silver.dim_hora")
)

print("✅ dim_hora creada exitosamente con 24 registros.")
display(spark.table("lh_flota_silver.dim_hora"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
