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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random

# 1. Definición de la estructura de Recursos Humanos (Base de datos ficticia)
esquema_conductores = StructType([
    StructField("id_empleado", StringType(), True),
    StructField("nombre_conductor", StringType(), True),
    StructField("vehiculo_asignado", StringType(), True),
    StructField("turno", StringType(), True),
    StructField("experiencia_anos", IntegerType(), True),
    StructField("categoria_carnet", StringType(), True),
    StructField("telefono_contacto", StringType(), True)
])

# Listas auxiliares para dar realismo a los nombres (puedes cambiarlos)
nombres = ["Carlos", "Ana", "Miguel", "Laura", "Javier", "María", "David", "Elena", "Fernando", "Lucía", "Jorge", "Marta", "Miguel Angel", "Camilo", "William", "Rafael", "Juanjo","Alicia", "Emilia"]
apellidos = ["Sainz", "García", "López", "Martínez", "Pérez", "Rodríguez", "Sánchez", "Gómez", "Fernández", "Díaz"]

datos_rrhh = []
id_counter = 1000

# 2. Generar 3 conductores activos (Mañana, Tarde, Noche) para cada uno de los 45 vehículos
for i in range(1, 46):
    veh_id = f"VEH-{1000 + i}"  # VEH-1001 a VEH-1045
    
    for turno in ["Mañana", "Tarde", "Noche"]:
        nombre_completo = f"{random.choice(nombres)} {random.choice(apellidos)} {random.choice(apellidos)}"
        experiencia = random.randint(1, 25)
        # Novatos (<3 años) suelen tener D1, el resto D completo
        categoria = "D" if experiencia >= 3 else "D1"
        
        datos_rrhh.append((
            f"EMP-{id_counter}",
            nombre_completo,
            veh_id,
            turno,
            experiencia,
            categoria,
            f"600{random.randint(100000, 999999)}"
        ))
        id_counter += 1

# 3. Generar 15 conductores "comodín" (sin vehículo fijo, en vacaciones o baja)
for _ in range(15):
    nombre_completo = f"{random.choice(nombres)} {random.choice(apellidos)} {random.choice(apellidos)}"
    estado = random.choice(["Vacaciones", "Baja Médica", "Reserva/Correturnos"])
    
    datos_rrhh.append((
        f"EMP-{id_counter}",
        nombre_completo,
        None,  # No tienen vehículo asignado hoy
        estado,
        random.randint(5, 30),
        "D",
        f"600{random.randint(100000, 999999)}"
    ))
    id_counter += 1

# 4. Crear el DataFrame y guardarlo como tabla Delta en la Capa Plata (Silver)
df_conductores = spark.createDataFrame(datos_rrhh, schema=esquema_conductores)

# Persistencia optimizada
df_conductores.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_conductores")

print(f"Éxito: Tabla 'dim_conductores' generada en Silver con {df_conductores.count()} empleados.")
display(spark.table("dim_conductores").limit(150))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr, when, rand, current_date, date_sub

# Cargamos la tabla actual de vehículos
df_vehiculos = spark.table("dim_vehiculos_tecnica")

# Enriquecimiento con métricas de Calidad y ESG (usando paréntesis para sintaxis limpia y comentarios)
df_vehiculos_enriquecida = (
    df_vehiculos
    .withColumn(
        "Clasificacion_Ambiental", 
        when(rand() < 0.2, "CERO") # 20% flota eléctrica
        .when(rand() < 0.6, "ECO") # 40% flota híbrida / GNC
        .otherwise("Euro 6") # 40% flota diésel moderno
    )
    .withColumn(
        "Fecha_Certificado_Emisiones", 
        date_sub(current_date(), (rand() * 300).cast("int")) # Fecha aleatoria del último año
    )
)

# Sobrescribimos la tabla (Evolución de esquema)
(
    df_vehiculos_enriquecida.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_vehiculos_tecnica")
)

print("¡Dimensión dim_vehiculos_tecnica actualizada con éxito con métricas de Calidad y ESG!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr, when, rand

df_conductores = spark.table("dim_conductores")

df_conductores_enriquecida = df_conductores \
    .withColumn("Tipo_Contrato", when(rand() < 0.7, "Indefinido").when(rand() < 0.9, "Temporal").otherwise("Prácticas")) \
    .withColumn("Apto_Medico", when(rand() < 0.95, True).otherwise(False)) \
    .withColumn("Certificados_SS_IRPF_Al_Dia", when(rand() < 0.98, True).otherwise(False)) \
    .withColumn("Entrega_EPIs", when(rand() < 0.99, "Firmado").otherwise("Pendiente")) \
    .withColumn("Coste_Hora_Base", expr("ROUND(12.5 + (experiencia_anos * 0.4), 2)"))

df_conductores_enriquecida.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_conductores")
print("Dimensión dim_conductores actualizada con datos de PRL y RRHH.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generación de Dimensión Calendario Maestra con Spark SQL
df_calendario = spark.sql("""
    SELECT 
        CAST(d AS DATE) AS Fecha,
        YEAR(d) AS Anio,
        MONTH(d) AS Mes_Num,
        DATE_FORMAT(d, 'MMMM') AS Mes_Nombre,
        QUARTER(d) AS Trimestre,
        DAY(d) AS Dia,
        DAYOFWEEK(d) AS Dia_Semana_Num,
        DATE_FORMAT(d, 'EEEE') AS Dia_Semana_Nombre
    FROM (
        SELECT explode(sequence(to_date('2025-01-01'), to_date('2030-12-31'), interval 1 day)) AS d
    )
""")

# Guardar como tabla Delta administrada
df_calendario.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_calendario")

print("¡Tabla dim_calendario creada con éxito!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
