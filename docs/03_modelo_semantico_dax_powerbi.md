# 🟡 Módulo 3: Modelo Semántico en Estrella V2 (Direct Lake) y Biblioteca DAX

## 1. Modelo Semántico en Estrella (`SM_Flota_Enriquecida`)

El Modelo Semántico **`SM_Flota_Enriquecida`** constituye la Capa Oro de la plataforma analítica. Diseñado en un esquema de **Estrella Puro V2**, se ejecuta sobre el motor VertiPaq en modo **Direct Lake**, consultando directamente los archivos Parquet físicos en OneLake con latencia en milisegundos y sin requerir procesos de importación de datos.

```
                  ┌───────────────────────┐
                  │     dim_calendario    │
                  └───────────┬───────────┘
                              │ (1:*)
                              ▼
 ┌──────────────────┐   ┌────────────────────────┐   ┌─────────────────────┐
 │ dim_conductores  │──►│ telemetria_enriquecida │◄──│dim_vehiculos_tecnica│
 └────────┬─────────┘   └────────────────────────┘   └──────────┬──────────┘
          │ (1:*)                 ▲                             │ (1:*)
          ├───────────────────────┼─────────────────────────────┤
          │                       │ (1:*)                       │
          ▼                       │                             ▼
 ┌──────────────────┐   ┌─────────┴──────────────┐   ┌─────────────────────┐
 │  fact_multas     │   │     dim_dtc_codigos    │   │ fact_mantenimientos │
 └──────────────────┘   └────────────────────────┘   └──────────┬──────────┘
                                                                │ (*)
                                                                ▲
                                                     ┌──────────┴──────────┐
                                                     │   dim_proveedores   │
                                                     └─────────────────────┘
```

---

## 2. Catálogo de 13 Relaciones Relacionales (1:N, Filtro Cruzado Único)

| Tabla Origen (Dimensión 1) | Campo Clave Origen | Tabla Destino (Hecho N) | Campo Clave Destino | Propósito Operativo |
| :--- | :--- | :--- | :--- | :--- |
| `dim_vehiculos_tecnica` | `VehicleID` | `telemetria_enriquecida` | `VehicleID` | Trazabilidad telemática del vehículo |
| `dim_vehiculos_tecnica` | `VehicleID` | `fact_multas` | `VehicleID` | Historial de infracciones por unidad |
| `dim_vehiculos_tecnica` | `VehicleID` | `fact_checklist_diario` | `VehicleID` | Inspecciones visuales por autobús |
| `dim_vehiculos_tecnica` | `VehicleID` | `fact_mantenimientos` | `VehicleID` | Costes mecánicos e inmovilización |
| `dim_vehiculos_tecnica` | `VehicleID` | `fact_incidencias` | `VehicleID` | Partes de averías técnicas |
| `dim_conductores` | `id_empleado` | `telemetria_enriquecida` | `id_empleado` | Identificación de conductor en servicio |
| `dim_conductores` | `id_empleado` | `fact_multas` | `id_empleado` | Asignación Point-in-Time de infracciones DGT |
| `dim_conductores` | `id_empleado` | `fact_checklist_diario` | `id_empleado` | Responsable del parte pre-turno |
| `dim_conductores` | `id_empleado` | `fact_control_jornada` | `id_empleado` | Fichajes, turnos y absentismo |
| `dim_conductores` | `id_empleado` | `fact_incidencias` | `id_empleado_reportador` | Creador de partes de soporte |
| `dim_proveedores` | `ID_Proveedor` | `fact_mantenimientos` | `ID_Proveedor` | Control de talleres externos y SLAs |
| `dim_calendario` | `Fecha` | `telemetria_enriquecida` | `Fecha_Evento` | Análisis temporal histórico |
| `dim_hora` | `Hora` | `telemetria_enriquecida` | `Hora` | Filtrado horario por franjas (0-23) |
| `dim_dtc_codigos` | `DTC_Code` | `telemetria_enriquecida` | `DTC_Code` | Diagnóstico de códigos de avería |

> **Nota de Gobernanza de Mapas**: La tabla `dim_paradas_emt` (1.007 registros) se mantiene **desconectada** del modelo semántico. Actúa como capa de referencia espacial WKT/GeoJSON en Azure Maps e Icon Map para evitar la distorsión del contexto de filtro.

---

## 3. Dimensión Horaria (`dim_hora`)

Para posibilitar la segmentación del informe por tramos de 60 minutos sin penalizar el volumen de datos en Direct Lake:

```python
from pyspark.sql.functions import col, expr

df_hora_base = spark.range(0, 24).toDF("Hora")

df_dim_hora = (
    df_hora_base.withColumn("Hora_Texto", expr("printf('%02d:00', Hora)"))
    .withColumn("Franja_Horaria", expr("printf('%02d:00 - %02d:00', Hora, (Hora + 1) % 24)"))
    .withColumn("Turno_Horario", expr("""
        CASE 
            WHEN Hora BETWEEN 6 AND 13 THEN 'Mañana'
            WHEN Hora BETWEEN 14 AND 21 THEN 'Tarde'
            ELSE 'Noche'
        END
    """))
)

df_dim_hora.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_flota_silver.dim_hora")
```

---

## 4. Biblioteca de Medidas DAX Avanzadas

### A. Medida para Torre de Control: Última Posición Conocida (`Es_Ultima_Posicion`)
Aísla la posición geolocalizada más reciente de cada vehículo para evitar dibujar trazos superpuestos en el mapa en vivo:

```dax
Es_Ultima_Posicion = 
VAR MaxEventoVehiculo = 
    CALCULATE(
        MAX('telemetria_enriquecida'[Hora_Evento]),
        ALLEXCEPT('telemetria_enriquecida', 'telemetria_enriquecida'[VehicleID])
    )
RETURN 
    IF(
        SELECTEDVALUE('telemetria_enriquecida'[Hora_Evento]) = MaxEventoVehiculo,
        1,
        0
    )
```

### B. Medida de Alerta de Cumplimiento DGT / RRHH (`2_Alerta_Operativa_RRHH`)
Audita el estado legal y médico del conductor evitando errores de evaluación en campos booleanos mediante `SELECTEDVALUE`:

```dax
2_Alerta_Operativa_RRHH = 
VAR Saldo = [1_Saldo_Puntos_Actual]
VAR Medico_Apto = SELECTEDVALUE('dim_conductores'[Apto_Medico])
VAR Permiso = SELECTEDVALUE('dim_conductores'[Estado_Permiso])

RETURN
SWITCH(
    TRUE(),
    Medico_Apto = FALSE(), "🔴 SUSPENSIÓN: No Apto Médico (Reasignar a Oficina/Cochera)",
    Permiso = "Caducado", "🔴 SUSPENSIÓN: Permiso de Conducir Caducado",
    ISBLANK(Saldo), "Sin Datos",
    Saldo >= 8, "🟢 OK (Operativo)",
    Saldo >= 4, "🟡 AVISO: Curso Recuperación Parcial",
    Saldo >= 1, "🟠 CRÍTICO: Pre-reemplazo",
    "🔴 SUSPENSIÓN: Retirada de Licencia por Puntos"
)
```

### C. Medida de Nivel Crítico de Combustible (`Vehiculos_Combustible_Critico`)
```dax
Vehiculos_Combustible_Critico = 
COALESCE(
    CALCULATE(
        DISTINCTCOUNT('telemetria_enriquecida'[VehicleID]),
        'telemetria_enriquecida'[FuelLevel] < 15
    ),
    0
)
```
