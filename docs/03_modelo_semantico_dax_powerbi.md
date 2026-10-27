# 🟡 Módulo 3: Modelo Semántico en Estrella Direct Lake (17 Relaciones y 28 Medidas DAX)

## 1. Topología del Modelo Semántico (`SM_Flota_Enriquecida`)

El Modelo Semántico **`SM_Flota_Enriquecida`** constituye la Capa Oro corporativa. Diseñado bajo un esquema de **Constelación en Estrella**, se ejecuta sobre el motor VertiPaq en modo **Direct Lake**, consultando directamente los archivos Delta Parquet en OneLake.

* **Tablas Totales:** 13 (11 conectadas al grafo relacional, 1 tabla de referencia espacial desacoplada `dim_paradas_emt` y 1 tabla técnica/soporte).
* **Relaciones Activas:** 17 relaciones (todas de cardinalidad 1 a Varios `1:*` y filtro de dirección única).
* **Medidas DAX:** 28 medidas analíticas distribuidas entre los 4 dominios de negocio.
* **Expresiones:** 1 expresión técnica.
```
                                     ┌─────────────────────────┐
                                     │      dim_calendario     │
                                     └────────────┬────────────┘
                                                  │ (4 relaciones 1:*)
     ┌───────────────────────┐                    │                  ┌─────────────────────────┐
     │    dim_conductores    │                    │                  │  dim_vehiculos_tecnica  │
     └───────────┬───────────┘                    │                  └────────────┬────────────┘
                 │ (5 relaciones 1:*)             ▼                               │ (5 relaciones 1:*)
                 ├───────────────────────►┌────────────────────────┐◄─────────────┤
                 │                        │  telemetria_enriquecida│              │
                 │          ┌────────────►└───────────┬────────────┘              │
                 │          │ (1:*)                   │ (1:*)                     │
                 │   ┌──────┴───────────┐      ┌──────▼──────┐                    │
                 │   │  dim_dtc_codigos │      │   dim_hora  │                    │
                 │   └──────────────────┘      └─────────────┘                    │
                 │                                                                │
  ┌──────────────┼─────────────────────────┬──────────────────────────┐           │
  │              │                         │                          │           │
  ▼              ▼                         ▼                          ▼           ▼
┌─────────────┐┌────────────────────┐    ┌──────────────────────┐   ┌─────────────┴───────┐
│ fact_multas ││fact_control_jornada│    │fact_checklist_diario │   │ fact_mantenimientos │
└─────────────┘└────────────────────┘    └──────────────────────┘   └─────────────┬───────┘
                                                                                  ▲
                                                ┌───────────────────────┐         │ (1:*)
                                                │    dim_proveedores    │─────────┘
                                                └───────────────────────┘

┌────────────────────────────────────────┐
│            fact_incidencias            │ ◄─── Conectada a dim_vehiculos_tecnica, dim_conductores
└────────────────────────────────────────┘      y dim_calendario

┌────────────────────────────────────────┐
│            dim_paradas_emt             │ (DESCONECTADA: Capa Geoespacial para Azure Maps)
└────────────────────────────────────────┘
└──────────────────────────────────────┘
```

---
## 2. Catálogo Oficial de las 17 Relaciones Activas (1:N, Filtro Cruzado Único)

### A. Eje Hardware y Activos Físicos (`dim_vehiculos_tecnica`) - 5 Relaciones
1. `dim_vehiculos_tecnica[VehicleID]` ──(1:*)──► `telemetria_enriquecida[VehicleID]` (Telemetría viva, velocidad y combustible).
2. `dim_vehiculos_tecnica[VehicleID]` ──(1:*)──► `fact_multas[VehicleID]` (Historial y reincidencia de sanciones por autobús).
3. `dim_vehiculos_tecnica[VehicleID]` ──(1:*)──► `fact_checklist_diario[VehicleID]` (Inspecciones visuales pre-turno por unidad).
4. `dim_vehiculos_tecnica[VehicleID]` ──(1:*)──► `fact_mantenimientos[VehicleID]` (Costes de taller y días inmovilizado).
5. `dim_vehiculos_tecnica[VehicleID]` ──(1:*)──► `fact_incidencias[VehicleID]` (Tickets de averías mecánicas).

### B. Eje Personal y Recursos Humanos (`dim_conductores`) - 5 Relaciones
6. `dim_conductores[id_empleado]` ──(1:*)──► `telemetria_enriquecida[id_empleado]` (Identificación del conductor activo al volante).
7. `dim_conductores[id_empleado]` ──(1:*)──► `fact_multas[id_empleado]` (Asignación legal de expedientes e imputación de puntos DGT).
8. `dim_conductores[id_empleado]` ──(1:*)──► `fact_checklist_diario[id_empleado]` (Responsable del reporte de daños en cochera).
9. `dim_conductores[id_empleado]` ──(1:*)──► `fact_control_jornada[id_empleado]` (Control de fichajes, horas efectivas y absentismo).
10. `dim_conductores[id_empleado]` ──(1:*)──► `fact_incidencias[id_empleado_reportador]` (Conductor o empleado que abre el ticket).

### C. Eje Temporal y de Calendario (`dim_calendario`) - 4 Relaciones
11. `dim_calendario[Fecha]` ──(1:*)──► `telemetria_enriquecida[Fecha_Evento]` (Eje temporal para telemetría NRT).
12. `dim_calendario[Fecha]` ──(1:*)──► `fact_control_jornada[Fecha_Fichaje]` (Análisis histórico de jornada y turnicidad).
13. `dim_calendario[Fecha]` ──(1:*)──► `fact_mantenimientos[Fecha_Entrada_Taller]` (Control presupuestario y estacionalidad de averías).
14. `dim_calendario[Fecha]` ──(1:*)──► `fact_incidencias[Fecha_Apertura]` (Evolución y seguimiento de tickets en el tiempo).

### D. Ejes Especializados: Franja Horaria, Proveedores y Diagnosis - 3 Relaciones
15. `dim_hora[Hora]` ──(1:*)──► `telemetria_enriquecida[Hora]` (Segmentación intradiaria de 0 a 23h y franjas horarias).
16. `dim_dtc_codigos[DTC_Code]` ──(1:*)──► `telemetria_enriquecida[DTC_Code]` (Diagnóstico de fallos OBD-II en tiempo real).
17. `dim_proveedores[ID_Proveedor]` ──(1:*)──► `fact_mantenimientos[ID_Proveedor]` (Control de talleres colaboradores, tarifas y SLAs).

> **Gobernanza de Capas Espaciales:** La tabla `dim_paradas_emt` (1.007 paradas) se mantiene **desconectada** del grafo relacional como capa de referencia espacial WKT/GeoJSON en Azure Maps para no alterar el contexto de filtro analítico.
---

## 3. Catálogo Consolidado de las 28 Medidas DAX

El modelo semántico implementa **28 medidas analíticas** distribuidas entre las 4 páginas del informe directivo:

### I. Torre de Control de Operaciones (NRT)
1. `Es_Ultima_Posicion`: Filtra el último evento temporal por autobús para el renderizado cartográfico en Azure Maps.
2. `Vehiculos_Activos`: Conteo de autobuses emitiendo telemetría en la ventana temporal activa.
3. `Velocidad_Media_Flota`: Media aritmética de la velocidad (`Speed`) en servicio.
4. `Nivel_Combustible_Actual`: Nivel porcentual instantáneo del depósito para la flota activa.
5. `Vehiculos_Combustible_Critico`: Recuento de vehículos con `FuelLevel < 15%`.
6. `Filtro_Alerta_Combustible`: Flag condicional de coloración para avisos operativos.
7. `Total_Eventos_NRT`: Paquetes telemáticos totales procesados en streaming.

### II. Compliance de RRHH, PRL y Puntos DGT
8. `1_Saldo_Puntos_Actual`: Saldo neto de puntos DGT (`Puntos_Iniciales - Puntos_Retirados`).
9. `2_Alerta_Operativa_RRHH`: Clasificación textual de aptitud (🟢 OK, 🟡 Aviso, 🟠 Crítico, 🔴 Suspensión).
10. `3_Color_Alerta_RRHH`: Código de color HEX para formato condicional dinámico en tabla maestra.
11. `Conductores_Suspendidos`: Chóferes inhabilitados (puntos = 0, no apto médico o carnet caducado).
12. `Alertas_Carnet`: Permisos vencidos o con renovación obligatoria < 30 días.
13. `Coste_Multas_RRHH`: Suma económica blindada de sanciones (`COALESCE(SUM(Importe_EUR), 0)`).
14. `Multas_Sin_Conductor`: Control de fuga financiera por multas huérfanas sin identificar.
15. `Total_Infracciones`: Número total de expedientes sancionadores abiertos.
16. `Conductores_Activos`: Total de plantilla asignada a turnos en servicio.

### III. Calidad de Servicio y Sostenibilidad (ESG)
17. `Pct_Flota_Cero`: Cuota porcentual de autobuses eléctricos CERO emisiones netas.
18. `Vehiculos_CERO_Total`: Conteo absoluto de unidades catalogadas como `CERO`.
19. `Vehiculos_ECO_Total`: Conteo absoluto de unidades catalogadas como `ECO`.
20. `Vehiculos_Euro6_Total`: Conteo de unidades de combustión estándar `Euro 6`.
21. `Checklists_Con_Danos`: Inspecciones pre-ruta con desperfectos (`Estado_Visual IN {"Con Daños", "Con Daños Previos"}`).
22. `Total_Checklists`: Revisiones totales realizadas antes de salida a línea.
23. `Tasa_Incidentes_PreRuta`: Ratio porcentual de checklists con desperfectos sobre el total de salidas.

### IV. FinOps de Flota y Control de Mantenimiento
24. `Gasto_Total_Mantenimiento`: Facturación total en talleres (`Coste_Materiales_EUR + Coste_ManoObra_EUR`).
25. `Gasto_Mantenimiento_Correctivo`: Coste económico imputable a averías imprevistas.
26. `Gasto_Mantenimiento_Preventivo`: Coste económico imputable a revisiones programadas.
27. `Pct_Gasto_Correctivo`: Cuota porcentual del presupuesto consumida por reparaciones correctivas.
28. `Vehiculos_SLA_Excedido`: Órdenes de trabajo cuyos `Dias_Inmovilizado` superan el `SLA_Dias` contractual.
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
