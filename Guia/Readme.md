# 🚀 Guía de Replicación Paso a Paso (End-to-End) - v2

Esta guía documenta el procedimiento secuencial completo para desplegar y replicar desde cero la **Plataforma Analítica IoT y ERP de Flotas NRT en Microsoft Fabric**, combinando la infraestructura de simulación IoT en **Docker (Ubuntu)**, la arquitectura de datos **Medallion en Microsoft Fabric (OneLake & Direct Lake)** y el diseño del cuadro de mando corporativo multidepartamental en **Power BI**.

---

## 📋 Prerrequisitos

1. **Entorno Cloud**:
   - Suscripción activa o capacidad de prueba (*Trial*) en **Microsoft Fabric**.
   - Permisos para crear Workspaces, Eventstreams, Lakehouses, Data Factory Pipelines, Modelos Semánticos e Informes de Power BI.

2. **Entorno Local / Edge**:
   - Máquina virtual o servidor Linux (**Ubuntu 22.04 LTS o 24.04 LTS**).
   - **Docker Engine** v24+ y **Docker Compose V2** instalados.
   - Git instalado.

3. **Repositorio de Código**:
   - Clonar el repositorio oficial desde tu cuenta de GitHub e ingresar a la carpeta del proyecto.

---

## 🛠️ Fase 1: Despliegue de la Ingesta IoT Local (Ubuntu + Docker)

### Paso 1.1: Obtención de la Cadena de Conexión de Eventstream
1. Accede a **Microsoft Fabric** (`app.fabric.microsoft.com`).
2. Crea el área de trabajo **`WS_Fabric_Flota_NRT`**.
3. Crea un artefacto de tipo **Eventstream** denominado **`es_flota_nrt`**.
4. Añade un origen de tipo **Custom App** (Aplicación personalizada).
5. Copia la **Cadena de conexión primaria (AMQP / EventHub)** proporcionada por Fabric.

### Paso 1.2: Configuración del Entorno Local
1. Ve a la carpeta `Docker/` del proyecto: `cd Docker`
2. Crea el archivo de variables de entorno `.env` basándote en `.env.example`: `cp .env.example .env`
3. Edita `.env` con tu editor preferido (`nano .env`) y pega la cadena de conexión obtenida en Fabric:
   - `EVENTSTREAM_CONNECTION_STRING=<CADENA_DE_CONEXION_FABRIC>`
   - `EVENTSTREAM_NAME=es_flota_nrt`

### Paso 1.3: Construcción y Arranque de Contenedores
Ejecuta Docker Compose para compilar la imagen Python y levantar las 3 réplicas simuladoras (correspondientes a los 3 bloques de rutas de la EMT de Madrid):
- `docker compose up -d --build`

### Paso 1.4: Verificación de la Telemetría
Comprueba que los tres contenedores estén en estado *Running* y transmitiendo lotes cada 10 segundos:
- `docker compose ps`
- `docker compose logs -f --tail=20`

---

## 🟤 Fase 2: Configuración de la Capa Bronce en Microsoft Fabric

### Paso 2.1: Creación del Lakehouse Bronce
1. En el área de trabajo **`WS_Fabric_Flota_NRT`**, crea un artefacto **Lakehouse** llamado **`lh_flota_bronce`**.

### Paso 2.2: Enrutamiento del Eventstream a Delta Lake
1. Abre el Eventstream **`es_flota_nrt`**.
2. Añade un destino de tipo **Lakehouse**.
3. Selecciona `lh_flota_bronce` y asigna la tabla Delta de destino como **`telemetria_nrt_v2`**.
4. Publica los cambios y verifica que los registros comiencen a ingresar en streaming.

### Paso 2.3: Ingesta Batch de Referencia Espacial
1. Ejecuta el cuaderno **`nb_bronce_ingesta_paradas_emt.Notebook`** (ubicado en la carpeta `Fabric/`).
2. Este cuaderno descargará e ingesta las 1.007 paradas de la red de autobuses de Madrid en la tabla **`lh_flota_bronce.dim_paradas_emt`**.

---

## ⚪ Fase 3: Procesamiento y Gobernanza en la Capa Plata (PySpark)

### Paso 3.1: Creación del Lakehouse Plata
1. Crea un nuevo Lakehouse en Fabric denominado **`lh_flota_silver`**.

### Paso 3.2: Generación de Dimensiones Maestras y ERP
Ejecuta secuencialmente los cuadernos de dimensiones en la Capa Plata:
1. **`nb_silver_crear_conductores.Notebook`**: Genera la dimensión de RRHH (`dim_conductores`) con 150 empleados, turnos, experiencia, reconocimientos médicos y puntos DGT.
2. **`nb_silver_crea_proveedores.Notebook`**: Genera el catálogo de talleres mecánicos externos (`dim_proveedores`) con SLAs y tarifas, además del enriquecimiento ambiental de vehículos (CERO, ECO, Euro 6).
3. **`nb_silver_crea_codigos_dtc.Notebook`**: Crea el diccionario de códigos de averías de motor (`dim_dtc_codigos`).
4. **`nb_silver_dimensiones_maestras.Notebook`**: Genera `dim_vehiculos_tecnica`, `fact_mantenimientos`, `fact_multas`, `fact_checklist_diario`, `fact_control_jornada` y `fact_incidencias`.

### Paso 3.3: Enriquecimiento NRT y Data Quality Gate
Ejecuta el cuaderno principal **`nb_silver_enriquecimiento_nrt.Notebook`**:
- **Función**: Une las tablas Bronce, realiza el cruce relacional con `dim_conductores`, proyecta los campos de partición temporal (`Fecha_Evento`, `Hora`) y el identificador `id_empleado`.
- **Data Quality Gate**: Evalúa automáticamente el volumen de datos y la frescura de la telemetría. Si el retraso supera los **45 minutos**, se fuerza una excepción diagnóstica con Runbook de resolución.

---

## 🟡 Fase 4: Modelo Semántico en Estrella V2 (Direct Lake)

### Paso 4.1: Creación del Modelo Semántico
1. En `lh_flota_silver`, haz clic en **Nuevo modelo semántico** y nómbralo **`SM_Flota_Enriquecida`**.
2. Selecciona las tablas de la Capa Plata: `telemetria_enriquecida`, `dim_conductores`, `dim_vehiculos_tecnica`, `dim_proveedores`, `dim_dtc_codigos`, `dim_calendario`, `dim_hora`, `fact_mantenimientos`, `fact_multas`, `fact_checklist_diario`, `fact_control_jornada`, `fact_incidencias` y `dim_paradas_emt`.

### Paso 4.2: Definición de Relaciones (1:*)
Establece las 13 relaciones de cardinalidad **1 a Varios (1:*)** con dirección de filtro único:
- `dim_vehiculos_tecnica[VehicleID]` ➔ `telemetria_enriquecida[VehicleID]`
- `dim_conductores[id_empleado]` ➔ `telemetria_enriquecida[id_empleado]`
- `dim_proveedores[ID_Proveedor]` ➔ `fact_mantenimientos[ID_Proveedor]`
- `dim_dtc_codigos[DTC_Code]` ➔ `telemetria_enriquecida[DTC_Code]`
- `dim_calendario[Fecha]` ➔ `telemetria_enriquecida[Fecha_Evento]`
- `dim_hora[Hora]` ➔ `telemetria_enriquecida[Hora]`
- `dim_conductores[id_empleado]` ➔ `fact_control_jornada[id_empleado]`
- `dim_conductores[id_empleado]` ➔ `fact_multas[id_empleado]`
- `dim_conductores[id_empleado]` ➔ `fact_incidencias[id_empleado_reportador]`
- `dim_vehiculos_tecnica[VehicleID]` ➔ `fact_mantenimientos[VehicleID]`
- `dim_vehiculos_tecnica[VehicleID]` ➔ `fact_incidencias[VehicleID]`
- `dim_vehiculos_tecnica[VehicleID]` ➔ `fact_checklist_diario[VehicleID]`
- `dim_calendario[Fecha]` ➔ `fact_mantenimientos[Fecha_Entrada_Taller]`
*(Mantén `dim_paradas_emt` desconectada para servir como capa espacial sin filtrar las transacciones)*.

---

## 🎨 Fase 5: Diseño y Construcción de los Informes Visuales en Power BI (`VistaRutaFlota`)

### Paso 5.1: Configuración del Sistema de Diseño (Dark Mode)
Para salas de control operativo (NOC) y presentaciones ejecutivas:
- **Colores del Lienzo**: Fondo antracita `#1E1E24`, tarjetas `#25272A`, bordes `#3A3D40` (1 px con radio de 6 px).
- **Tipografía**: Segoe UI / DIN. Título de informe a 18 pt negrita, títulos de tarjetas a 11–12 pt seminegrita, llamadas KPI a **26 pt negrita** (cambiando "Mostrar unidades" a "Ninguno").
- **Acentos**: Verde `#2ECC71`, Ámbar `#F59E0B`, Rojo `#EF4444`, Azul `#3B82F6`.

### Paso 5.2: Página 1 - Torre de Control de Flotas (Operaciones NRT)
1. **Filtros Superiores (Slicers)**:
   - `dim_calendario[Fecha]`
   - `dim_hora[Franja_Horaria]`
   - `dim_vehiculos_tecnica[VehicleID]`
2. **Cabecera de KPIs (4 Tarjetas)**:
   - `Vehiculos activos` = `COALESCE(DISTINCTCOUNT('telemetria_enriquecida'[VehicleID]), 0)`
   - `Velocidad_Media_Flota` = `ROUND(COALESCE(AVERAGE('telemetria_enriquecida'[Speed]), 0), 1)`
   - `Vehiculos_Combustible_Critico` = `CALCULATE(DISTINCTCOUNT('telemetria_enriquecida'[VehicleID]), 'telemetria_enriquecida'[FuelLevel] < 15)`
   - `Conductores_En_Servicio` = `DISTINCTCOUNT('telemetria_enriquecida'[Conductor_Activo])`
3. **Visualizador de Azure Maps (Centro de Control)**:
   - **Capa Principal**: `Latitude` y `Longitude` de `telemetria_enriquecida`, `VehicleID` en Leyenda.
   - **Capa de Referencia (Reference Layer)**: Subir `paradas_madrid_filtradas.geojson` (1.007 marquesinas oficiales de ida). Estilo "Gris oscuro".
   - **Filtro Anti-Maraña**: Arrastrar la medida `Es_Ultima_Posicion` al panel de "Filtros de este objeto visual" y fijar la regla `= 1` para aislar los 45 marcadores vivos en tiempo real.
4. **Títulos Dinámicos Combinados**:
   - Crear las medidas `Fechas_Seleccionadas` y `Horas_Seleccionadas` con `CONCATENATEX(VALUES(...), ..., ", ")`.
   - Asignar `Titulo_Cabecera_Operaciones` = `"Torre de Control de Flota | Fecha: " & [Fechas_Seleccionadas] & " | Horas: " & [Horas_Seleccionadas]` mediante la función *fx* del título de Azure Maps.

### Paso 5.3: Página 2 - Compliance y RRHH / Prevención (PRL)
1. **Semáforo Dinámico Jerárquico DAX**:
   - Implementar `2_Alerta_Operativa_RRHH` con `SELECTEDVALUE`:
     ```dax
     2_Alerta_Operativa_RRHH = 
     VAR Saldo = [1_Saldo_Puntos_Actual]
     VAR Medico_Apto = SELECTEDVALUE('dim_conductores'[Apto_Medico])
     VAR Permiso = SELECTEDVALUE('dim_conductores'[Estado_Permiso])
     RETURN
     SWITCH(
         TRUE(),
         Medico_Apto = FALSE(), "🔴 SUSPENSIÓN: No Apto Médico (Reasignar a Oficina/Cochera)",
         Permiso = "Caducado", "🔴 SUSPENSIÓN: Permiso Caducado",
         ISBLANK(Saldo), "Sin Datos",
         Saldo >= 8, "🟢 OK (Operativo)",
         Saldo >= 4, "🟡 AVISO: Curso Recuperación",
         Saldo >= 1, "🟠 CRÍTICO: Pre-reemplazo",
         "🔴 SUSPENSIÓN: Retirada de Licencia (0 pts)"
     )
     ```
   - Formato condicional de fondo de celda aplicando `3_Color_Alerta_RRHH` con "Valor de campo".
2. **Tabla Maestra de Conductores**: `nombre_conductor`, `turno`, `vehiculo_asignado`, `Estado_Permiso`, `1_Saldo_Puntos_Actual`, `2_Alerta_Operativa_RRHH`.
3. **Tarjetas KPI**:
   - `Coste_Multas_Total` = `SUM('fact_multas'[Importe_EUR])` (€, 0 decimales).
   - `Conductores_Suspendidos` = Conteo de inhabilitados por saldo, carnet caducado o no apto médico.
   - `No_Aptos_Medicos` = Conteo de evaluaciones médicas negativas.

### Paso 5.4: Página 3 - Calidad (QoS) y Sostenibilidad ESG
1. **Tarjetas KPI**:
   - `% Flota Cero Emisiones` = `DIVIDE(CALCULATE(COUNTROWS('dim_vehiculos_tecnica'), 'dim_vehiculos_tecnica'[Clasificacion_Ambiental] = "CERO"), COUNTROWS('dim_vehiculos_tecnica'), 0)` (formato %).
   - `Checklists con Daños Pre-Turno` = `CALCULATE(COUNTROWS('fact_checklist_diario'), 'fact_checklist_diario'[Estado_Visual] = "Con Daños")`.
2. **Gráfico de Donut**: Distribución de la flota por etiqueta ambiental (`CERO`, `ECO`, `Euro 6`).
3. **Tabla de Helpdesk & Incidencias**: `ID_Incidencia`, `VehicleID`, `Tipo_Incidencia`, `Prioridad`, `Estado` y tiempo medio de resolución (MTTR).

### Paso 5.5: Página 4 - FinOps y Mantenimiento Técnico
1. **Tarjetas KPI**:
   - `Gasto Total Talleres` = `SUM('fact_mantenimientos'[Coste_Total_Intervencion])` (€).
   - `Días Totales Inmovilizado` = `SUM('fact_mantenimientos'[Dias_Inmovilizado])`.
   - `Vehículos con ITV Próxima` = `CALCULATE(COUNTROWS('dim_vehiculos_tecnica'), 'dim_vehiculos_tecnica'[Caducidad_ITV] <= TODAY() + 30)`.
2. **Gráfico de Barras Agrupadas**: Coste acumulado por taller (`dim_proveedores[Nombre_Taller]`) desglosado por tipo (`Preventivo` vs `Correctivo`).
3. **Matriz de Auditoría y SLA**:
   - Convertir tabla en **Matriz** con jerarquía de desglose (*Drill-Down*): `VehicleID` ➔ `Conductor_Activo` ➔ `Hora_Evento`.
   - Medida de cumplimiento de SLA: `Excede_SLA` = `IF(SELECTEDVALUE('fact_mantenimientos'[Dias_Inmovilizado]) > SELECTEDVALUE('dim_proveedores'[SLA_Dias]), "⚠️ Retraso", "✅ En Plazo")`.
   - **Tabla de Auditoría de Reserva Crítica de Combustible (<15%)**: Trasladada a esta página para auditar historiales de `FuelLevel < 15` sin interferir con la vista ejecutiva en vivo de la Torre de Control.

---

## ⚙️ Fase 6: Orquestación, Mantenimiento Delta y Alertas

### Paso 6.1: Pipeline de Procesamiento
1. Importa el Data Pipeline **`pl_procesamiento_flota_plata.DataPipeline`**.
2. Configura un **Trigger programado** cada 15 minutos para ejecutar el cuaderno `nb_silver_enriquecimiento_nrt`.
3. Añade la actividad de **Refresco del Modelo Semántico** (`SM_Flota_Enriquecida`) activada *On Success*.

### Paso 6.2: Pipelines Nocturnos de Mantenimiento Delta
1. Importa `pl_bronce_mantenimiento.DataPipeline` (Programado a las 02:00 AM) para ejecutar `OPTIMIZE` y `VACUUM` en Bronce.
2. Importa `pl_silver_mantenimiento.DataPipeline` (Programado a las 02:30 AM) para ejecutar `OPTIMIZE` y `VACUUM` en Plata.

### Paso 6.3: Alertas Proactivas en Data Activator (Reflex)
1. Abre el artefacto **`act_flota_alertas_nrt.Reflex`**.
2. Configura la regla para monitorizar `telemetria_enriquecida[FuelLevel] < 15`.
3. Establece la acción de enviar una notificación automática por correo/Teams al centro de control.

---

## 🔄 Fase 7: Sincronización del Workspace con GitHub

1. En Microsoft Fabric, ve a **Configuración del área de trabajo** ➔ **Integración con Git**.
2. Conecta tu cuenta de GitHub y selecciona el repositorio **`flota-docker-nrt-fabric`** y la rama `main`.
3. Haz clic en **Sincronizar** para mantener el código de tu área de trabajo controlado por versiones.

---

¡Felicidades! La plataforma analítica IoT de gestión de flotas y sus cuadros de mando están completamente replicados y operativos.