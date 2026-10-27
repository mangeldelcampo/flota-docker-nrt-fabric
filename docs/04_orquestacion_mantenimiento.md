# ⚙️ Módulo 4: Orquestación Data Factory, Mantenimiento Delta y Data Activator

## 1. Pipeline Principal de Orquestación (`pl_procesamiento_flota_plata`)

La canalización **`pl_procesamiento_flota_plata`** en Data Factory gestiona el ciclo operativo recurrente del sistema en ventanas de **15 minutos**.

```
 [ Schedule Trigger (15 min) ]
               │
               ▼
   [ Actividad Cuaderno: nb_silver_enriquecimiento_nrt ]
               │
      ┌────────┴────────┐
  (On Success)     (Upon Failure)
      │                 │
      ▼                 ▼
 [ Actividad: Refresco Modelo ]   [ Notificación Correo Outlook ]
  SM_Flota_Enriquecida
```

### Pasos de la Canalización:
1. **Actividad de Cuaderno (`nb_silver_enriquecimiento_nrt`)**: Despierta el clúster Spark, consolida la Capa Plata y evalúa las tres reglas del **Data Quality Gate**.
2. **Encadenamiento *On Success***: Si el cuaderno supera los controles de calidad, activa automáticamente la actividad **Actualizar modelo semántico** sobre `SM_Flota_Enriquecida`, sincronizando los archivos Parquet físicos con la memoria VertiPaq en Direct Lake.
3. **Manejo de Errores (*Upon Failure*)**: Si el Quality Gate aborta la ejecución (por datos estancados > 45 min), se detiene la canalización y se envía una notificación de alerta por correo electrónico.

---

## 2. Pipelines Nocturnos Desacoplados de Mantenimiento Delta

Para resolver el problema de micro-archivos (*Small Files Problem*) derivado del streaming continuo y evitar contenciones de transacciones durante la jornada operativa:

### A. Pipeline de Mantenimiento Bronce (`pl_bronce_mantenimiento`)
* **Invocación**: Cuaderno `nb_bronce_mantenimiento`.
* **Programación**: Diario a las **02:00 AM (UTC+01:00 Madrid)**.
* **Instrucciones Ejecutadas**:
  ```sql
  OPTIMIZE lh_flota_bronce.telemetria_nrt_v2;
  VACUUM lh_flota_bronce.telemetria_nrt_v2 RETAIN 168 HOURS;
  ```

### B. Pipeline de Mantenimiento Plata (`pl_silver_mantenimiento`)
* **Invocación**: Cuaderno `nb_silver_mantenimiento`.
* **Programación**: Diario a las **02:30 AM (UTC+01:00 Madrid)** (desfase de 30 minutos).
* **Instrucciones Ejecutadas**:
  ```sql
  OPTIMIZE lh_flota_silver.telemetria_enriquecida;
  VACUUM lh_flota_silver.telemetria_enriquecida RETAIN 168 HOURS;

  OPTIMIZE lh_flota_silver.dim_conductores;
  VACUUM lh_flota_silver.dim_conductores RETAIN 168 HOURS;

  OPTIMIZE lh_flota_silver.dim_vehiculos_tecnica;
  VACUUM lh_flota_silver.dim_vehiculos_tecnica RETAIN 168 HOURS;

  OPTIMIZE lh_flota_silver.dim_proveedores;
  VACUUM lh_flota_silver.dim_proveedores RETAIN 168 HOURS;

  OPTIMIZE lh_flota_silver.fact_mantenimientos;
  VACUUM lh_flota_silver.fact_mantenimientos RETAIN 168 HOURS;

  OPTIMIZE lh_flota_silver.fact_checklist_diario;
  VACUUM lh_flota_silver.fact_checklist_diario RETAIN 168 HOURS;
  ```

---

## 3. Alertas Proactivas 24/7 con Data Activator (`act_flota_alertas_nrt`)

El componente **Data Activator (Reflex)** supervisa continuamente el flujo telemático en tiempo real sin requerir la apertura manual del cuadro de mando.

### Regla de Negocio: Combustible Crítico en Ruta
* **Origen**: Conexión al stream `es_flota_nrt-stream`.
* **Entidad de Negocio**: Agrupado por `VehicleID`.
* **Condición de Disparo**:
  $$	ext{FuelLevel} < 10.0 \quad 	ext{AND} \quad 	ext{Speed} > 0$$
* **Acción Automatizada**: Envío inmediato de notificación por Microsoft Teams / Correo a la Central de Operaciones indicando ID de vehículo, conductor activo, nivel de reserva y coordenadas GPS.
