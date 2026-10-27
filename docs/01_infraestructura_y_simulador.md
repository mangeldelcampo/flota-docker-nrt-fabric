# 🚚 Módulo 1: Infraestructura Local, Simulador IoT en Docker y Transmisión AMQP

## 1. Arquitectura de Simulación Telemática (Edge / On-Premise)

El módulo de simulación telemática actúa como el emisor primario de datos IoT de la flota de autobuses urbanos. En lugar de ejecutar bucles de simulación dentro de Microsoft Fabric (lo que consumiría innecesariamente Unidades de Capacidad - CUs de clústeres Spark optimizados para analítica batch), la simulación se despliega fuera de la nube en un entorno desacoplado sobre una Máquina Virtual Linux Ubuntu con Docker Engine.

```
 [ Servidor Linux Ubuntu (Edge / On-Premise) ]
   ├── Docker Container 1 (simulator-1): Vehículos VEH-1001 a VEH-1015
   ├── Docker Container 2 (simulator-2): Vehículos VEH-1016 a VEH-1030
   └── Docker Container 3 (simulator-3): Vehículos VEH-1031 a VEH-1045
            │
            │ (Peticiones AMQP asíncronas con TLS/SAS)
            ▼
   [ Microsoft Fabric Eventstream: es_flota_nrt ]
```

### Justificación de Arquitectura Técnica:
* **Separación de Responsabilidades (Producer vs. Consumer)**: Los vehículos reales son dispositivos embarcados (*Edge Computing*) ajenos a la nube analítica. Alojar la generación de datos dentro de Fabric desvirtuaría el principio de diseño de una plataforma IoT empresarial.
* **Eficiencia de Cómputo en Fabric**: Los clústeres PySpark en Fabric están concebidos para procesar grandes volúmenes de datos por lotes o micro-batches en segundos y liberar recursos, no para actuar como demonios de servicio en bucle continuo `while True` con tiempos de inactividad (`sleep`).
* **Escalabilidad Horizontal**: Cada contenedor Docker administra un bloque independiente de la flota, permitiendo simular mayor densidad de tráfico simplemente aumentando el número de réplicas en el archivo de orquestación.

---

## 2. Linaje y Preparación de Datos Topológicos (`preparar_datos.py`)

Para garantizar que la simulación refleje el comportamiento del transporte público de Madrid, se seleccionó un conjunto representativo de **45 rutas comerciales en sentido de ida (`toA`)** del Portal de Datos Abiertos del Ayuntamiento de Madrid (dataset de la EMT).

### Desafíos Técnicos Resueltos en la Ingesta Batch Previa:
1. **Mapeo por Labels vs. Lines**: Mapeo entre nombres comerciales (`C1`, `E3`, `N22`) y sus identificadores numéricos oficiales de línea.
2. **Superación del Límite de Caracteres de Excel**: Las geometrías GeoJSON complejas de la columna `itinerary` superaban el límite estándar de 32.767 caracteres por celda en Microsoft Excel, provocando el truncamiento del trazado.
3. **Procesamiento ETL con Python/Pandas (`preparar_datos.py`)**: Se desarrolló un script en Python que importó los archivos brutos en formato UTF-8, sorteó las restricciones de Excel, eliminó la columna `stops` (para reducir la huella en memoria) y exportó el archivo maestro `data/topologia_rutas_flota_nrt.csv` delimitado por punto y coma (`;`).
4. **Partición para Contenedores**: El dataset clasifica cada ruta asociándola a un contenedor objetivo (`Contenedor_1`, `Contenedor_2`, `Contenedor_3`), garantizando que cada microservicio lea únicamente las líneas que le corresponden.

---

## 3. Código Fuente del Emisor Telemático (`Docker/simulator.py`)

El script `Docker/simulator.py` es un cliente asíncrono desarrollado en Python 3.11 que lee el dataset topológico maestro y transmite lotes de telemetría continuos hacia Fabric Eventstream mediante el protocolo AMQP.

```python
import ast
import asyncio
import csv
import json
import os
import random
import sys
from datetime import datetime, timezone
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

# 1. Elevar el límite de tamaño de campo CSV para soportar itinerarios GeoJSON extensos
csv.field_size_limit(sys.maxsize)

# 2. Inyección de variables de entorno desde Docker Compose
CONNECTION_STR = os.environ.get("EVENTSTREAM_CONNECTION_STRING")
EVENTHUB_NAME = os.environ.get("EVENTSTREAM_NAME")
CONTAINER_ID = os.environ.get("CONTAINER_ID", "Contenedor_1")
VEHICLE_START_ID = int(os.environ.get("VEHICLE_START_ID", "1001"))
VEHICLE_END_ID = int(os.environ.get("VEHICLE_END_ID", "1015"))
ROUTE_FILE = os.environ.get("ROUTE_FILE", "/app/topologia_rutas_flota_nrt.csv")

def extraer_coordenadas_itinerario(itinerary_str):
    """Parsea el campo itinerary soportando JSON estándar o diccionarios Python (comillas simples)
    y estructuras GeoJSON FeatureCollection ('toA')."""
    data = None
    try:
        data = json.loads(itinerary_str)
    except Exception:
        try:
            data = ast.literal_eval(itinerary_str)
        except Exception as e:
            return None

    if not data:
        return None

    # Extracción GeoJSON de la EMT (FeatureCollection -> geometry -> coordinates)
    if isinstance(data, dict):
        bloque = data.get("toA") or data.get("toB") or data
        if isinstance(bloque, dict) and "features" in bloque:
            coords = []
            for feat in bloque.get("features", []):
                geom = feat.get("geometry", {})
                gtype = geom.get("type", "")
                gcoords = geom.get("coordinates", [])
                if gtype == "LineString":
                    for pt in gcoords:
                        coords.append((float(pt[1]), float(pt[0])))  # (Lat, Lon)
                elif gtype == "MultiLineString":
                    for linea in gcoords:
                        for pt in linea:
                            coords.append((float(pt[1]), float(pt[0])))
            if len(coords) > 1:
                return coords

    # Coordenadas en lista simple [[lat, lon], ...]
    if isinstance(data, list):
        coords = []
        for pt in data:
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                coords.append((float(pt[0]), float(pt[1])))
            elif isinstance(pt, dict):
                lat = float(pt.get("lat") or pt.get("latitude"))
                lon = float(pt.get("lon") or pt.get("longitude") or pt.get("lng"))
                coords.append((lat, lon))
        if len(coords) > 1:
            return coords

    return None

def cargar_rutas_csv(ruta_archivo, contenedor_target):
    rutas = []
    if not os.path.exists(ruta_archivo):
        print(f"ERROR: No se encuentra '{ruta_archivo}'. Usando ruta de respaldo.")
        return [[(40.4167, -3.7037), (40.4200, -3.7000)]]
    
    with open(ruta_archivo, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            contenedor_fila = str(row.get("Contenedor", "")).strip()
            if contenedor_fila == contenedor_target:
                itinerary_str = row.get("itinerary", "")
                if not itinerary_str:
                    continue
                coords = extraer_coordenadas_itinerario(itinerary_str)
                if coords:
                    rutas.append(coords)
                    
    if not rutas:
        print(f"Sin rutas para {contenedor_target}. Usando ruta de respaldo.")
        rutas.append([(40.4167, -3.7037), (40.4200, -3.7000)])
    
    print(f"Cargadas {len(rutas)} rutas para {contenedor_target}.")
    return rutas

async def run():
    rutas_disponibles = cargar_rutas_csv(ROUTE_FILE, CONTAINER_ID)
    num_rutas = len(rutas_disponibles)
    vehiculos_estado = []
    lista_ids = [f"VEH-{i}" for i in range(VEHICLE_START_ID, VEHICLE_END_ID + 1)]
    
    for idx, veh_id in enumerate(lista_ids):
        ruta_asignada = rutas_disponibles[idx % num_rutas]
        total_puntos = len(ruta_asignada)
        start_offset = (idx * (total_puntos // max(1, len(lista_ids)))) % total_puntos
        vehiculos_estado.append({
            "VehicleID": veh_id,
            "coords": ruta_asignada,
            "current_idx": start_offset,
            "direction": 1,
            "fuel_level": round(random.uniform(50.0, 100.0), 1)
        })
        
    # Inicialización del cliente Azure EventHub: Si la cadena incluye EntityPath, no se duplica eventhub_name
    if "EntityPath=" in CONNECTION_STR:
        producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR)
    else:
        producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)
    
    async with producer:
        print(f"{CONTAINER_ID} iniciado: Transmitiendo Vehículos {VEHICLE_START_ID} a {VEHICLE_END_ID}...")
        
        while True:
            event_batch = await producer.create_batch()
            
            for veh in vehiculos_estado:
                coords = veh["coords"]
                idx = veh["current_idx"]
                lat, lon = coords[idx]
                
                payload = {
                    "VehicleID": veh["VehicleID"],
                    "Timestamp": datetime.now(timezone.utc).isoformat(),
                    "Latitude": round(lat, 6),
                    "Longitude": round(lon, 6),
                    "Speed": random.randint(15, 50),
                    "EngineRPM": random.randint(1200, 2800),
                    "FuelLevel": veh["fuel_level"],
                    "DTC_Code": random.choices([None, "P0171", "P0300", "P0420"], weights=[0.9, 0.04, 0.03, 0.03], k=1)[0]
                }
                
                # Consumo progresivo de combustible
                veh["fuel_level"] = max(5.0, round(veh["fuel_level"] - 0.01, 2))
                next_idx = idx + veh["direction"]
                
                # Inversión de marcha al alcanzar la cabecera final de la línea
                if next_idx >= len(coords):
                    veh["direction"] = -1
                    next_idx = len(coords) - 2 if len(coords) > 1 else 0
                elif next_idx < 0:
                    veh["direction"] = 1
                    next_idx = 1 if len(coords) > 1 else 0
                    
                veh["current_idx"] = next_idx
                event_batch.add(EventData(json.dumps(payload)))
                
            await producer.send_batch(event_batch)
            print(f"{CONTAINER_ID}: Transmitido lote de {len(vehiculos_estado)} eventos.")
            await asyncio.sleep(2)

if __name__ == "__main__":
    if not CONNECTION_STR:
        print("ERROR: Variable EVENTSTREAM_CONNECTION_STRING requerida en .env.")
        exit(1)
    asyncio.run(run())
```

---

## 4. Orquestación Distribuida (`Docker/docker-compose.yml` y `Dockerfile`)

### Arquitectura de Contenedores:
Se instancian 3 réplicas que se reparten la flota de 45 vehículos (`VEH-1001` a `VEH-1045`):

```yaml
version: '3.8'

services:
  simulator-1:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: simulator-1
    restart: unless-stopped
    environment:
      - PYTHONUNBUFFERED=1
      - CONTAINER_ID=Contenedor_1
      - VEHICLE_START_ID=1001
      - VEHICLE_END_ID=1015
      - ROUTE_FILE=/app/topologia_rutas_flota_nrt.csv
      - EVENTSTREAM_CONNECTION_STRING=${EVENTSTREAM_CONNECTION_STRING}
      - EVENTSTREAM_NAME=${EVENTSTREAM_NAME}
    volumes:
      - ../data/topologia_rutas_flota_nrt.csv:/app/topologia_rutas_flota_nrt.csv:ro

  simulator-2:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: simulator-2
    restart: unless-stopped
    environment:
      - PYTHONUNBUFFERED=1
      - CONTAINER_ID=Contenedor_2
      - VEHICLE_START_ID=1016
      - VEHICLE_END_ID=1030
      - ROUTE_FILE=/app/topologia_rutas_flota_nrt.csv
      - EVENTSTREAM_CONNECTION_STRING=${EVENTSTREAM_CONNECTION_STRING}
      - EVENTSTREAM_NAME=${EVENTSTREAM_NAME}
    volumes:
      - ../data/topologia_rutas_flota_nrt.csv:/app/topologia_rutas_flota_nrt.csv:ro

  simulator-3:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: simulator-3
    restart: unless-stopped
    environment:
      - PYTHONUNBUFFERED=1
      - CONTAINER_ID=Contenedor_3
      - VEHICLE_START_ID=1031
      - VEHICLE_END_ID=1045
      - ROUTE_FILE=/app/topologia_rutas_flota_nrt.csv
      - EVENTSTREAM_CONNECTION_STRING=${EVENTSTREAM_CONNECTION_STRING}
      - EVENTSTREAM_NAME=${EVENTSTREAM_NAME}
    volumes:
      - ../data/topologia_rutas_flota_nrt.csv:/app/topologia_rutas_flota_nrt.csv:ro
```

### Especificación del `Docker/Dockerfile`:
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator.py .

CMD ["python", "simulator.py"]
```

### Claves de Optimización en el Despliegue:
* **`PYTHONUNBUFFERED=1`**: Desactiva el búfer de salida estándar en Python dentro de los contenedores Docker, permitiendo la visualización inmediata de los logs de emisión con `docker compose logs -f`.
* **Volúmenes de Solo Lectura (`:ro`)**: Protege el archivo maestro `topologia_rutas_flota_nrt.csv` frente a modificaciones accidentales desde el runtime del contenedor.
* **Resiliencia Automática (`restart: unless-stopped`)**: Garantiza la recuperación de la transmisión ante pérdidas temporales de conectividad de red con el endpoint de Azure Event Hubs en Fabric.
