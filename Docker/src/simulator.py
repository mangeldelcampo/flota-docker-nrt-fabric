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

# 1. Aumentar el límite de tamaño de celda para soportar los GeoJSON largos de itinerary
csv.field_size_limit(sys.maxsize)

# 2. Variables de entorno inyectadas por Docker Compose
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

    # Caso 1: Estructura de la EMT con FeatureCollection en 'toA'
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
                        coords.append((float(pt[1]), float(pt[0]))) # (Lat, Lon)
                elif gtype == "MultiLineString":
                    for linea in gcoords:
                        for pt in linea:
                            coords.append((float(pt[1]), float(pt[0])))
            if len(coords) > 1:
                return coords

    # Caso 2: Lista directa de puntos [[lat, lon], ...]
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
                else:
                    print(f"Aviso: No se pudieron extraer coordenadas válidas para una ruta de {contenedor_target}.")
                    
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
        
    # Inicialización del cliente: Si CONNECTION_STR ya incluye EntityPath, no se duplica eventhub_name
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
                    "Timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    "Latitude": round(lat, 6),
                    "Longitude": round(lon, 6),
                    "Speed": random.randint(15, 50),
                    "EngineRPM": random.randint(1200, 2800),
                    "FuelLevel": veh["fuel_level"],
                    "DTC_Code": random.choices([None, "P0171", "P0300", "P0420"], weights=[0.9, 0.04, 0.03, 0.03], k=1)[0]
                }
                
                veh["fuel_level"] = max(5.0, round(veh["fuel_level"] - 0.01, 2))
                next_idx = idx + veh["direction"]
                
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
            await asyncio.sleep(10) # Emitir cada 10 segundos

if __name__ == "__main__":
    if not CONNECTION_STR:
        print("ERROR: Variable EVENTSTREAM_CONNECTION_STRING requerida.")
        exit(1)
    asyncio.run(run())
