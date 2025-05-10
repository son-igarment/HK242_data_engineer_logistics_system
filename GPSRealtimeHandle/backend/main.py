from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pika
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Dict, Optional

load_dotenv()

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load route data
with open('../vietmap_route_data.json', 'r') as f:
    route_data = json.load(f)

# Mock database
vehicles: Dict[str, Dict] = {}
vehicle_routes: Dict[str, List[Dict]] = {}

# Initialize routes for vehicles
for route in route_data['processed_routes']:
    start_lat = route['route_info']['start']['lat']
    start_lon = route['route_info']['start']['lng']
    end_lat = route['route_info']['end']['lat']
    end_lon = route['route_info']['end']['lng']
    
    # Find matching vehicle from data.json
    with open('../data.json', 'r') as f:
        vehicle_data = json.load(f)
        vehicle = next((v for v in vehicle_data 
                       if v['start_lat'] == start_lat 
                       and v['start_lon'] == start_lon 
                       and v['end_lat'] == end_lat 
                       and v['end_lon'] == end_lon), None)
        
        if vehicle:
            vehicle_routes[vehicle['vehicle_id']] = route['points']

class VehicleUpdate(BaseModel):
    vehicle_id: str
    latitude: float
    longitude: float
    speed: float
    heading: float
    odometer: float
    timestamp: str

def is_vehicle_on_route(vehicle: Dict, route: List[Dict]) -> bool:
    if not route:
        return False
        
    # Find the closest point in the route
    min_distance = float('inf')
    for point in route:
        lat_diff = abs(vehicle["latitude"] - point["latitude"])
        lon_diff = abs(vehicle["longitude"] - point["longitude"])
        distance = (lat_diff ** 2 + lon_diff ** 2) ** 0.5
        
        if distance < min_distance:
            min_distance = distance
    
    # Consider vehicle on route if within 100 meters
    return min_distance < 0.001  # Approximately 100 meters

def process_vehicle_update(ch, method, properties, body):
    try:
        update = json.loads(body)
        vehicle_id = update["vehicle_id"]
        
        # Store vehicle data
        vehicles[vehicle_id] = update
        
        # Check if vehicle is on route
        if vehicle_id in vehicle_routes:
            route = vehicle_routes[vehicle_id]
            if not is_vehicle_on_route(update, route):
                # Notify frontend (in a real implementation, this would use WebSocket)
                print(f"Vehicle {vehicle_id} is off route!")
                
    except Exception as e:
        print(f"Error processing update: {e}")

def start_rabbitmq_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv('RABBITMQ_HOST', 'localhost'),
            port=int(os.getenv('RABBITMQ_PORT', 5672)),
            credentials=pika.PlainCredentials(
                os.getenv('RABBITMQ_USER', 'user'),
                os.getenv('RABBITMQ_PASSWORD', 'password')
            )
        )
    )
    channel = connection.channel()
    
    channel.queue_declare(queue='backend_queue')
    channel.basic_consume(
        queue='backend_queue',
        on_message_callback=process_vehicle_update,
        auto_ack=True
    )
    
    channel.start_consuming()

@app.get("/vehicles")
async def get_vehicles():
    # Ensure 'vehicles' is the global mock dictionary
    vehicle_list = list(vehicles.values()) # Get the list of vehicle data

    # Calculate counts based on the 'is_off_route' flag IN the data
    vehicles_off_route_count = sum(1 for v in vehicle_list if v.get("is_off_route", False)) # Default to False if flag missing
    vehicles_on_route_count = len(vehicle_list) - vehicles_off_route_count

    return {
        "total_vehicles": len(vehicle_list),
        "vehicles_on_route": vehicles_on_route_count,  # Use count based on existing flag
        "vehicles_off_route": vehicles_off_route_count, # Use count based on existing flag
        "vehicles": vehicle_list
    }

@app.get("/vehicles/{vehicle_id}")
async def get_vehicle(vehicle_id: str):
    if vehicle_id not in vehicles:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    return vehicles[vehicle_id]

@app.get("/routes/{vehicle_id}")
async def get_vehicle_route(vehicle_id: str):
    if vehicle_id not in vehicle_routes:
        raise HTTPException(status_code=404, detail="Route not found for vehicle")
    return vehicle_routes[vehicle_id]

# Phase 21: Endpoint to serve warehouse data
@app.get("/warehouses")
async def get_warehouses():
    warehouse_data = []
    try:
        # Construct path relative to the backend script location
        script_dir = os.path.dirname(__file__)
        # Assuming genesisData is two levels up from backend/main.py
        # Adjust relative path if needed: ../../genesisData/datawarehouse.json
        file_path = os.path.abspath(os.path.join(script_dir, "../../genesisData/datawarehouse.json"))
        print(f"[API /warehouses] Attempting to load from: {file_path}")
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                warehouse_data = json.load(f)
            print(f"[API /warehouses] Successfully loaded {len(warehouse_data)} warehouses.")
            return warehouse_data
        else:
             print(f"[API /warehouses] Error: File not found at {file_path}")
             raise HTTPException(status_code=404, detail=f"Warehouse data file not found.")
    except json.JSONDecodeError as e:
        print(f"[API /warehouses] Error decoding JSON: {e}")
        raise HTTPException(status_code=500, detail="Error reading warehouse data file")
    except Exception as e:
        print(f"[API /warehouses] Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error loading warehouse data")

if __name__ == "__main__":
    import uvicorn
    import threading
    


    # Start RabbitMQ consumer in a separate thread
    rabbitmq_thread = threading.Thread(target=start_rabbitmq_consumer)
    rabbitmq_thread.daemon = True
    rabbitmq_thread.start()

    
    uvicorn.run(app, host="0.0.0.0", port=8000) 