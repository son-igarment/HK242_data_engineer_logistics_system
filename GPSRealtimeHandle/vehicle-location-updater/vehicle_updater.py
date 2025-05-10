import pika
import json
import time
import random
from datetime import datetime
import os
from dotenv import load_dotenv
import math

load_dotenv()

# Load mock data
with open('../data.json', 'r') as f:
    vehicle_data = json.load(f)

with open('../vietmap_route_data.json', 'r') as f:
    route_data = json.load(f)

# Initialize vehicle positions
vehicles = []
for vehicle in vehicle_data:
    # Find matching route
    route = next((r for r in route_data['processed_routes'] 
                 if r['route_info']['start']['lat'] == vehicle['start_lat'] 
                 and r['route_info']['start']['lng'] == vehicle['start_lon'] 
                 and r['route_info']['end']['lat'] == vehicle['end_lat'] 
                 and r['route_info']['end']['lng'] == vehicle['end_lon']), None)
    
    if route:
        vehicles.append({
            "vehicle_id": vehicle['vehicle_id'],
            "route": route['points'],
            "current_point_index": 0,
            "latitude": route['points'][0]['latitude'],
            "longitude": route['points'][0]['longitude'],
            "speed": random.randint(40, 80),
            "heading": 0,
            "odometer": 0,
            "is_off_route": False
        })

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in kilometers"""
    R = 6371  # Earth's radius in kilometers
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def get_random_point_nearby(lat, lon, max_distance=50):
    """Generate a random point within specified distance"""
    R = 6371  # Earth's radius in kilometers
    bearing = random.uniform(0, 360)
    distance = max_distance / R  # Convert to radians
    
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    
    lat2 = math.asin(math.sin(lat1) * math.cos(distance) + 
                    math.cos(lat1) * math.sin(distance) * math.cos(math.radians(bearing)))
    
    lon2 = lon1 + math.atan2(math.sin(math.radians(bearing)) * math.sin(distance) * math.cos(lat1),
                            math.cos(distance) - math.sin(lat1) * math.sin(lat2))
    
    return math.degrees(lat2), math.degrees(lon2)

def update_vehicle_location(vehicle):
    # For the first vehicle (index 0), simulate 15% chance of going off route
    if vehicle["vehicle_id"] == vehicles[0]["vehicle_id"] and random.random() < 0.60:
        # Generate a point 50 meters away from the current position
        new_lat, new_lon = get_random_point_nearby(
            vehicle["latitude"], 
            vehicle["longitude"], 
            0.005  # 50 meters
        )
        vehicle["latitude"] = new_lat
        vehicle["longitude"] = new_lon
        vehicle["is_off_route"] = True
    else:
        # Normal route following
        if vehicle['current_point_index'] < len(vehicle['route']) - 1:
            vehicle['current_point_index'] += 1
            next_point = vehicle['route'][vehicle['current_point_index']]
            
            # Update vehicle data
            vehicle['latitude'] = next_point['latitude']
            vehicle['longitude'] = next_point['longitude']
            vehicle['is_off_route'] = False
        else:
            # Reset to start of route
            vehicle['current_point_index'] = 0
            start_point = vehicle['route'][0]
            vehicle['latitude'] = start_point['latitude']
            vehicle['longitude'] = start_point['longitude']
            vehicle['is_off_route'] = False
    
    # Update other vehicle data
    vehicle['speed'] = random.randint(40, 80)
    vehicle['heading'] = random.randint(0, 360)
    vehicle['odometer'] += random.randint(1, 5)
    
    return vehicle

def main():
    # RabbitMQ connection
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

    channel = connection.channel()

    # Declare exchange
    channel.exchange_declare(exchange='vehicle_updates', exchange_type='fanout')

    # Declare queue for backend
    channel.queue_declare(queue='backend_queue')
    channel.queue_bind(exchange='vehicle_updates', queue='backend_queue')


    # Declare exchange
    channel.exchange_declare(exchange='vehicle_updates', exchange_type='fanout')

    try:
        while True:
            for vehicle in vehicles:
                # Update vehicle location
                updated_vehicle = update_vehicle_location(vehicle)
                
                # Add timestamp
                updated_vehicle["timestamp"] = datetime.now().isoformat()
                
                # Remove route data before publishing
                vehicle_update = {
                    "vehicle_id": updated_vehicle["vehicle_id"],
                    "latitude": updated_vehicle["latitude"],
                    "longitude": updated_vehicle["longitude"],
                    "speed": updated_vehicle["speed"],
                    "heading": updated_vehicle["heading"],
                    "odometer": updated_vehicle["odometer"],
                    "timestamp": updated_vehicle["timestamp"],
                    "is_off_route": updated_vehicle["is_off_route"]
                }
                
                # Publish to RabbitMQ
                channel.basic_publish(
                    exchange='vehicle_updates',
                    routing_key='',
                    body=json.dumps(vehicle_update)
                )
                print(f"Published update for vehicle {vehicle['vehicle_id']} - {'OFF ROUTE' if vehicle['is_off_route'] else 'ON ROUTE'}")
            
            time.sleep(5)  # Wait for 10 seconds before next update

    except KeyboardInterrupt:
        print("Stopping vehicle location updater...")
    finally:
        connection.close()

if __name__ == "__main__":
    main() 