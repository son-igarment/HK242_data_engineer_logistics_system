import time
import json
import random
import pika
import redis
import math
from datetime import datetime, timezone
import uuid # Use UUID for better ID generation
import requests # Phase 16 Requirement (moved here)

# Assuming config is importable from parent directory
import config

# Mock data storage (in-memory for simplicity in this initial version)
# In a real dev setup, this might load from files or a dev db
warehouses_data = []
vehicles_data = []

# Redis connection
redis_client = None

def get_redis_connection():
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB,
                decode_responses=True # Decode responses to strings automatically
            )
            redis_client.ping() # Check connection
            print("Connected to Redis.")
        except redis.exceptions.ConnectionError as e:
            print(f"Error connecting to Redis: {e}")
            redis_client = None # Reset client if connection fails
        except Exception as e: # Catch other potential errors like config issues
            print(f"An unexpected error occurred connecting to Redis: {e}")
            redis_client = None
    return redis_client

# --- New Redis Storage Functions --- 

def store_data_in_redis(redis_conn, key, data):
    """Generic function to store dictionary data as JSON in Redis."""
    try:
        redis_conn.set(key, json.dumps(data))
        # Consider adding TTL (expiration) for some keys if appropriate
        # redis_conn.expire(key, 3600 * 24) # Example: expire in 1 day
        print(f"Stored key '{key}' in Redis.")
        return True
    except redis.exceptions.RedisError as e:
        print(f"Redis Error storing key '{key}': {e}")
    except Exception as e:
        print(f"Error storing key '{key}' in Redis: {e}")
    return False

def store_trip_order_redis(redis_conn, trip_data):
    trip_id = trip_data.get('trip_order_id')
    if not trip_id:
        print("Error: Cannot store trip order without trip_order_id.")
        return False
    key = f"trip:{trip_id}"
    # Changing back to use HMSET to store as Hash
    try:
        mapping_to_store = {}
        for k, v in trip_data.items():
            if isinstance(v, (str, int, float, bytes)):
                mapping_to_store[k] = v
            elif v is None:
                mapping_to_store[k] = "" # Store None as empty string
            else:
                try:
                    mapping_to_store[k] = json.dumps(v)
                except TypeError:
                    print(f"Warning: Could not JSON serialize value for key '{k}' in trip {trip_id}. Storing as string representation.")
                    mapping_to_store[k] = str(v)

        # Use HMSET (or HSET with mapping in newer redis-py)
        redis_conn.hmset(key, mapping_to_store)
        print(f"Stored TripOrder {trip_id} as Hash in Redis key '{key}'.")
        return True
    except redis.exceptions.RedisError as e:
        print(f"Redis Error storing TripOrder hash '{key}': {e}")
    except Exception as e:
        print(f"Error storing TripOrder hash '{key}': {e}")
    return False

def store_order_batch_redis(redis_conn, batch_data):
    batch_id = batch_data.get('batch_id')
    if not batch_id:
        print("Error: Cannot store order batch without batch_id.")
        return False
    key = f"batch:{batch_id}"
    # Store the main batch data
    if not store_data_in_redis(redis_conn, key, batch_data):
        return False
    # Additionally, add batch_id to a set associated with the trip_order for easy lookup
    trip_id = batch_data.get('trip_order_id')
    if trip_id:
        try:
            trip_batches_key = f"trip:{trip_id}:batches"
            redis_conn.sadd(trip_batches_key, batch_id)
        except redis.exceptions.RedisError as e:
             print(f"Redis Error adding batch {batch_id} to set {trip_batches_key}: {e}")
        # Ignore failure to add to set, main data is stored.
    return True

def store_batch_item_redis(redis_conn, item_data):
    item_id = item_data.get('item_id')
    batch_id = item_data.get('batch_id')
    if not item_id:
        print("Error: Cannot store batch item without item_id.")
        return False
    key = f"batch_item:{item_id}"
    # Store the main item data
    if not store_data_in_redis(redis_conn, key, item_data):
        return False
    # Additionally, add item_id to a set associated with the batch for easy lookup
    if batch_id:
        try:
            batch_items_key = f"batch:{batch_id}:items"
            redis_conn.sadd(batch_items_key, item_id)
        except redis.exceptions.RedisError as e:
            print(f"Redis Error adding item {item_id} to set {batch_items_key}: {e}")
        # Ignore failure to add to set, main data is stored.
    return True

# --- End New Redis Storage Functions ---

# --- Phase 16: Vietmap API Route Fetching (Copied from vehicle_management) ---
def get_vietmap_route(source_lat, source_lon, dest_lat, dest_lon, vehicle_type="car"):
    """Fetches a route polyline from the Vietmap Routing API."""
    api_key = config.VIETMAP_API_KEY
    base_url = 'https://maps.vietmap.vn/api/route'

    if not api_key:
        print("[Vietmap API] Error: VIETMAP_API_KEY is not configured.")
        return None
    if not base_url:
        # Fallback URL if not set in config
        base_url = 'https://maps.vietmap.vn/api/route' 
        print(f"[Vietmap API] Warning: VIETMAP_ROUTE_URL not configured, using default: {base_url}")
    
    # Validate coords (basic check)
    try:
        # Ensure they are float or can be converted
        f_source_lat = float(source_lat)
        f_source_lon = float(source_lon)
        f_dest_lat = float(dest_lat)
        f_dest_lon = float(dest_lon)
    except (ValueError, TypeError):
        print(f"[Vietmap API] Error: Invalid coordinates provided - Source:({source_lat},{source_lon}), Dest:({dest_lat},{dest_lon})")
        return None

    # Determine vehicle type parameter for API
    api_vehicle = "car" # Default
    if vehicle_type.lower() == "heavy truck": # Assuming trip_type maps directly
        api_vehicle = "truck"
        
    params = {
        'api-version': '1.1',
        'apikey': api_key,
        'point': [f"{f_source_lat},{f_source_lon}", f"{f_dest_lat},{f_dest_lon}"],
        'vehicle': api_vehicle,
        'points_encoded': 'true'
    }

    print(f"[Vietmap API] Requesting route from {f_source_lat},{f_source_lon} to {f_dest_lat},{f_dest_lon} for vehicle type: {api_vehicle}")

    try:
        response = requests.get(base_url, params=params, timeout=15)
        response.raise_for_status()
        route_data = response.json()

        if route_data and "paths" in route_data and len(route_data["paths"]) > 0 and "points" in route_data["paths"][0]:
            polyline = route_data["paths"][0]["points"]
            distance_meters = route_data["paths"][0].get("distance", 0)
            time_seconds = route_data["paths"][0].get("time", 0)
            print(f"[Vietmap API] Received route. Polyline length: {len(polyline)}, Dist: {distance_meters/1000:.2f}km, Time: {time_seconds/60:.1f}min")
            return polyline
        else:
            print("[Vietmap API] Error: Unexpected response structure or missing polyline.")
            return None
    except requests.exceptions.Timeout:
        print("[Vietmap API] Error: Request timed out.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[Vietmap API] Error: Request failed: {e}")
        return None
    except json.JSONDecodeError:
        print(f"[Vietmap API] Error: Failed to decode JSON response.")
        print(f"[Vietmap API] Response Text: {response.text[:200]}...")
        return None
    except Exception as e:
        print(f"[Vietmap API] Error: Unexpected error during API call: {e}")
        return None
# --- End Phase 16 --- 

def load_mock_data():
    global warehouses_data, vehicles_data
    print("Loading mock data...")
    try:
        # Adjust path if genesisData is one level up
        with open('../genesisData/datawarehouse.json', 'r') as f: 
            warehouses_data = json.load(f)
            print(f"Loaded {len(warehouses_data)} warehouses.")
            r = get_redis_connection()
            if r:
                for wh in warehouses_data:
                    # Add initial capacity to Redis if not present in mock file
                    if 'capacity' not in wh:
                        wh['capacity'] = {'weight_kg': 1000000, 'volume_m3': 10000} # Default large capacity
                        print(f"Added default capacity to warehouse {wh['warehouse_id']} in memory.")

                    wh_key = f"warehouse:{wh['warehouse_id']}:status"
                    if not r.exists(wh_key):
                        # Initialize with capacity from data
                        initial_status = {
                            'current_weight_kg': 0,
                            'current_volume_m3': 0,
                            'max_weight_kg': wh['capacity'].get('weight_kg', 1000000),
                            'max_volume_m3': wh['capacity'].get('volume_m3', 10000),
                            'last_updated': time.time()
                        }
                        r.hset(wh_key, mapping=initial_status)
                        print(f"Initialized status for warehouse {wh['warehouse_id']} in Redis.")
    except FileNotFoundError:
        print("Warning: ../genesisData/datawarehouse.json not found. Cannot load warehouse data.")
    except Exception as e:
        print(f"Error loading datawarehouse.json: {e}")

    try:
        # Adjust path if genesisData is one level up
        with open('../genesisData/vehicle.json', 'r') as f: 
            vehicles_data = json.load(f)
            print(f"Loaded {len(vehicles_data)} vehicle types.")
    except FileNotFoundError:
        print("Warning: ../genesisData/vehicle.json not found. Cannot load vehicle data.")
    except Exception as e:
        print(f"Error loading vehicle.json: {e}")

def get_rabbitmq_connection():
    credentials = None
    if config.RABBITMQ_USER and config.RABBITMQ_PASS:
        credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    
    parameters = pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=credentials
    )
    return pika.BlockingConnection(parameters)

def declare_rabbitmq_topology(channel):
    # Declare exchange where orders are published
    channel.exchange_declare(exchange=config.ORDER_EXCHANGE, exchange_type='direct', durable=True)
    # Declare queue to consume orders from
    channel.queue_declare(queue=config.ORDER_QUEUE, durable=True)
    # Bind the queue
    channel.queue_bind(queue=config.ORDER_QUEUE, exchange=config.ORDER_EXCHANGE, routing_key=config.ORDER_ROUTING_KEY)

    # Declare exchange for publishing trip updates
    channel.exchange_declare(exchange=config.TRIP_UPDATE_EXCHANGE, exchange_type='topic', durable=True)
    # We don't necessarily need to declare/bind the trip update queue here,
    # as this service only publishes to it. Other services would consume.
    # However, declaring it can be useful for ensuring it exists.
    channel.queue_declare(queue=config.TRIP_UPDATE_QUEUE, durable=True)
    channel.queue_bind(queue=config.TRIP_UPDATE_QUEUE, exchange=config.TRIP_UPDATE_EXCHANGE, routing_key=config.TRIP_UPDATE_ROUTING_KEY)

def calculate_distance(lat1, lon1, lat2, lon2):
    # Simple Haversine distance calculation
    R = 6371 # Radius of Earth in kilometers
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def find_nearest_warehouse(lat, lon):
    if not warehouses_data:
        return None

    # Convert lat/lon in warehouses_data to float for calculation
    # Access latitude/longitude directly from the warehouse object
    nearest_wh = min(warehouses_data, key=lambda wh: calculate_distance(
        lat, lon, float(wh['latitude']), float(wh['longitude']) # Access directly and convert to float
    ))
    return nearest_wh

def check_warehouse_capacity(warehouse_id, order_weight, order_volume):
    # Now reads capacity limits directly from Redis status hash
    r = get_redis_connection()
    if not r:
        print("Cannot check capacity: Redis unavailable.")
        return False # Default to false if no redis

    wh_key = f"warehouse:{warehouse_id}:status"
    if not r.exists(wh_key):
         # This shouldn't happen if load_mock_data ran successfully
         print(f"Warning: Status for Warehouse {warehouse_id} not found in Redis. Assuming capacity.")
         return True

    current_status = r.hgetall(wh_key)
    try:
        current_weight = float(current_status.get('current_weight_kg', 0))
        current_volume = float(current_status.get('current_volume_m3', 0))
        max_weight = float(current_status.get('max_weight_kg', float('inf')))
        max_volume = float(current_status.get('max_volume_m3', float('inf')))
    except (ValueError, TypeError) as e:
        print(f"Warning: Could not parse numeric capacity/status for Warehouse {warehouse_id} from Redis: {e}. Assuming capacity.")
        return True

    # Check if adding the order exceeds capacity
    if (current_weight + order_weight <= max_weight and
            current_volume + order_volume <= max_volume):
        return True
    else:
        print(f"Warehouse {warehouse_id} capacity exceeded (Current: {current_weight:.2f}/{max_weight:.2f}kg, {current_volume:.2f}/{max_volume:.2f}m3). Waiting...")
        return False

def update_warehouse_status(redis_conn, warehouse_id, weight_change, volume_change):
    wh_key = f"warehouse:{warehouse_id}:status"
    try:
        # Use pipeline for atomic update
        pipe = redis_conn.pipeline()
        pipe.hincrbyfloat(wh_key, 'current_weight_kg', weight_change)
        pipe.hincrbyfloat(wh_key, 'current_volume_m3', volume_change)
        pipe.hset(wh_key, 'last_updated', time.time())
        pipe.execute()
        print(f"Updated warehouse {warehouse_id} status: weight +{weight_change}, volume +{volume_change}")
    except Exception as e:
        print(f"Error updating Redis for warehouse {warehouse_id}: {e}")

def process_order(channel, method, properties, body):
    print(f"Received order message: {method.delivery_tag}")
    order_data = None
    try:
        order_data = json.loads(body.decode('utf-8'))
        print(f"Processing order: {order_data['order_code']}")

        r = get_redis_connection()
        if not r:
            print("Redis unavailable. Requeuing message.")
            time.sleep(5) # Delay before requeue
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        # --- Phase 4 Logic Start --- 

        # 1. Dispatch: Find nearest warehouses (as before)
        start_warehouse = find_nearest_warehouse(
            order_data['start_location_latitude'],
            order_data['start_location_longitude']
        )
        end_warehouse = find_nearest_warehouse(
            order_data['end_location_latitude'],
            order_data['end_location_longitude']
        )

        if not start_warehouse:
            print(f"Could not find nearest start warehouse for order {order_data['order_code']}. Acknowledging and dropping.")
            channel.basic_ack(delivery_tag=method.delivery_tag) 
            return

        print(f"Order {order_data['order_code']}: Start WH {start_warehouse['warehouse_id']}, End WH {end_warehouse['warehouse_id'] if end_warehouse else 'N/A'}")

        # 2. Operation: Check capacity and create Trip/Batch/Items
        # Use start warehouse ID as the initial location for the batch
        origin_warehouse_id = start_warehouse['warehouse_id'] 
        order_weight = order_data['weight_kg']
        order_volume = order_data['volume_m3']
        current_time_iso = datetime.now(timezone.utc).isoformat()

        if check_warehouse_capacity(origin_warehouse_id, order_weight, order_volume):
            print(f"Warehouse {origin_warehouse_id} has capacity for order {order_data['order_code']}. Creating Trip and Batch.")
            
            # Update warehouse status in Redis (before creating trip)
            update_warehouse_status(r, origin_warehouse_id, order_weight, order_volume)

            # Generate IDs
            trip_order_id = f"trip_{uuid.uuid4()}"
            batch_id = f"batch_{trip_order_id}"
            item_id = f"item_{order_data['order_code']}_{uuid.uuid4().hex[:6]}" 

            # Determine trip type 
            trip_type = "Local Transfer" # Default
            if start_warehouse and end_warehouse:
                if start_warehouse['warehouse_id'] == end_warehouse['warehouse_id']:
                    trip_type = "Pickup/Delivery"
                else:
                    trip_type = "Inter-Warehouse Transfer"
            elif start_warehouse:
                 trip_type = "Pickup" 

            # --- Phase 16: Get Route from Vietmap API ---
            planned_route_polyline = None
            try:
                # Use coordinates from the order data
                src_lat = float(order_data['start_location_latitude'])
                src_lon = float(order_data['start_location_longitude'])
                dest_lat = float(order_data['end_location_latitude'])
                dest_lon = float(order_data['end_location_longitude'])
                
                # Determine vehicle type for API (example: could be based on weight/volume later)
                # For now, map trip_type loosely or default to car
                api_vehicle = "car"
                if trip_type == "Inter-Warehouse Transfer": # Example condition
                    # Heavier load might imply truck, adjust as needed
                    api_vehicle = "truck" 
                
                planned_route_polyline = get_vietmap_route(src_lat, src_lon, dest_lat, dest_lon, api_vehicle)
                if not planned_route_polyline:
                     print(f"Warning: Failed to get route from Vietmap API for trip {trip_order_id}. planned_route will be empty.")
                     planned_route_polyline = "" # Store empty string if API fails
            except Exception as route_err:
                print(f"Error getting route for trip {trip_order_id}: {route_err}")
                planned_route_polyline = "" # Store empty string on error
            # --- End Phase 16 --- 
            print("LIfebow log --------------------")
            print(planned_route_polyline)
            # Create TripOrder data structure
            trip_order_data = {
                "trip_order_id": trip_order_id,
                "batch_id": batch_id,
                "source_address": order_data['start_address'],
                "source_latitude": order_data['start_location_latitude'],
                "source_longitude": order_data['start_location_longitude'],
                "destination_address": order_data['end_address'],
                "destination_latitude": order_data['end_location_latitude'],
                "destination_longitude": order_data['end_location_longitude'],
                "trip_type": trip_type,
                "status": "Pending Assignment", 
                "vehicle_id": None,
                "driver_id": None,
                "estimated_duration_minutes": None, # TODO: Calculate based on distance/traffic?
                "actual_duration_minutes": None,
                "planned_route": planned_route_polyline,
                "arrive_time_to_source": None,
                "wait_time_at_source": 0,
                "created_at": current_time_iso,
                "updated_at": current_time_iso
            }

            # Create OrderBatches data structure
            order_batch_data = {
                "batch_id": batch_id,
                "trip_order_id": trip_order_id,
                "status": "Open", # Initial status: Open, waiting for warehouse
                "location_type": "Warehouse", # Initial location type
                "location_id": origin_warehouse_id, # Initial location ID
                "created_at": current_time_iso,
                "updated_at": current_time_iso
            }

            # Create OrderBatchItems data structure
            batch_item_data = {
                "item_id": item_id,
                "batch_id": batch_id,
                "order_code": order_data['order_code'],
                "status": "Pending Pickup",
                "sequence": 1, 
                "created_at": current_time_iso,
                "updated_at": current_time_iso
            }

            # Store all structures in Redis
            stored_trip = store_trip_order_redis(r, trip_order_data)
            stored_batch = store_order_batch_redis(r, order_batch_data)
            stored_item = store_batch_item_redis(r, batch_item_data)

            if not (stored_trip and stored_batch and stored_item):
                print(f"Error storing trip/batch/item data in Redis for order {order_data['order_code']}. Reverting warehouse status update and NACKing.")
                # Attempt to revert warehouse status (best effort)
                update_warehouse_status(r, origin_warehouse_id, -order_weight, -order_volume)
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return
            
            print(f"Created Trip: {trip_order_id}, Batch: {batch_id}, Item: {item_id}")

            # Publish TripOrder Update Notification to RabbitMQ
            try:
                # Publish the full TripOrder data
                message_body = json.dumps(trip_order_data) 
                channel.basic_publish(
                    exchange=config.TRIP_UPDATE_EXCHANGE,
                    routing_key=config.TRIP_UPDATE_ROUTING_KEY,
                    body=message_body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"Published trip update for {trip_order_id}")
            except Exception as pub_err:
                print(f"Error publishing trip update for {trip_order_id}: {pub_err}")
                # Data is stored in Redis, but notification failed.
                # Should we try to compensate? For now, just log and acknowledge the order.
                # Depending on requirements, might need a dead-letter queue or retry mechanism here.

            # Acknowledge the original order message
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"Successfully processed and acknowledged order {order_data['order_code']}")

        else:
            # Warehouse doesn't have capacity
            print(f"Warehouse {origin_warehouse_id} lacks capacity for order {order_data['order_code']}. Requeuing message.")
            time.sleep(5) # Wait a bit before requeueing
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        # --- Phase 4 Logic End --- 

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}. Message body: {body}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except KeyError as e:
        print(f"Missing expected key in order data: {e}. Order data: {order_data}")
        channel.basic_ack(delivery_tag=method.delivery_tag) # Acknowledge invalid message
    except Exception as e:
        print(f"An unexpected error occurred processing message {method.delivery_tag}: {e}")
        try:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as channel_err:
             print(f"Error NACKing message: {channel_err}")

def main():
    print("Starting Dispatch Service (Realtime - Dev Mode - Phase 4 Logic)...")
    load_mock_data() 
    # Connect to Redis (moved after load_mock_data which might initialize status)
    get_redis_connection()

    connection = None
    while True:
        try:
            if connection is None or connection.is_closed:
                print("Connecting to RabbitMQ...")
                connection = get_rabbitmq_connection()
                channel = connection.channel()
                declare_rabbitmq_topology(channel)
                channel.basic_qos(prefetch_count=1) # Process one message at a time
                channel.basic_consume(queue=config.ORDER_QUEUE, on_message_callback=process_order)
                print("Connected to RabbitMQ. Waiting for orders...")
                channel.start_consuming()
        
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection error: {e}. Retrying in 5 seconds...")
            if connection and not connection.is_closed:
                try:
                    connection.close()
                except Exception as close_err:
                    print(f"Error closing connection: {close_err}")
            connection = None
            # Also reset Redis client in case RabbitMQ error is related to network issues
            global redis_client
            redis_client = None 
            time.sleep(5)
        except KeyboardInterrupt:
            print("Interrupted. Closing connection...")
            if connection and not connection.is_closed:
                connection.close()
            break
        except Exception as e:
            print(f"An unexpected error occurred in main loop: {e}")
            # Add more robust error handling/logging if needed
            if connection and not connection.is_closed:
                try:
                    connection.close()
                except Exception as close_err:
                    print(f"Error closing connection after error: {close_err}")
            connection = None
            redis_client = None
            print("Restarting consumer loop after 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    main() 