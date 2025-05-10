import pika
import os
import sys
import time
import json
import threading
import redis
import math # Added for distance calculation
import random # Added for Phase 6 simulation in trip_update_callback
from dotenv import load_dotenv
from datetime import datetime, timezone

# --- Redis Connection Global ---
redis_client = None

# --- Phase 9: Data Loading and Seeding ---
WAREHOUSE_KEY_PREFIX = "warehouse:"

def load_warehouse_data_from_json(filepath="../genesisData/datawarehouse.json"):
    """Loads warehouse data from the specified JSON file."""
    script_dir = os.path.dirname(__file__)
    base_dir = os.path.dirname(script_dir)
    abs_filepath = os.path.abspath(os.path.join(base_dir, "genesisData/datawarehouse.json"))
    print(f"[Data Load] Attempting to load warehouse data from: {abs_filepath}")
    try:
        with open(abs_filepath, 'r') as f:
            data = json.load(f)
            print(f"[Data Load] Successfully loaded {len(data)} warehouse records from JSON.")
            return data
    except FileNotFoundError:
        print(f"[Data Load] Error: Warehouse data file not found at {abs_filepath}")
        return None
    except json.JSONDecodeError as e:
        print(f"[Data Load] Error: Failed to decode JSON from {abs_filepath}: {e}")
        return None
    except Exception as e:
        print(f"[Data Load] Error: An unexpected error occurred loading {abs_filepath}: {e}")
        return None

def seed_warehouses_redis(r, warehouse_data):
    """Stores warehouse data into Redis hashes."""
    if not r or not warehouse_data:
        print("[Redis Seed] Cannot seed warehouses: Redis client or data missing.")
        return False
    print("[Redis Seed] Seeding warehouse data into Redis...")
    count = 0
    try:
        for wh in warehouse_data:
            warehouse_id = wh.get('warehouse_id')
            if not warehouse_id: continue
            key = f"{WAREHOUSE_KEY_PREFIX}{warehouse_id}"
            redis_data = {
                'warehouse_id': warehouse_id,
                'warehouse_name': wh.get('warehouse_name'),
                'address': wh.get('warehouse_address'),
                'latitude': wh.get('latitude'),
                'longitude': wh.get('longitude'),
                'total_capacity_volume': wh.get('capacity', {}).get('volume_m3'),
                'num_loading_bays': wh.get('num_loading_bays', 5), 
                'last_seeded': datetime.now(timezone.utc).isoformat()
            }
            redis_data_filtered = {k: v for k, v in redis_data.items() if v is not None}
            r.hset(key, mapping=redis_data_filtered)
            count += 1
        print(f"[Redis Seed] Successfully seeded {count} warehouses.")
        return True
    except redis.exceptions.RedisError as e: print(f"[Redis Seed] Redis error: {e}"); return False
    except Exception as e: print(f"[Redis Seed] Unexpected error: {e}"); return False

# --- Phase 9: Warehouse Lookup Logic ---
def haversine_distance(lat1, lon1, lat2, lon2):
    try: lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    except (ValueError, TypeError): return float('inf')
    lon1_rad, lat1_rad, lon2_rad, lat2_rad = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2_rad - lon1_rad; dlat = lat2_rad - lat1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    if a < 0: a = 0; 
    if a > 1: a = 1
    try: c = 2 * math.asin(math.sqrt(a)); 
    except ValueError: c = 0
    r = 6371; return c * r

def find_warehouse_by_coords(r, target_lat, target_lon):
    if not r: return None
    try: target_lat_f, target_lon_f = float(target_lat), float(target_lon)
    except (ValueError, TypeError): print(f"[WH Lookup] Invalid target coords: ({target_lat}, {target_lon})"); return None
    nearest_warehouse_id = None; min_distance = float('inf')
    try:
        cursor = '0'; checked_count = 0
        while cursor != 0:
            cursor, keys = r.scan(cursor=cursor, match=f"{WAREHOUSE_KEY_PREFIX}*", count=100)
            for key in keys:
                checked_count += 1; warehouse_data = r.hgetall(key)
                wh_id = warehouse_data.get("warehouse_id"); wh_lat = warehouse_data.get("latitude"); wh_lon = warehouse_data.get("longitude")
                if wh_id and wh_lat and wh_lon:
                    distance = haversine_distance(target_lat_f, target_lon_f, wh_lat, wh_lon)
                    if distance < min_distance: min_distance = distance; nearest_warehouse_id = wh_id
        if checked_count == 0: print(f"[WH Lookup] No warehouse data found in Redis for lookup.")
        elif nearest_warehouse_id:
            if min_distance > 50.0: print(f"[WH Lookup] Nearest ({nearest_warehouse_id}) > 50km away. No match."); nearest_warehouse_id = None
            else: print(f"[WH Lookup] Nearest WH to ({target_lat}, {target_lon}) is {nearest_warehouse_id} ({min_distance:.2f} km)")
        else: print(f"[WH Lookup] Checked {checked_count} warehouses, none found/valid.")
        return nearest_warehouse_id
    except redis.exceptions.RedisError as e: print(f"[WH Lookup] Redis error: {e}"); return None
    except Exception as e: print(f"[WH Lookup] Unexpected error: {e}"); return None
# --- End Phase 9 Logic ---

def load_config():
    # Load .env from the current directory
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        print(f"Loaded environment variables from: {dotenv_path}")
    else:
        print(f"Warning: .env file not found in {os.path.dirname(__file__)}. Using default or environment variables.")

    config = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST', 'localhost'),
        "rabbitmq_port": int(os.getenv('RABBITMQ_PORT', 5672)),
        "rabbitmq_user": os.getenv('RABBITMQ_USER'),
        "rabbitmq_pass": os.getenv('RABBITMQ_PASS'),
        # Trip update consumer config
        "trip_update_exchange": os.getenv('TRIP_UPDATE_EXCHANGE', 'trip_update_exchange'),
        "trip_update_routing_key": os.getenv('TRIP_UPDATE_ROUTING_KEY', 'trip.*'),
        "warehouse_trip_update_queue": os.getenv('WAREHOUSE_TRIP_UPDATE_QUEUE', 'warehouse_trip_updates'),
        # Trip completed consumer config (Phase 5)
        "warehouse_ops_exchange": os.getenv('WAREHOUSE_OPS_EXCHANGE', 'warehouse_ops_exchange'),
        "trip_completed_binding_key": os.getenv('WAREHOUSE_TRIP_COMPLETED_BINDING_KEY', 'trip.completed'),
        "trip_completed_queue_name": os.getenv('WAREHOUSE_TRIP_COMPLETED_QUEUE', 'warehouse_trip_completed'),
        # Batch Ready publisher config (Phase 6)
        "vehicle_ops_exchange": os.getenv('VEHICLE_OPS_EXCHANGE', 'vehicle_ops_exchange'),
        "batch_ready_routing_key": os.getenv('BATCH_READY_ROUTING_KEY', 'batch.ready'),
        # Vehicle Arrived consumer config (Phase 6)
        "vehicle_arrived_binding_key": os.getenv('VEHICLE_ARRIVED_BINDING_KEY', 'vehicle.arrived.pickup'),
        "vehicle_arrived_queue_name": os.getenv('VEHICLE_ARRIVED_QUEUE', 'warehouse_vehicle_arrived'),
        # Redis config
        "redis_host": os.getenv('REDIS_HOST', 'localhost'),
        "redis_port": int(os.getenv('REDIS_PORT', 6379)),
        "redis_db": int(os.getenv('REDIS_DB', 0)),
    }
    return config

# --- RabbitMQ Publisher Setup (similar to vehicle_management) ---
_publisher_channel = None
_publisher_connection = None
_publisher_lock = threading.Lock()

def get_rabbitmq_connection(config):
    credentials = None
    if config["rabbitmq_user"] and config["rabbitmq_pass"]:
        credentials = pika.PlainCredentials(config["rabbitmq_user"], config["rabbitmq_pass"])
    parameters = pika.ConnectionParameters(
        host=config["rabbitmq_host"],
        port=config["rabbitmq_port"],
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(parameters)

# --- Redis Connection (same as before) ---
def get_redis_connection(config):
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.Redis(
                host=config["redis_host"],
                port=config["redis_port"],
                db=config["redis_db"],
                decode_responses=True
            )
            redis_client.ping()
            print("[Warehouse Service] Connected to Redis.")
        except Exception as e:
            print(f"[Warehouse Service] Error connecting to Redis: {e}")
            redis_client = None
    return redis_client

# --- New Phase 7 Helper Function ---
def update_warehouse_storage_redis(redis_conn, warehouse_id, order_code, update_type, item_details=None):
    """
    Simulates creating or updating a WarehouseStorage record in Redis.
    update_type: 'input' (create on arrival) or 'output' (update on pickup)
    item_details: Dictionary containing item info (weight, volume) - Placeholder for now
    """
    if not redis_conn:
        print(f"[Warehouse Storage] Redis unavailable, cannot update storage for order {order_code}.")
        return False
    if not warehouse_id:
        print(f"[Warehouse Storage] Warehouse ID unknown, cannot update storage for order {order_code}.")
        return False # Cannot store without warehouse ID

    storage_key = f"storage:{warehouse_id}:{order_code}"
    current_time_iso = datetime.now(timezone.utc).isoformat()

    try:
        if update_type == 'input':
            # Simulate getting weight/volume - Placeholder values!
            # In a real system, this should come from the order or item data.
            weight_kg = item_details.get('weight_kg', 10.0) if item_details else 10.0
            volume_m3 = item_details.get('volume_m3', 0.1) if item_details else 0.1

            storage_data = {
                "warehouse_id": warehouse_id,
                "order_code": order_code,
                "input_date": current_time_iso,
                "output_date": None, # Initially null
                "weight_kg": weight_kg,
                "volume_m3": volume_m3,
                "last_updated": current_time_iso
            }
            # Use HSET to store the hash
            redis_conn.hset(storage_key, mapping=storage_data)
            print(f"[Warehouse Storage] Created storage entry for Order: {order_code} at Warehouse: {warehouse_id}")

        elif update_type == 'output':
            # Check if the record exists before updating
            if redis_conn.exists(storage_key):
                redis_conn.hset(storage_key, "output_date", current_time_iso)
                redis_conn.hset(storage_key, "last_updated", current_time_iso)
                print(f"[Warehouse Storage] Updated output_date for Order: {order_code} at Warehouse: {warehouse_id}")
            else:
                # This might happen if the 'input' message was missed or order is unexpected
                print(f"[Warehouse Storage] Warning: Tried to update output_date for non-existent storage record: {storage_key}")
        else:
            print(f"[Warehouse Storage] Unknown update_type: {update_type}")
            return False
        return True
    except redis.exceptions.RedisError as e:
        print(f"[Warehouse Storage] Redis error updating storage key '{storage_key}': {e}")
    except Exception as e:
        print(f"[Warehouse Storage] Unexpected error updating storage key '{storage_key}': {e}")
    return False
# --- End New Phase 7 Helper Function ---

def declare_rabbitmq_topology(channel, config, context):
    # context: 'trip_completed_consumer', 'vehicle_arrived_consumer', 'trip_update_consumer' (Phase 9)
    print(f"Declaring topology for context: {context}")
    declared_queue_name = None; exchange = None; queue_name_cfg = None; routing_key = None; exchange_type = 'topic'

    # Determine params based on context - Use the full context string including _consumer
    if context == 'trip_completed_consumer':
        exchange = config["warehouse_ops_exchange"]
        queue_name_cfg = config["trip_completed_queue_name"] # Ensure this matches load_config
        routing_key = config["trip_completed_binding_key"] # Ensure this matches load_config
    elif context == 'vehicle_arrived_consumer':
        exchange = config["warehouse_ops_exchange"]
        queue_name_cfg = config["vehicle_arrived_queue_name"] # Ensure this matches load_config
        routing_key = config["vehicle_arrived_binding_key"] # Ensure this matches load_config
    elif context == 'trip_update_consumer': # Phase 9
        exchange = config["trip_update_exchange"]
        queue_name_cfg = config["warehouse_trip_update_queue"]
        routing_key = config["trip_update_routing_key"]
    elif context == 'publisher': # Phase 6 publisher context
         exchange = config["vehicle_ops_exchange"]
    else: 
        # Keep the error for truly unexpected contexts
        raise ValueError(f"Invalid context specified for topology declaration: {context}") 

    # Declare exchange if defined for the context
    if exchange:
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        print(f"Declared exchange: {exchange}")
    else:
         print(f"Warning: No exchange defined for context '{context}'.")
         return None # Cannot proceed for consumer without exchange

    # Declare queue and bind if it's a consumer context
    if queue_name_cfg and routing_key:
        result = channel.queue_declare(queue=queue_name_cfg, durable=True)
        queue_name = result.method.queue
        channel.queue_bind(queue=queue_name, exchange=exchange, routing_key=routing_key)
        print(f"Bound queue {queue_name} to exchange {exchange} with key {routing_key}")
        return queue_name
    elif context != 'publisher':
         print(f"Warning: Queue/binding info missing for consumer context '{context}'.")
         
    return None # Return None if no queue declared (e.g., for publisher)


def get_publisher_channel(config):
    """Gets a dedicated channel for publishing messages."""
    global _publisher_channel, _publisher_connection
    with _publisher_lock:
        # ... (Implementation similar to vehicle_management, ensure context='publisher') ...
        if _publisher_channel and _publisher_channel.is_open:
            return _publisher_channel
        print("[Publisher] Establishing connection for publishing...")
        try:
            if _publisher_connection and _publisher_connection.is_open:
                 _publisher_channel = _publisher_connection.channel()
                 declare_rabbitmq_topology(_publisher_channel, config, 'publisher') # Declare publisher exchange
                 print("[Publisher] Reused connection, created new channel.")
                 return _publisher_channel
            _publisher_connection = get_rabbitmq_connection(config)
            _publisher_channel = _publisher_connection.channel()
            declare_rabbitmq_topology(_publisher_channel, config, 'publisher') # Declare publisher exchange
            print("[Publisher] Established new connection and channel.")
            return _publisher_channel
        except Exception as e:
            print(f"[Publisher] Failed to get publisher channel: {e}")
            if _publisher_channel and _publisher_channel.is_open: 
                try: _publisher_channel.close() 
                except: pass
            if _publisher_connection and _publisher_connection.is_open: 
                try: _publisher_connection.close() 
                except: pass
            _publisher_channel, _publisher_connection = None, None
            return None

def publish_batch_ready(config, batch_id, trip_id, warehouse_id):
    """Publishes a message indicating a batch is ready for pickup."""
    publisher_channel = get_publisher_channel(config)
    if not publisher_channel:
        print("[Publisher] Cannot publish batch ready: No publisher channel.")
        return

    message = {
        "event_type": "batch_ready_for_pickup",
        "batch_id": batch_id,
        "trip_order_id": trip_id,
        "warehouse_id": warehouse_id,
        "ready_at": datetime.now(timezone.utc).isoformat()
    }
    message_body = json.dumps(message)

    try:
        publisher_channel.basic_publish(
            exchange=config["vehicle_ops_exchange"],
            routing_key=config["batch_ready_routing_key"],
            body=message_body,
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
        )
        print(f"[Publisher] Published Batch Ready message for Batch ID: {batch_id}")
    except Exception as e:
        print(f"[Publisher] Error publishing Batch Ready message: {e}")


# --- Callbacks --- 

def trip_update_callback(channel, method, properties, body, config):
    """Handles trip update messages to infer warehouse IDs from coordinates."""
    r = get_redis_connection(config)
    # print(f"[Trip Update Consumer] Received message (Tag: {method.delivery_tag}) - RK: {method.routing_key}") # Verbose
    processed_successfully = False
    try:
        message = json.loads(body.decode('utf-8'))
        trip_id = message.get("trip_order_id")
        source_lat = message.get("source_latitude")
        source_lon = message.get("source_longitude")
        dest_lat = message.get("destination_latitude")
        dest_lon = message.get("destination_longitude")

        if trip_id:
            print(f"[Trip Update Consumer] Processing Trip ID: {trip_id}")
            if r:
                # Check source coordinates
                if source_lat is not None and source_lon is not None:
                    inferred_source_wh_id = find_warehouse_by_coords(r, source_lat, source_lon)
                    if inferred_source_wh_id: print(f"[Trip Update Consumer] ---> Inferred Source Warehouse ID: {inferred_source_wh_id}")
                    # Potential TODO: Add logic to update WarehouseStatusHistory if needed
                
                # Check destination coordinates
                if dest_lat is not None and dest_lon is not None:
                    inferred_dest_wh_id = find_warehouse_by_coords(r, dest_lat, dest_lon)
                    if inferred_dest_wh_id: print(f"[Trip Update Consumer] ---> Inferred Destination Warehouse ID: {inferred_dest_wh_id}")
                    # Potential TODO: Add logic here too
            else:
                print(f"[Trip Update Consumer] Cannot infer warehouse (Redis unavailable) for Trip {trip_id}.")
            processed_successfully = True # Mark as processed even if lookup failed
        else:
            print(f"[Trip Update Consumer] Ignoring message - Missing trip_id.")
            processed_successfully = True # Ack messages without trip_id

    except json.JSONDecodeError as e: print(f"[Trip Update Consumer] Error decoding JSON: {e}. Acking."); processed_successfully = True 
    except Exception as e: print(f"[Trip Update Consumer] Unexpected error: {e}")
    finally:
        try:
            # Always ACK these messages unless error prevented flag set
            if processed_successfully: channel.basic_ack(delivery_tag=method.delivery_tag)
            else: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False); print(f"[Trip Update Consumer] NACKed msg {method.delivery_tag}")
        except Exception as channel_err: print(f"[Trip Update Consumer] Error Ack/Nack: {channel_err}")

def trip_completed_callback(channel, method, properties, body, config):
    processed_successfully = False # Flag for final ack/nack
    try:
        message = json.loads(body.decode('utf-8'))
        print(f"[Warehouse Service] Received Trip Completed (Tag: {method.delivery_tag}):")
        print(json.dumps(message, indent=2))

        trip_id = message.get("trip_order_id")
        batch_id = message.get("batch_id")
        completion_status = message.get("status", "Unknown")

        if not trip_id or not batch_id:
            print(f"[Warehouse Service] TripCompleted - Invalid message, missing trip_id or batch_id. Acking.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if completion_status == "Completed":
            print(f"[Warehouse Service] Processing completion for Trip: {trip_id}, Batch: {batch_id}")
            r = get_redis_connection(config)
            if r:
                # --- Phase 7: Update WarehouseStorage ---
                destination_warehouse_id = None
                try:
                    # Attempt to fetch TripOrder to find destination warehouse
                    trip_key = f"trip:{trip_id}"
                    trip_data = r.hgetall(trip_key)
                    if trip_data:
                        # PROBLEM: TripOrder doesn't store dest warehouse ID, only lat/lon.
                        # Design Limitation: Cannot reliably determine destination warehouse ID.
                        # For simulation, we'd need to either:
                        # 1. Add dest warehouse ID to TripOrder in dispatch_service
                        # 2. Add dest warehouse ID to trip_completed message in vehicle_management
                        # 3. Implement find_nearest_warehouse logic here (complex)
                        # Using a placeholder for now.
                        # If trip_type indicates delivery, we assume destination is a warehouse.
                        if trip_data.get("trip_type") in ["Inter-Warehouse Transfer", "Pickup/Delivery"]:
                            # This is a guess/placeholder!
                            destination_warehouse_id = trip_data.get("destination_warehouse_id", "DEST_WH_UNKNOWN")
                            print(f"[Warehouse Service] Info: Inferred destination warehouse '{destination_warehouse_id}' for Trip: {trip_id} (Placeholder!)")
                        else:
                             print(f"[Warehouse Service] Info: Trip {trip_id} type '{trip_data.get('trip_type')}' may not end at a warehouse. Skipping storage input.")

                    else:
                        print(f"[Warehouse Service] Warning: Could not fetch TripOrder data for {trip_id} to determine destination warehouse.")

                except redis.exceptions.RedisError as e:
                    print(f"[Warehouse Service] Redis error fetching trip data for {trip_id}: {e}")
                except Exception as e:
                    print(f"[Warehouse Service] Error processing trip data for {trip_id}: {e}")


                # Only proceed if we think it arrived at a warehouse
                if destination_warehouse_id:
                    batch_items_key = f"batch:{batch_id}:items"
                    try:
                        item_ids = r.smembers(batch_items_key)
                        print(f"[Warehouse Service] Found {len(item_ids)} items in Batch: {batch_id} for storage input.")
                        for item_id in item_ids:
                            item_key = f"batch_item:{item_id}"
                            item_data_str = r.get(item_key)
                            if item_data_str:
                                item_data = json.loads(item_data_str)
                                order_code = item_data.get("order_code")
                                if order_code:
                                    # Pass placeholder details to the storage function
                                    item_details_placeholder = {
                                        # Add actual weight/volume if available in item_data later
                                        'weight_kg': item_data.get('weight_kg'), # Will be None if not present
                                        'volume_m3': item_data.get('volume_m3')  # Will be None if not present
                                    }
                                    update_warehouse_storage_redis(r, destination_warehouse_id, order_code, 'input', item_details_placeholder)
                                else:
                                    print(f"[Warehouse Service] Warning: Item {item_id} missing order_code.")
                            else:
                                print(f"[Warehouse Service] Warning: Could not fetch details for item {item_id}.")
                    except redis.exceptions.RedisError as e:
                        print(f"[Warehouse Service] Redis error fetching/processing items for batch {batch_id}: {e}")
                    except json.JSONDecodeError as e:
                         print(f"[Warehouse Service] Error decoding item JSON for batch {batch_id}: {e}")
                    except Exception as e:
                         print(f"[Warehouse Service] Error processing items for batch {batch_id}: {e}")
                # --- End Phase 7 ---

                # Previous Phase 5/6 logic (Update Batch status, etc.)
                batch_key = f"batch:{batch_id}"
                try:
                    updated_count = r.hset(batch_key, "status", "Done")
                    r.hset(batch_key, "updated_at", datetime.now(timezone.utc).isoformat())
                    if updated_count == 0: print(f"[Warehouse Service] Updated batch {batch_id} status to Done (field added).")
                    else: print(f"[Warehouse Service] Updated batch {batch_id} status to Done (field updated).")
                except redis.exceptions.RedisError as e:
                    print(f"[Warehouse Service] Redis error updating batch {batch_id} status: {e}")

                print(f"[Warehouse Service] SIMULATING: Would update warehouse capacity/bays based on Batch: {batch_id}") # Keep simulation log

            else:
                print("[Warehouse Service] Cannot perform updates: Redis unavailable.")
        else:
            print(f"[Warehouse Service] Received non-Completed status '{completion_status}' for Trip: {trip_id}. No action taken.")

        # Acknowledge the message if processing reached here without fatal error
        processed_successfully = True

    except json.JSONDecodeError as e:
        print(f"[Warehouse Service] TripCompleted - Error decoding JSON: {e}. Message body: {body}")
        processed_successfully = True # Ack malformed message
    except Exception as e:
        print(f"[Warehouse Service] TripCompleted - Unexpected error processing message {method.delivery_tag}: {e}")
        # Attempt NACK below

    # Final Ack/Nack based on flag
    try:
        if processed_successfully:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[Warehouse Service] Acknowledged Trip Completed message {method.delivery_tag}")
        else:
            print(f"[Warehouse Service] Attempting NACK for Trip Completed message {method.delivery_tag}.")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            print(f"[Warehouse Service] TripCompleted - NACKed message {method.delivery_tag}, requeue=True")
    except Exception as channel_err:
        print(f"[Warehouse Service] TripCompleted - Error during final Ack/Nack: {channel_err}")

# Callback for Vehicle Arrived messages (Phase 6)
def vehicle_arrived_callback(channel, method, properties, body, config):
    processed_successfully = False # Flag for final ack/nack
    try:
        message = json.loads(body.decode('utf-8'))
        print(f"[Warehouse Service] Received Vehicle Arrived (Tag: {method.delivery_tag}):")
        print(json.dumps(message, indent=2))

        trip_id = message.get("trip_order_id")
        batch_id = message.get("batch_id")
        vehicle_id = message.get("vehicle_id")
        warehouse_id = message.get("warehouse_id") # Phase 6: Included in the message

        if not trip_id or not batch_id or not vehicle_id or not warehouse_id:
            print(f"[Warehouse Service] VehicleArrived - Invalid message, missing required fields. Acking.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[Warehouse Service] Processing vehicle arrival at Warehouse {warehouse_id} for Trip: {trip_id}, Batch: {batch_id}, Vehicle: {vehicle_id}")
        r = get_redis_connection(config)
        if r:
            # --- Phase 7: Update WarehouseStorage (Output Date) ---
            batch_items_key = f"batch:{batch_id}:items"
            try:
                item_ids = r.smembers(batch_items_key)
                print(f"[Warehouse Service] Found {len(item_ids)} items in Batch: {batch_id} for storage output update.")
                for item_id in item_ids:
                    item_key = f"batch_item:{item_id}"
                    item_data_str = r.get(item_key)
                    if item_data_str:
                        item_data = json.loads(item_data_str)
                        order_code = item_data.get("order_code")
                        if order_code:
                            # Update the storage record with output_date
                            update_warehouse_storage_redis(r, warehouse_id, order_code, 'output')
                        else:
                            print(f"[Warehouse Service] Warning: Item {item_id} missing order_code.")
                    else:
                        print(f"[Warehouse Service] Warning: Could not fetch details for item {item_id}.")
            except redis.exceptions.RedisError as e:
                 print(f"[Warehouse Service] Redis error fetching/processing items for batch {batch_id} (storage output): {e}")
            except json.JSONDecodeError as e:
                 print(f"[Warehouse Service] Error decoding item JSON for batch {batch_id} (storage output): {e}")
            except Exception as e:
                 print(f"[Warehouse Service] Error processing items for batch {batch_id} (storage output): {e}")
            # --- End Phase 7 ---

            # Previous Phase 6 Logic (Update Batch Status, etc.)
            batch_key = f"batch:{batch_id}"
            update_time = datetime.now(timezone.utc).isoformat()
            try:
                update_mapping = {
                    "status": "In Transit",
                    "location_type": "Vehicle",
                    "location_id": vehicle_id,
                    "updated_at": update_time
                }
                r.hset(batch_key, mapping=update_mapping)
                print(f"[Warehouse Service] Updated batch {batch_id} status to In Transit, Location: Vehicle {vehicle_id}.")

                batch_items_key = f"batch:{batch_id}:items"
                item_ids = r.smembers(batch_items_key) # Re-fetch needed? Reuse from above if guaranteed.
                print(f"[Warehouse Service] SIMULATING: Update Order Status (In Delivery) & Create OrderStatusHistory for {len(item_ids)} items in Batch: {batch_id}")

            except redis.exceptions.RedisError as e:
                 print(f"[Warehouse Service] Redis error updating batch {batch_id} for vehicle arrival: {e}")
            except Exception as e:
                 print(f"[Warehouse Service] Error during vehicle arrival update for {batch_id}: {e}")
        else:
            print(f"[Warehouse Service] Cannot perform updates for vehicle arrival: Redis unavailable.")

        # Acknowledge the message if processing reached here without fatal error
        processed_successfully = True

    except json.JSONDecodeError as e:
        print(f"[Warehouse Service] VehicleArrived - Error decoding JSON: {e}. Message body: {body}")
        processed_successfully = True # Ack malformed message
    except Exception as e:
        print(f"[Warehouse Service] VehicleArrived - Unexpected error processing message {method.delivery_tag}: {e}")
        # Attempt NACK below

    # Final Ack/Nack based on flag
    try:
        if processed_successfully:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[Warehouse Service] Acknowledged Vehicle Arrived message {method.delivery_tag}")
        else:
            print(f"[Warehouse Service] Attempting NACK for Vehicle Arrived message {method.delivery_tag}.")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            print(f"[Warehouse Service] VehicleArrived - NACKed message {method.delivery_tag}, requeue=True")
    except Exception as channel_err:
        print(f"[Warehouse Service] VehicleArrived - Error during final Ack/Nack: {channel_err}")

# --- Consumer Thread Runner (Generic) ---
def run_consumer(config, queue_context, callback_func):
    """Generic function to run a blocking consumer in a loop."""
    consumer_connection = None; consumer_channel = None; queue_name = f"UNKNOWN ({queue_context})"
    while True:
        try:
            # Establish connection and channel if needed
            if consumer_connection is None or consumer_connection.is_closed:
                print(f"[{queue_context} Consumer] Connecting...")
                consumer_connection = get_rabbitmq_connection(config)
                consumer_channel = consumer_connection.channel()
                print(f"[{queue_context} Consumer] Connected.")
                # Declare topology specific to this consumer
                queue_name = declare_rabbitmq_topology(consumer_channel, config, queue_context)
                if not queue_name: 
                    print(f"[ERROR][{queue_context}] Failed topology declaration. Retrying...")
                    raise ConnectionError("Topology failed for consumer") # Raise error to trigger reconnect
                consumer_channel.basic_qos(prefetch_count=1)
                # Pass config to all callbacks using lambda
                on_message_callback = lambda ch, method, properties, body: callback_func(ch, method, properties, body, config)
                consumer_channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)
                print(f"[{queue_context} Consumer] Consuming from '{queue_name}'...")
            
            # Start consuming (blocks here until connection/channel issue or stop)
            if consumer_channel: consumer_channel.start_consuming()
            else: 
                # This state should be temporary due to connection logic above
                print(f"[{queue_context} Consumer] Channel is None before consuming. Retrying...")
                time.sleep(5); 
                continue

        # --- Exception Handling --- 
        except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPChannelError, pika.exceptions.AMQPConnectionError, ConnectionError) as e:
            print(f"[{queue_context} Consumer] Connection/Channel Error ({type(e).__name__}). Reconnecting in 5s...")
            # Clean up connection before retrying
            if consumer_connection and not consumer_connection.is_closed: 
                try: consumer_connection.close(); 
                except Exception as close_err: print(f"Error closing connection: {close_err}")
            consumer_connection, consumer_channel = None, None; 
            time.sleep(5)
        except KeyboardInterrupt: 
            print(f"[{queue_context} Consumer] Interrupted. Stopping...")
            # Clean up connection before exiting thread
            if consumer_connection and not consumer_connection.is_closed: 
                try: consumer_connection.close(); 
                except Exception as close_err: print(f"Error closing connection on interrupt: {close_err}")
            break # Exit the while loop
        except Exception as e:
            print(f"[{queue_context} Consumer] Unexpected error: {type(e).__name__}: {e}. Restarting in 10s...")
             # Clean up connection before retrying
            if consumer_connection and not consumer_connection.is_closed: 
                try: consumer_connection.close(); 
                except Exception as close_err: print(f"Error closing connection: {close_err}")
            consumer_connection, consumer_channel = None, None; 
            time.sleep(10)
            
    print(f"[{queue_context} Consumer] Consumer thread finished.")

def main():
    cfg = load_config()
    print("Starting Warehouse Service (Phase 9 - Geo Lookup)...") 
    r = get_redis_connection(cfg) 
    if r:
        warehouse_data = load_warehouse_data_from_json()
        if warehouse_data: seed_warehouses_redis(r, warehouse_data)
        else: print("CRITICAL: Failed to load warehouse data.")
    else: print("CRITICAL: Redis connection failed.")

    # --- Start Consumers in Threads ---
    consumer_threads = []
    callback_map = {
        'trip_completed_consumer': trip_completed_callback,
        'vehicle_arrived_consumer': vehicle_arrived_callback,
        'trip_update_consumer': trip_update_callback # Phase 9
    }

    for context, callback_func in callback_map.items():
        thread = threading.Thread(
            target=run_consumer, 
            args=(cfg, context, callback_func), 
            name=f"{context.replace('_consumer','').capitalize()}Consumer", 
            daemon=True
        )
        consumer_threads.append(thread)
        thread.start()

    print("All consumer threads started.")

    # Keep main thread alive
    try:
        while any(t.is_alive() for t in consumer_threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("Main thread interrupted. Shutting down...")
    except Exception as e:
        print(f"Main thread encountered error: {e}")

    print("Warehouse Service finished.")

if __name__ == "__main__":
    main() 