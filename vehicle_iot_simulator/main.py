import pika
import os
import sys
import time
import json
import random
import uuid
import threading # Added for concurrent consuming/publishing
from datetime import datetime, timezone
from dotenv import load_dotenv
import math # Added for distance calc
import redis # Phase 16
import polyline # Phase 16

# Global dictionary to track vehicle assignments (vehicle_id: trip_order_id)
# In a real system, this state might be managed differently or sync'd with vehicle_management
# Phase 16: This might need adjustment - assignment is now more complex
vehicle_assignments = {}
# Lock for thread-safe access to vehicle_assignments
assignment_lock = threading.Lock()

# Phase 16: Global dictionary to track route following progress
# { vehicle_id: {"route_coords": [(lat, lon), ...], "current_index": 0, "assignment_id": "..."} }
vehicle_route_progress = {}
route_progress_lock = threading.Lock()

# Phase 16: Redis connection
redis_client = None
def get_redis_connection(config):
    global redis_client
    # No need to reconnect if already connected and connection is valid
    if redis_client:
        try:
            redis_client.ping()
            return redis_client # Return existing valid client
        except redis.exceptions.ConnectionError:
            print("[Redis] Existing connection lost. Reconnecting...")
            redis_client = None # Force reconnect
        except Exception as e:
             print(f"[Redis] Error checking existing connection: {e}. Reconnecting...")
             redis_client = None

    # Attempt to establish a new connection
    try:
        redis_client = redis.Redis(
            host=config.get("redis_host", "localhost"),
            port=config.get("redis_port", 6379),
            db=config.get("redis_db", 0),
            decode_responses=True # Important for easier handling
        )
        redis_client.ping()
        print("[Redis] Connected successfully.")
        return redis_client
    except redis.exceptions.ConnectionError as e:
        print(f"[Redis] Connection failed: {e}")
        redis_client = None
        return None
    except Exception as e:
         print(f"[Redis] Unexpected error connecting: {e}")
         redis_client = None
         return None

# --- Phase 16: Polyline Decoder ---
def decode_polyline(p_line):
    """Decodes a polyline string into a list of (lat, lon) tuples."""
    if not p_line:
        return []
    try:
        # polyline library expects precision 5 by default
        decoded_coords = polyline.decode(p_line)
        # Ensure coords are float
        return [(float(lat), float(lon)) for lat, lon in decoded_coords]
    except Exception as e:
        print(f"[Polyline] Error decoding polyline '{p_line[:30]}...': {e}")
        return []
# --- End Phase 16 ---

# --- Phase 16: Helper to get WH coords --- 
def get_warehouse_coords(r, warehouse_id):
    """Fetches latitude and longitude for a given warehouse ID from Redis."""
    if not r or not warehouse_id:
        return None, None
    try:
        key = f"warehouse:{warehouse_id}" # Assuming this key structure based on veh_mgmt seeding
        data = r.hgetall(key)
        lat = data.get('latitude')
        lon = data.get('longitude')
        if lat and lon:
            return float(lat), float(lon)
        else:
            print(f"[Redis] Coordinates not found in Redis for WH {warehouse_id} at key {key}")
    except redis.exceptions.RedisError as e:
        print(f"[Redis] Error fetching coords for WH {warehouse_id}: {e}")
    except (ValueError, TypeError) as e:
        print(f"[Redis] Invalid coord data for WH {warehouse_id}: {e}")
    except Exception as e:
        print(f"[Redis] Unexpected error fetching coords for WH {warehouse_id}: {e}")
    return None, None
# --- End Phase 16 ---

def load_config():
    # Load .env from the current directory
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env') # Read from example
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        print(f"Loaded environment variables from: {dotenv_path}")
    else:
        # Try loading .env.example if .env doesn't exist
        dotenv_example_path = os.path.join(os.path.dirname(__file__), '.env.example')
        if os.path.exists(dotenv_example_path):
             load_dotenv(dotenv_path=dotenv_example_path)
             print(f"Loaded environment variables from: {dotenv_example_path}")
        else:
            print(f"Warning: Neither .env nor .env.example file found in {os.path.dirname(__file__)}. Using default or environment variables.")

    config = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST', 'localhost'),
        "rabbitmq_port": int(os.getenv('RABBITMQ_PORT', 5672)),
        "rabbitmq_user": os.getenv('RABBITMQ_USER'),
        "rabbitmq_pass": os.getenv('RABBITMQ_PASS'),
        # Location Update Publisher Config
        "location_update_exchange": os.getenv('VEHICLE_LOCATION_UPDATE_EXCHANGE', 'vehicle_updates_exchange'), # Renamed for clarity
        "location_update_routing_key": os.getenv('VEHICLE_LOCATION_UPDATE_ROUTING_KEY', 'vehicle.location.update'), # Renamed
        "interval_seconds": int(os.getenv('UPDATE_INTERVAL_SECONDS', 30)), # Phase 8: 30 seconds
        # Assignment Request Consumer Config (Phase 8)
        "assignment_request_exchange": os.getenv('VEHICLE_ASSIGNMENT_REQUEST_EXCHANGE', 'vehicle_assignment_exchange'),
        "assignment_request_queue": os.getenv('VEHICLE_ASSIGNMENT_REQUEST_QUEUE', 'vehicle_assignment_request_queue'),
        "assignment_request_routing_key": os.getenv('VEHICLE_ASSIGNMENT_REQUEST_ROUTING_KEY', 'vehicle.assignment.request'),
        # Assignment Response Publisher Config (Phase 8)
        "assignment_response_exchange": os.getenv('VEHICLE_ASSIGNMENT_RESPONSE_EXCHANGE', 'vehicle_assignment_exchange'), # Can reuse exchange
        "assignment_response_routing_key": os.getenv('VEHICLE_ASSIGNMENT_RESPONSE_ROUTING_KEY', 'vehicle.assignment.response'),
        "approval_delay_seconds": int(os.getenv('APPROVAL_DELAY_SECONDS', 1)), # Phase 8: 20 seconds delay
        # Phase 16: Redis Config
        "redis_host": os.getenv('REDIS_HOST', 'localhost'),
        "redis_port": int(os.getenv('REDIS_PORT', 6379)),
        "redis_db": int(os.getenv('REDIS_DB', 0)),
        # Phase 16: Constants
        "max_history_entries": int(os.getenv('MAX_VEHICLE_HISTORY_ENTRIES', 100))
    }
    return config

def get_rabbitmq_connection(config):
    credentials = None
    if config["rabbitmq_user"] and config["rabbitmq_pass"]:
        credentials = pika.PlainCredentials(config["rabbitmq_user"], config["rabbitmq_pass"])
    
    parameters = pika.ConnectionParameters(
        host=config["rabbitmq_host"],
        port=config["rabbitmq_port"],
        credentials=credentials
    )
    return pika.BlockingConnection(parameters)

def declare_rabbitmq_topology(channel, config):
    print("Declaring RabbitMQ Topology...")
    # 1. Exchange for Publishing Location Updates (Existing)
    channel.exchange_declare(
        exchange=config["location_update_exchange"],
        exchange_type='topic',
        durable=True
    )
    print(f"Declared Location Update Exchange: {config['location_update_exchange']}")

    # 2. Exchange for Assignment Requests/Responses (Phase 8)
    # Using a single topic exchange for both for simplicity, could be separate
    channel.exchange_declare(
        exchange=config["assignment_request_exchange"], # Also used for responses
        exchange_type='topic',
        durable=True
    )
    print(f"Declared Assignment Exchange: {config['assignment_request_exchange']}")

    # 3. Queue for Consuming Assignment Requests (Phase 8)
    channel.queue_declare(
        queue=config["assignment_request_queue"],
        durable=True # Ensure queue survives restarts
    )
    print(f"Declared Assignment Request Queue: {config['assignment_request_queue']}")

    # 4. Binding Assignment Request Queue to Exchange (Phase 8)
    channel.queue_bind(
        exchange=config["assignment_request_exchange"],
        queue=config["assignment_request_queue"],
        routing_key=config["assignment_request_routing_key"]
    )
    print(f"Bound Queue '{config['assignment_request_queue']}' to Exchange '{config['assignment_request_exchange']}' with RK '{config['assignment_request_routing_key']}'")

def generate_vehicle_update(vehicle_id, config):
    # Phase 16: Route following logic
    r = get_redis_connection(config)

    route_update_generated = False
    lat, lon = None, None
    status = "Unknown"
    current_trip_id = None
    speed = 0.0
    heading = None

    active_assignment_id = None
    route_polyline = None
    assignment_trip_id = None

    # 1. Check Redis for active "En Route to Source" assignment
    if r:
        try:
            cursor = '0'
            while cursor != 0:
                cursor, keys = r.scan(cursor=cursor, match="assignment:*", count=100)
                should_break = False
                for key in keys:
                    try:
                        data = r.hgetall(key)
                        # Match vehicle AND status
                        if data.get("vehicle_id") == vehicle_id and data.get("approval_status") == "En Route to Source":
                            active_assignment_id = data.get("assignment_id")
                            route_polyline = data.get("route_to_source")
                            assignment_trip_id = data.get("trip_order_id")
                            print(f"[Location Sim] Found active assignment {active_assignment_id} for {vehicle_id} (Route: {len(route_polyline) if route_polyline else 0} chars)")
                            should_break = True; break # Found the active one
                    except Exception as e_hget:
                        print(f"[Location Sim] Error reading assignment key {key}: {e_hget}")
                if should_break: break # Exit outer loop too

        except redis.exceptions.RedisError as e_scan:
            print(f"[Location Sim] Redis error scanning assignments: {e_scan}")
        except Exception as e_outer:
            print(f"[Location Sim] Error checking assignments: {e_outer}")
    else:
        print(f"[Location Sim] Warning: No Redis connection, cannot check for active assignment.")

    # 2. Route Following Logic
    if active_assignment_id and route_polyline:
        with route_progress_lock:
            progress = vehicle_route_progress.get(vehicle_id)
            load_new_route = False

            if not progress or progress.get("assignment_id") != active_assignment_id:
                print(f"[Location Sim] Loading new route for {vehicle_id}, assignment {active_assignment_id}")
                decoded_coords = decode_polyline(route_polyline)
                if decoded_coords:
                    vehicle_route_progress[vehicle_id] = {
                        "route_coords": decoded_coords,
                        "current_index": 0,
                        "assignment_id": active_assignment_id
                    }
                    progress = vehicle_route_progress[vehicle_id] # Update local var
                else:
                    print(f"[Location Sim] Failed to decode route for assignment {active_assignment_id}. Clearing progress.")
                    vehicle_route_progress.pop(vehicle_id, None)
                    progress = None # Ensure fallback is triggered
            
            # If route is loaded (or was just loaded)
            if progress:
                current_index = progress["current_index"]
                route_coords = progress["route_coords"]
                if current_index < len(route_coords):
                    lat, lon = route_coords[current_index]
                    status = "En Route to Source"
                    current_trip_id = assignment_trip_id # Use trip ID from the assignment
                    # Simulate some movement speed/heading
                    speed = random.uniform(30.0, 60.0)
                    heading = random.randint(0, 359) # Simplified
                    progress["current_index"] += 1
                    print(f"[Location Sim] {vehicle_id} following route, step {current_index+1}/{len(route_coords)} -> ({lat:.4f}, {lon:.4f})")
                    route_update_generated = True
                else:
                    # Route finished
                    print(f"[Location Sim] {vehicle_id} finished route for assignment {active_assignment_id}. Clearing progress.")
                    # Get final coords for this update
                    if route_coords:
                        lat, lon = route_coords[-1]
                    # Decide status after route completion? Back to Idle?
                    status = "Idle" # Or maybe "Arrived at Source"?
                    current_trip_id = None # Clear trip association
                    speed = 0.0
                    heading = None
                    # Remove progress entry
                    vehicle_route_progress.pop(vehicle_id, None)
                    route_update_generated = True # Publish final point

    # 3. Fallback Logic (if no active route or route finished/failed)
    if not route_update_generated:
        # Ensure route progress is cleared if we fall back
        with route_progress_lock:
            vehicle_route_progress.pop(vehicle_id, None)
        
        # Use original fallback logic (based on potentially stale vehicle_assignments dict)
        # print(f"[Location Sim] {vehicle_id} using fallback location generation.")
        temp_trip_id = None
        fallback_status = "Idle"
        with assignment_lock:
            temp_trip_id = vehicle_assignments.get(vehicle_id)
        if temp_trip_id:
            fallback_status = "Delivering" # Or other non-idle status
        else:
             fallback_status = "Idle"
        
        status = fallback_status
        current_trip_id = temp_trip_id
        lat = random.uniform(10.7, 10.9)  # Random HCMC location
        lon = random.uniform(106.6, 106.8)
        speed = random.uniform(0.0, 80.0) if status != "Idle" else 0.0
        heading = random.randint(0, 359) if status != "Idle" else None

    # 4. Construct final update message
    update = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z"),
        "vehicle_id": vehicle_id,
        "trip_order_id": current_trip_id,
        "status": status,
        "gps": {
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "altitude_m": round(random.uniform(5.0, 50.0), 1),
            "accuracy_m": round(random.uniform(1.0, 15.0), 1)
        },
        "speed_kmh": round(speed, 1),
        "heading": heading,
        "odometer_km": round(random.uniform(1000.0, 200000.0), 1)
    }
    return update

# Phase 8: Callback for Assignment Request Messages
def assignment_request_callback(channel, method, properties, body, config):
    global vehicle_assignments
    print(f"[Assign Consumer] Received assignment request (Tag: {method.delivery_tag}):")
    try:
        message = json.loads(body.decode('utf-8'))
        print(json.dumps(message, indent=2))

        trip_order_id = message.get("trip_order_id")
        vehicle_id = message.get("vehicle_id")
        # Other fields like batch_id, warehouse_ids are present but not used by IoT sim directly for now

        if not trip_order_id or not vehicle_id:
            print(f"[Assign Consumer] Invalid assignment request, missing trip_order_id or vehicle_id. Acking.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[Assign Consumer] Processing assignment for Vehicle: {vehicle_id}, Trip: {trip_order_id}")

        # Simulate driver/system checking and deciding (Phase 8: Fixed 20s delay and approve)
        delay = config["approval_delay_seconds"]
        print(f"[Assign Consumer] Simulating approval delay of {delay} seconds...")
        time.sleep(delay)

        approval_status = "approved" # Per phase 8 requirement
        print(f"[Assign Consumer] Assignment {approval_status} for Vehicle: {vehicle_id}, Trip: {trip_order_id}")

        # Update internal state (thread-safe)
        with assignment_lock:
            vehicle_assignments[vehicle_id] = trip_order_id

        # Construct response message
        response_message = {
            "trip_order_id": trip_order_id,
            "vehicle_id": vehicle_id,
            "approval_status": approval_status,
            "response_timestamp": datetime.now(timezone.utc).isoformat()
        }
        response_body = json.dumps(response_message)

        # Publish response back to vehicle_management
        try:
            # Need a separate channel or reuse? Reusing within callback is complex.
            # Best practice is often separate connections/channels for publish/consume.
            # For simplicity here, let's try reopening channel if needed, but this isn't robust.
            # A better approach uses separate threads or async programming.
            # Let's assume the channel passed in is usable for publishing (may fail in real pika usage)

            # Ensure the publisher channel is open (this is a simplification)
             # temp_connection = get_rabbitmq_connection(config)
             # temp_channel = temp_connection.channel()
             # declare_rabbitmq_topology(temp_channel, config) # Ensure topology exists

             # Re-get channel from the callback's connection if possible (safer than global)
             pub_channel = channel # Try using the consumer's channel

             pub_channel.basic_publish(
                 exchange=config["assignment_response_exchange"],
                 routing_key=config["assignment_response_routing_key"],
                 body=response_body,
                 properties=pika.BasicProperties(
                     delivery_mode=2, # Persistent
                     content_type='application/json',
                     correlation_id=properties.correlation_id # Echo correlation_id if provided
                 )
             )
             print(f"[Assign Consumer] Published approval response for Trip: {trip_order_id}")
             # temp_connection.close() # Close temporary connection


        except Exception as pub_err:
            print(f"[Assign Consumer] Failed to publish assignment response: {pub_err}")
            # Decide if we should NACK the original message or still ACK?
            # If response fails, the assignment state is inconsistent.
            # For now, log error and still ACK the request.
            # Consider NACK + requeue if publishing response is critical for workflow.

        # Acknowledge the original request message
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[Assign Consumer] Acknowledged assignment request message {method.delivery_tag}")

    except json.JSONDecodeError as e:
        print(f"[Assign Consumer] Error decoding JSON: {e}. Message body: {body}")
        channel.basic_ack(delivery_tag=method.delivery_tag) # Ack malformed message
    except Exception as e:
        print(f"[Assign Consumer] Unexpected error processing message {method.delivery_tag}: {e}")
        try:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Don't requeue on unexpected error
            print(f"[Assign Consumer] NACKed message {method.delivery_tag}")
        except Exception as nack_err:
            print(f"[Assign Consumer] Failed to NACK message {method.delivery_tag}: {nack_err}")


# Phase 8: Function to run the consumer loop
def run_assignment_consumer(config):
    consumer_connection = None
    while True:
        try:
            if consumer_connection is None or consumer_connection.is_closed:
                print("[Assign Consumer] Connecting to RabbitMQ...")
                consumer_connection = get_rabbitmq_connection(config)
                consumer_channel = consumer_connection.channel()
                declare_rabbitmq_topology(consumer_channel, config) # Ensure topology exists
                consumer_channel.basic_qos(prefetch_count=1) # Process one message at a time

                on_message_callback = lambda ch, method, properties, body: assignment_request_callback(ch, method, properties, body, config)

                consumer_channel.basic_consume(
                    queue=config["assignment_request_queue"],
                    on_message_callback=on_message_callback
                    # auto_ack=False # Manual ack is handled in callback
                )
                print("[Assign Consumer] Started consuming assignment requests. Waiting for messages...")
                consumer_channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[Assign Consumer] Connection error: {e}. Retrying in 5 seconds...")
            if consumer_connection and not consumer_connection.is_closed:
                try: consumer_connection.close()
                except Exception: pass
            consumer_connection = None
            time.sleep(5)
        except KeyboardInterrupt:
            print("[Assign Consumer] Interrupted. Stopping consumer...")
            if consumer_connection and not consumer_connection.is_closed:
                consumer_connection.close()
            break
        except Exception as e:
            print(f"[Assign Consumer] Unexpected error in consumer loop: {e}. Restarting consumer in 10s.")
            if consumer_connection and not consumer_connection.is_closed:
                 try: consumer_connection.close()
                 except Exception: pass
            consumer_connection = None
            time.sleep(10)


# Phase 8: Function to run the publisher loop
def run_location_publisher(config):
    publisher_connection = None
    publisher_channel = None
    interval_seconds = config.get("interval_seconds", 30)

    while True:
        try:
            # Ensure connection and channel are open
            if publisher_connection is None or publisher_connection.is_closed:
                print("[Location Publisher] Connecting to RabbitMQ...")
                publisher_connection = get_rabbitmq_connection(config)
                publisher_channel = publisher_connection.channel()
                # Declare topology needed for publishing
                declare_rabbitmq_topology(publisher_channel, config)
                print("[Location Publisher] Connected and topology declared.")
            
            # Phase 19: Get current vehicle list from Redis
            current_vehicle_ids = []
            r = get_redis_connection(config)
            if r:
                try:
                    cursor = '0'
                    while cursor != 0:
                        cursor, keys = r.scan(cursor=cursor, match="vehicle:SIM_*", count=100)
                        for key in keys:
                            if not key.endswith(":history"):
                                # Extract ID from key like 'vehicle:SIM_PICKUP_1_1'
                                vehicle_id = key.split(':', 1)[-1]
                                current_vehicle_ids.append(vehicle_id)
                except redis.exceptions.RedisError as e:
                    print(f"[Location Publisher] Error scanning Redis for vehicles: {e}")
                except Exception as e_scan:
                    print(f"[Location Publisher] Unexpected error scanning Redis: {e_scan}")
            else:
                print("[Location Publisher] Warning: No Redis connection, cannot fetch vehicle list.")

            if not current_vehicle_ids:
                print("[Location Publisher] No vehicles found in Redis to simulate. Sleeping...")
            else:
                print(f"[Location Publisher] Found {len(current_vehicle_ids)} vehicles in Redis to update.")
                for vehicle_id in current_vehicle_ids:
                    update_message = generate_vehicle_update(vehicle_id, config)
                    body = json.dumps(update_message)
                    
                    try:
                        publisher_channel.basic_publish(
                            exchange=config["location_update_exchange"],
                            routing_key=config["location_update_routing_key"],
                            body=body,
                            properties=pika.BasicProperties(
                                delivery_mode=2, # Persistent
                                content_type='application/json'
                            )
                        )
                        # print(f"[Location Publisher] Published update for {vehicle_id}") # Very verbose
                    except pika.exceptions.AMQPConnectionError as e_pub_conn:
                        print(f"[Location Publisher] AMQPConnectionError during publish: {e_pub_conn}. Attempting reconnect...")
                        # Close potentially broken connection/channel
                        if publisher_channel and publisher_channel.is_open: publisher_channel.close()
                        if publisher_connection and publisher_connection.is_open: publisher_connection.close()
                        publisher_connection, publisher_channel = None, None
                        time.sleep(2) # Brief pause before outer loop reconnects
                        break # Exit inner loop to trigger reconnect
                    except Exception as e_pub:
                        print(f"[Location Publisher] Error publishing update for {vehicle_id}: {e_pub}")
                        # Decide whether to continue or break/reconnect

            # Wait before next batch of updates
            # print(f"[Location Publisher] Updates sent. Sleeping for {interval_seconds} seconds...") # Verbose
            time.sleep(interval_seconds)

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[Location Publisher] Connection error: {e}. Retrying in 5 seconds...")
            if publisher_connection and not publisher_connection.is_closed:
                try: publisher_connection.close()
                except Exception: pass
            publisher_connection = None
            time.sleep(5)
            continue # Skip sleep interval on connection error
        except KeyboardInterrupt:
            print("[Location Publisher] Interrupted. Stopping publisher...")
            if publisher_connection and not publisher_connection.is_closed:
                publisher_connection.close()
            break # Exit the loop cleanly
        except Exception as e:
            print(f"[Location Publisher] An unexpected error occurred: {e}")
            # Consider adding more specific error handling or logging
            time.sleep(config["interval_seconds"]) # Still sleep even if publish failed
            continue

    print("[Location Publisher] Publisher loop finished.")


# --- Phase 16: Initialize Vehicle History --- 
def initialize_vehicle_history(config):
    """Adds an initial history entry for each simulated vehicle based on home region."""
    print("[Init History] Initializing vehicle location history...")
    r = get_redis_connection(config)
    if not r:
        print("[Init History] Error: Cannot initialize history, Redis connection failed.")
        return

    num_vehicles = config.get("num_vehicles", 5) # Get number of vehicles from config
    initialized_count = 0

    for i in range(num_vehicles):
        # Construct vehicle ID based on simulation convention (adjust if needed)
        # Assuming IDs are like SIM_PICKUP_1_1, SIM_HEAVY_1_1 etc.
        # This part is tricky without knowing the exact IDs seeded by veh_mgmt
        # Let's try scanning keys first as a more robust approach.
        pass # Placeholder - Need to implement scanning or use num_vehicles convention

    # Alternative/Better: Scan for vehicle keys seeded by vehicle_management
    vehicle_keys = []
    try:
        cursor = '0'
        while cursor != 0:
            cursor, keys = r.scan(cursor=cursor, match="vehicle:SIM_*", count=100)
            for key in keys:
                 if not key.endswith(":history"): # Exclude history keys
                     vehicle_keys.append(key)
    except redis.exceptions.RedisError as e:
         print(f"[Init History] Error scanning for vehicle keys: {e}")
         return
    
    print(f"[Init History] Found {len(vehicle_keys)} vehicle keys to initialize.")

    for vehicle_key in vehicle_keys:
        try:
            vehicle_id = vehicle_key.split(':')[-1]
            vehicle_data = r.hgetall(vehicle_key)
            home_region = vehicle_data.get('home_region')
            if not home_region:
                 print(f"[Init History] Warning: Missing home_region for {vehicle_id}. Skipping history init.")
                 continue

            # Get warehouse coords based on home_region (which should be a warehouse_id)
            wh_lat, wh_lon = get_warehouse_coords(r, home_region)
            if wh_lat is None or wh_lon is None:
                print(f"[Init History] Warning: Could not get coordinates for home region/warehouse {home_region} for {vehicle_id}. Using defaults.")
                # Use generic default coords if warehouse lookup fails
                wh_lat = 10.8
                wh_lon = 106.7

            # Create initial history entry
            initial_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z"),
                "vehicle_id": vehicle_id,
                "trip_order_id": None,
                "status": "Idle",
                "gps": {
                    "latitude": wh_lat,
                    "longitude": wh_lon,
                    "altitude_m": 10.0, # Default
                    "accuracy_m": 10.0  # Default
                },
                "speed_kmh": 0.0,
                "heading": None,
                "odometer_km": vehicle_data.get('odometer_km', 0.0) # Try to get from vehicle data if exists
            }
            history_entry_json = json.dumps(initial_entry)
            
            # Add to history list
            history_key = f"vehicle:{vehicle_id}:history"
            # Clear existing history? Optional. Let's just add.
            r.lpush(history_key, history_entry_json)
            # Trim immediately to maintain size limit
            r.ltrim(history_key, 0, config.get("max_history_entries", 100) - 1)
            initialized_count += 1
            # print(f"[Init History] Initialized history for {vehicle_id} at {wh_lat},{wh_lon}")

        except redis.exceptions.RedisError as e:
            print(f"[Init History] Redis error processing {vehicle_key}: {e}")
        except Exception as e:
            print(f"[Init History] Unexpected error processing {vehicle_key}: {e}")
    
    print(f"[Init History] Finished initializing history for {initialized_count} vehicles.")
# --- End Phase 16 --- 

def main():
    cfg = load_config() # Load config once
    print("Starting Vehicle IoT Simulator...")

    # --- Phase 16: Initialize Redis Connection & History ---
    # Establish initial connection (needed for history init)
    r = get_redis_connection(cfg)
    if not r:
        print("CRITICAL: Failed initial Redis connection. History initialization skipped.")
    else:
        # Initialize history based on vehicle home regions
        initialize_vehicle_history(cfg)
    # --- End Phase 16 ---

    # Start RabbitMQ consumers/publishers in threads
    assignment_consumer_thread = threading.Thread(
        target=run_assignment_consumer,
        args=(cfg,),
        name="AssignmentConsumer",
        daemon=True
    )
    location_publisher_thread = threading.Thread(
        target=run_location_publisher,
        args=(cfg,),
        name="LocationPublisher",
        daemon=True
    )

    assignment_consumer_thread.start()
    location_publisher_thread.start()

    print("Assignment consumer and location publisher threads started.")

    # Keep main thread alive while daemon threads run
    try:
        while assignment_consumer_thread.is_alive() or location_publisher_thread.is_alive():
            # Check threads periodically
            if not assignment_consumer_thread.is_alive():
                print("Warning: Assignment Consumer thread died.")
                # Optionally implement restart logic here
            if not location_publisher_thread.is_alive():
                print("Warning: Location Publisher thread died.")
                # Optionally implement restart logic here

            time.sleep(5) # Sleep for a while before checking again

    except KeyboardInterrupt:
        print("Main thread interrupted by user. Shutting down...")
        # Threads are daemons, they will exit automatically when the main thread exits.
        # Add explicit cleanup/shutdown signals here if necessary for graceful shutdown.
    except Exception as e:
        print(f"Main thread encountered an error: {e}")

    print("Vehicle IoT Simulator shutting down.")


if __name__ == "__main__":
    main() 