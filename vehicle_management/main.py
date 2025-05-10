import pika
import os
import sys
import time
import json
import threading
import redis # Added Redis
import requests # Phase 15: Re-added for route_to_source
from dotenv import load_dotenv
from datetime import datetime, timezone # Added datetime
import random
import uuid # Added for correlation_id
import math # Added for Phase 11 distance calculation

# --- Redis Connection ---
redis_client = None
def get_redis_connection(config):
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.Redis(
                host=config.get("redis_host", "localhost"),
                port=config.get("redis_port", 6379),
                db=config.get("redis_db", 0),
                decode_responses=True # Decode responses to strings
            )
            redis_client.ping()
            print("[Redis] Connected successfully.")
        except redis.exceptions.ConnectionError as e:
            print(f"[Redis] Connection failed: {e}")
            redis_client = None # Ensure it's None if connection fails
    return redis_client
# --- End Redis ---

# --- Redis Simulation Functions (Phase 8) ---

# Key Definitions
PENDING_ASSIGNMENT_SET_KEY = "trips:pending_assignment"
VEHICLE_KEY_PREFIX = "vehicle:"
VEHICLE_ASSIGNMENT_KEY_PREFIX = "assignment:"
TRIP_ORDER_KEY_PREFIX = "trip:"
TRIP_PENDING_DETAILS_KEY_PREFIX = "trip_pending_details:" # Added prefix

def add_placeholder_vehicles_redis(r, num_vehicles=5):
    """Adds simple placeholder vehicle data to Redis for simulation."""
    print("[Redis] Adding placeholder vehicle data...")
    vehicles = []
    for i in range(1, num_vehicles + 1):
        v_id = f"SIM_VEHICLE_{i}" # Match prefix used in IoT sim if needed
        # Assign types somewhat randomly, ensure some heavy trucks if needed later
        v_type = "Pickup Truck" if i % 3 != 0 else "Heavy Truck"
        # Simple status, assume initially Idle at a placeholder home region
        home_region = f"REGION_{(i % 2) + 1}" # Simple home region assignment
        vehicle_data = {
            "vehicle_id": v_id,
            "vehicle_type": v_type,
            "status": "Idle", # Initial status
            "current_latitude": random.uniform(10.7, 10.9), # Placeholder location
            "current_longitude": random.uniform(106.6, 106.8),
            "home_region": home_region, # Needed for assignment rules
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        key = f"{VEHICLE_KEY_PREFIX}{v_id}"
        try:
            r.hset(key, mapping=vehicle_data)
            vehicles.append(v_id)
        except redis.exceptions.RedisError as e:
            print(f"[Redis] Error adding vehicle {v_id}: {e}")
    print(f"[Redis] Added/Updated {len(vehicles)} placeholder vehicles: {vehicles}")

def find_suitable_vehicle_redis(r, trip_details):
    """Finds an available vehicle near the trip source that meets capacity requirements.
       Phase 17: Uses latest entry from vehicle history list for location/distance check.
    """
    if not r: # Added Redis check
        print("[AssignWorker] Redis connection unavailable in find_suitable_vehicle.")
        return None
    try:
        source_lat = float(trip_details.get('source_latitude'))
        source_lon = float(trip_details.get('source_longitude'))
    except (TypeError, ValueError) as e:
        print(f"[AssignWorker] Invalid source coordinates in trip details: {e}")
        return None

    print(f"[AssignWorker] Searching for suitable vehicle near {source_lat:.4f}, {source_lon:.4f} (using latest history)")
    best_vehicle_id = None
    min_distance = float('inf') # Find the closest suitable vehicle

    try:
        # --- Check active assignments (remains the same) ---
        assigned_vehicles = set()
        cursor = '0'
        while cursor != 0:
            cursor, keys = r.scan(cursor=cursor, match=f"{VEHICLE_ASSIGNMENT_KEY_PREFIX}*", count=100)
            for key in keys:
                 try:
                     key_type = r.type(key)
                     if key_type == 'hash':
                         assignment_data = r.hgetall(key)
                         if assignment_data.get("approval_status") not in ["completed", "rejected", "failed"]:
                             assigned_vehicles.add(assignment_data.get("vehicle_id"))
                     elif key_type != 'none':
                          print(f"[AssignWorker] Warning: Expected assignment key '{key}' to be Hash, found '{key_type}'. Skipping.")
                 except redis.exceptions.RedisError as e_assign:
                     print(f"[AssignWorker] Redis error getting assignment '{key}': {e_assign}")
                 except Exception as e_assign_other:
                     print(f"[AssignWorker] Unexpected error processing assignment key '{key}': {e_assign_other}")
        # --- End Check active assignments --- 

        # --- Iterate through vehicles --- 
        cursor = '0'
        checked_count = 0
        skipped_wrong_type = 0
        while cursor != 0:
            cursor, keys = r.scan(cursor=cursor, match=f"{VEHICLE_KEY_PREFIX}*", count=100)
            for key in keys:
                # Skip history list keys explicitly
                if key.endswith(":history"):
                    continue

                checked_count += 1
                vehicle_id = key.split(':')[-1]

                if vehicle_id in assigned_vehicles:
                    continue

                try:
                    key_type = r.type(key)
                    if key_type == 'hash':
                        vehicle_data = r.hgetall(key)
                    elif key_type == 'none': continue
                    else: print(f"[AssignWorker] Warning: Expected vehicle key '{key}' to be Hash, found '{key_type}'. Skipping."); skipped_wrong_type += 1; continue

                    # 1. Status Check (remains the same)
                    status = vehicle_data.get("status")
                    if status not in ["Idle", "Returning", "Available"]:
                        continue

                    # --- Phase 17: Use History for Location --- 
                    v_lat = None
                    v_lon = None
                    history_key = f"vehicle:{vehicle_id}:history"
                    try:
                        # Get the latest entry (index 0)
                        latest_history_json = r.lindex(history_key, 0)
                        if latest_history_json:
                             latest_history_entry = json.loads(latest_history_json)
                             gps_data = latest_history_entry.get("gps")
                             if gps_data:
                                 v_lat = gps_data.get("latitude")
                                 v_lon = gps_data.get("longitude")
                        # else: # No history found
                        #     print(f"[AssignWorker] No history found for vehicle {vehicle_id}. Skipping.")
                        #     continue 
                    except redis.exceptions.RedisError as e_hist:
                        print(f"[AssignWorker] Redis error getting history for '{history_key}': {e_hist}")
                        # Optionally fallback to vehicle_data location? Or skip?
                        continue # Skip if history fails
                    except json.JSONDecodeError as e_json:
                         print(f"[AssignWorker] Error decoding history JSON for '{history_key}': {e_json}")
                         continue # Skip if history is corrupt
                    except Exception as e_hist_other:
                         print(f"[AssignWorker] Error processing history for '{history_key}': {e_hist_other}")
                         continue # Skip on other history errors
                    
                    # If no valid location from history, skip this vehicle
                    if v_lat is None or v_lon is None:
                         # print(f"[AssignWorker] Could not get valid latest location from history for {vehicle_id}. Skipping.")
                         continue
                    # --- End Phase 17 Location Logic --- 

                    # 3. Calculate distance (using historical location)
                    try:
                        # Ensure coords are float for calculation
                        distance = haversine_distance(source_lat, source_lon, float(v_lat), float(v_lon))
                    except (ValueError, TypeError):
                        print(f"[AssignWorker] Invalid coordinates for distance calc (Veh: {vehicle_id}, HistLoc: {v_lat},{v_lon}). Skipping.")
                        continue # Skip if coords invalid for distance

                    # 4. Capacity/Type Checks (Optional - remains the same)
                    # ... add checks if needed ...
                    
                    # 5. Check if closer than current best (remains the same)
                    MAX_ASSIGNMENT_DISTANCE_KM = 50 # Example threshold
                    if distance < min_distance and distance <= MAX_ASSIGNMENT_DISTANCE_KM:
                        min_distance = distance
                        best_vehicle_id = vehicle_id
                        print(f"[AssignWorker] Found potential vehicle {vehicle_id} ({distance:.2f} km away based on history, Status: {status})")

                except redis.exceptions.RedisError as e_vehicle:
                    # ... (error handling for vehicle key access remains) ...
                    print(f"[AssignWorker] Redis error getting vehicle '{key}': {e_vehicle}")
                except Exception as e_vehicle_other:
                     print(f"[AssignWorker] Unexpected error processing vehicle key '{key}': {e_vehicle_other}")

        print(f"[AssignWorker] Checked {checked_count} vehicles. Skipped {skipped_wrong_type} due to wrong type.")
        if best_vehicle_id:
            print(f"[AssignWorker] Selected vehicle {best_vehicle_id} (Distance: {min_distance:.2f} km based on history) for assignment.")
        else:
             print(f"[AssignWorker] No suitable vehicle found based on history and status.")
        return best_vehicle_id

    except redis.exceptions.RedisError as e:
        print(f"[AssignWorker] Redis error during vehicle search: {e}")
        return None
    except Exception as e:
        print(f"[AssignWorker] Unexpected error finding vehicle: {e}")
        return None


def create_vehicle_assignment_redis(r, trip_id, vehicle_id, batch_id, trip_details, config, vehicle_lat, vehicle_lon):
    """Simulates creating a VehicleAssignment record in Redis with 'pending' status.
       Phase 16: Calculates and adds route_to_source.
    """
    if not r: return False
    assignment_id = f"ASSIGN_{trip_id}_{vehicle_id[:5]}_{random.randint(100,999)}" # Simple unique ID
    key = f"{VEHICLE_ASSIGNMENT_KEY_PREFIX}{assignment_id}"
    
    # --- Phase 16: Calculate route_to_source --- 
    route_to_source_polyline = ""
    trip_source_lat = trip_details.get('source_latitude')
    trip_source_lon = trip_details.get('source_longitude')

    if vehicle_lat is not None and vehicle_lon is not None and trip_source_lat is not None and trip_source_lon is not None:
        print(f"[AssignWorker] Calculating route_to_source for assignment {assignment_id}...")
        # Vehicle type for routing might not be easily known here, default to car
        route_to_source_polyline = get_vietmap_route(config, vehicle_lat, vehicle_lon, trip_source_lat, trip_source_lon)
        if not route_to_source_polyline:
            print(f"[AssignWorker] Warning: Failed to get route_to_source from Vietmap API for assignment {assignment_id}.")
            route_to_source_polyline = "" # Ensure empty string
        else:
            print(f"[AssignWorker] Successfully obtained route_to_source polyline (length: {len(route_to_source_polyline)}) for assignment {assignment_id}.")
    else:
        print(f"[AssignWorker] Warning: Missing vehicle or trip source coordinates for assignment {assignment_id}. Cannot calculate route_to_source.")
    # --- End Phase 16 ---

    # Prepare data, handle potential None values explicitly
    notes_str = f"Assignment created for Trip {trip_id}"
    assigned_at_iso = datetime.now(timezone.utc).isoformat()
    last_updated_iso = assigned_at_iso # Use same timestamp initially

    assignment_data = {
        "assignment_id": assignment_id,
        "trip_order_id": trip_id,
        "vehicle_id": vehicle_id,
        "batch_id": batch_id if batch_id is not None else "", # Convert None batch_id to empty string
        "assigned_at": assigned_at_iso,
        "approval_status": "pending", # Initial status
        "estimated_arrival_at_source": "", # Use empty string instead of None - maybe calculate later?
        "route_to_source": route_to_source_polyline, # Phase 16: Calculated above
        "notes": notes_str,
        "last_updated": last_updated_iso
    }
    
    # Although we converted None above, double-check just in case 
    # (or if other fields could become None unexpectedly)
    # redis_safe_assignment_data = {k: (v if v is not None else "") for k, v in assignment_data.items()}
    # Using explicit conversion above is clearer

    try:
        # Use the dictionary with None values converted
        r.hset(key, mapping=assignment_data)
        print(f"[Redis] Created pending assignment: {key} for Trip: {trip_id}, Vehicle: {vehicle_id}")
        return True
    except redis.exceptions.RedisError as e:
        print(f"[Redis] Error creating assignment {key}: {e}")
        return False
    except Exception as e:
         print(f"[Redis] Unexpected error creating assignment {key}: {e}")
         return False

def update_vehicle_assignment_redis(r, trip_id, vehicle_id, approval_status):
    """Simulates updating the status of a VehicleAssignment record in Redis."""
    if not r: return False
    # Need to find the assignment ID. Scan is inefficient but necessary for sim.
    assignment_key_to_update = None
    try:
        cursor = '0'
        while cursor != 0:
            cursor, keys = r.scan(cursor=cursor, match=f"{VEHICLE_ASSIGNMENT_KEY_PREFIX}*", count=100)
            for key in keys:
                data = r.hgetall(key)
                if data.get("trip_order_id") == trip_id and data.get("vehicle_id") == vehicle_id:
                     # Found the assignment (assume only one matches for simplicity)
                     assignment_key_to_update = key
                     break
            if assignment_key_to_update: break

        if assignment_key_to_update:
            update_data = {
                "approval_status": approval_status,
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
            r.hset(assignment_key_to_update, mapping=update_data)
            print(f"[Redis] Updated assignment {assignment_key_to_update}: Status -> {approval_status}")

            # Also update the vehicle's status based on approval
            vehicle_key = f"{VEHICLE_KEY_PREFIX}{vehicle_id}"
            new_vehicle_status = "Idle" # Default if rejected or completed
            if approval_status == "approved":
                # Need trip details to know if it's pickup or delivery
                # Simplified: Assume "En Route to Source"
                new_vehicle_status = "En Route to Source"
            elif approval_status == "rejected":
                new_vehicle_status = "Idle" # Vehicle becomes available again

            r.hset(vehicle_key, "status", new_vehicle_status)
            print(f"[Redis] Updated vehicle {vehicle_id} status to: {new_vehicle_status}")

            return True
        else:
            print(f"[Redis] Warning: Could not find assignment record for Trip: {trip_id}, Vehicle: {vehicle_id} to update status.")
            return False
    except redis.exceptions.RedisError as e:
        print(f"[Redis] Error updating assignment for Trip: {trip_id}, Vehicle: {vehicle_id}: {e}")
        return False


def update_vehicle_location_redis(r, vehicle_id, location_data):
    """Updates the vehicle's main status hash and adds the update to a history list in Redis."""
    vehicle_key = f"{VEHICLE_KEY_PREFIX}{vehicle_id}"
    
    # Prepare data for HSET (ensure basic types or JSON strings)
    update_mapping = {}
    for k, v in location_data.items():
        if isinstance(v, (str, int, float, bytes)):
            update_mapping[k] = v
        elif v is None:
            update_mapping[k] = ""
        else:
            try:
                update_mapping[k] = json.dumps(v) # Store nested dicts (like gps) as JSON
            except TypeError:
                update_mapping[k] = str(v)

    # Add/Update mandatory fields if missing from incoming data
    update_mapping['vehicle_id'] = vehicle_id # Ensure vehicle_id is present
    if 'last_updated_timestamp' not in update_mapping:
         update_mapping['last_updated_timestamp'] = datetime.now(timezone.utc).isoformat()

    try:
        # 1. Update the main vehicle hash with the latest status
        # Use HSET with mapping (replaces HMSET which might be deprecated)
        r.hset(vehicle_key, mapping=update_mapping)
        # print(f"Updated vehicle hash: {vehicle_key}") # Verbose

        # --- Phase 14: Add to History List --- 
        history_key = f"vehicle:{vehicle_id}:history"
        # Store the raw incoming data (or the processed mapping?) - let's store raw for completeness
        try:
            history_entry_json = json.dumps(location_data) 
            # LPUSH adds to the head (left) of the list
            r.lpush(history_key, history_entry_json)
            # LTRIM keeps only the N most recent entries (index 0 to 99 = 100 entries)
            MAX_HISTORY_ENTRIES = 100 # Configurable? 
            r.ltrim(history_key, 0, MAX_HISTORY_ENTRIES - 1)
            # print(f"Added entry to history list: {history_key}") # Verbose
        except redis.exceptions.RedisError as hist_err:
            print(f"Redis Error adding to history list '{history_key}': {hist_err}")
        except Exception as hist_gen_err:
             print(f"Error adding to history list '{history_key}': {hist_gen_err}")
        # --- End Phase 14 --- 

        return True
    except redis.exceptions.RedisError as e:
        print(f"Redis Error updating vehicle hash '{vehicle_key}': {e}")
        return False
    except Exception as e:
        print(f"Error updating vehicle hash '{vehicle_key}': {e}")
        return False


def add_trip_to_pending_assignment(r, trip_id, trip_details_json):
    """Adds a trip ID and its details to the pending set/list for the worker."""
    if not r: return False
    try:
        # Using a Hash to store details, and a Set to track pending IDs
        trip_pending_details_key = f"{TRIP_PENDING_DETAILS_KEY_PREFIX}{trip_id}"
        # Store the full details received
        r.set(trip_pending_details_key, trip_details_json)
        # Add the ID to the set
        added = r.sadd(PENDING_ASSIGNMENT_SET_KEY, trip_id)
        if added:
            print(f"[Redis] Added Trip: {trip_id} to pending assignment set.")
        else:
             print(f"[Redis] Trip: {trip_id} already in pending assignment set.")
        return True
    except redis.exceptions.RedisError as e:
        print(f"[Redis] Error adding trip {trip_id} to pending set: {e}")
        return False

def get_pending_assignment_trips(r, count=5):
     """Gets a batch of trip IDs from the pending set."""
     if not r: return []
     try:
         # Get up to 'count' members randomly (SRANDMEMBER doesn't remove them)
         pending_ids = r.srandmember(PENDING_ASSIGNMENT_SET_KEY, count)
         return pending_ids
     except redis.exceptions.RedisError as e:
         print(f"[Redis] Error getting pending trips: {e}")
         return []

def get_trip_pending_details(r, trip_id):
    """Gets the stored details for a pending trip."""
    if not r: return None
    try:
        trip_pending_details_key = f"{TRIP_PENDING_DETAILS_KEY_PREFIX}{trip_id}"
        details_json = r.get(trip_pending_details_key)
        if details_json:
            return json.loads(details_json)
        else:
            print(f"[Redis] No pending details found for trip {trip_id}")
            return None
    except redis.exceptions.RedisError as e:
        print(f"[Redis] Error getting pending trip details for {trip_id}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"[Redis] Error decoding pending trip details JSON for {trip_id}: {e}")
        return None


def remove_trip_from_pending_assignment(r, trip_id):
    """Removes a trip ID from the pending set and deletes its details."""
    if not r: return False
    try:
        # Remove from set
        removed = r.srem(PENDING_ASSIGNMENT_SET_KEY, trip_id)
        # Delete details
        trip_pending_details_key = f"{TRIP_PENDING_DETAILS_KEY_PREFIX}{trip_id}"
        r.delete(trip_pending_details_key)
        if removed:
            print(f"[Redis] Removed Trip: {trip_id} from pending assignment set.")
        return True
    except redis.exceptions.RedisError as e:
        print(f"[Redis] Error removing trip {trip_id} from pending set: {e}")
        return False

# --- End Redis Simulation Functions ---

# --- Phase 9/11: Warehouse Data Loading and Seeding ---
# Added for Phase 11 requirement to look up warehouses by coordinates
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
    except FileNotFoundError: print(f"[Data Load] Error: File not found at {abs_filepath}"); return None
    except json.JSONDecodeError as e: print(f"[Data Load] Error: Failed to decode JSON: {e}"); return None
    except Exception as e: print(f"[Data Load] Error: Unexpected error loading data: {e}"); return None

def seed_warehouses_redis(r, warehouse_data):
    """Stores warehouse data into Redis hashes (needed for Phase 11 lookup)."""
    if not r or not warehouse_data:
        print("[Redis Seed] Cannot seed warehouses: Redis client or data missing.")
        return False
    print("[Redis Seed - Phase 11] Seeding warehouse coordinate data...")
    count = 0
    try:
        for wh in warehouse_data:
            warehouse_id = wh.get('warehouse_id')
            if not warehouse_id: continue
            key = f"{WAREHOUSE_KEY_PREFIX}{warehouse_id}"
            # Store only essential info needed for lookup by this service
            redis_data = {
                'warehouse_id': warehouse_id,
                'warehouse_name': wh.get('warehouse_name'),
                'latitude': wh.get('latitude'),
                'longitude': wh.get('longitude'),
                'last_seeded': datetime.now(timezone.utc).isoformat()
            }
            redis_data_filtered = {k: v for k, v in redis_data.items() if v is not None}
            r.hset(key, mapping=redis_data_filtered)
            count += 1
        print(f"[Redis Seed - Phase 11] Successfully seeded coordinates for {count} warehouses.")
        return True
    except redis.exceptions.RedisError as e: print(f"[Redis Seed - Phase 11] Redis error: {e}"); return False
    except Exception as e: print(f"[Redis Seed - Phase 11] Unexpected error: {e}"); return False

# --- Phase 9/11: Warehouse Lookup Logic ---
def haversine_distance(lat1, lon1, lat2, lon2):
    # ... (same implementation as in warehouse_service) ...
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
    # ... (same implementation as in warehouse_service) ...
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
# --- End Phase 9/11 Logic ---

# --- Phase 15: Vietmap API Route Fetching (Re-added) ---
def get_vietmap_route(config, source_lat, source_lon, dest_lat, dest_lon, vehicle_type="car"):
    """Fetches a route polyline from the Vietmap Routing API."""
    api_key = config.get("vietmap_api_key")
    base_url = config.get("vietmap_route_url")

    if not api_key:
        print("[Vietmap API] Error: VIETMAP_API_KEY is not configured.")
        return None
    if not base_url:
        print("[Vietmap API] Error: VIETMAP_ROUTE_URL is not configured.")
        # Default URL if not configured
        base_url = "https://maps.vietmap.vn/api/route"
        print(f"[Vietmap API] Warning: VIETMAP_ROUTE_URL not configured, using default: {base_url}")

    # Validate coords (basic check)
    try:
        f_source_lat = float(source_lat)
        f_source_lon = float(source_lon)
        f_dest_lat = float(dest_lat)
        f_dest_lon = float(dest_lon)
    except (ValueError, TypeError):
        print(f"[Vietmap API] Error: Invalid coordinates provided - Source:({source_lat},{source_lon}), Dest:({dest_lat},{dest_lon})")
        return None

    api_vehicle = "car" # Default
    # Determine vehicle type from parameter if needed (e.g., for trucks)
    # We might not know the vehicle type accurately here, default to car
    # if vehicle_type.lower() == "heavy truck":
    #     api_vehicle = "truck"
        
    params = {
        'api-version': '1.1', 
        'apikey': api_key,
        'point': [f"{f_source_lat},{f_source_lon}", f"{f_dest_lat},{f_dest_lon}"],
        'vehicle': api_vehicle, # Use determined vehicle type
        'points_encoded': 'true'
    }

    print(f"[Vietmap API] Requesting route from {f_source_lat},{f_source_lon} to {f_dest_lat},{f_dest_lon} for vehicle type: {api_vehicle}")

    try:
        response = requests.get(base_url, params=params, timeout=15)
        response.raise_for_status()
        route_data = response.json()
        if route_data and "paths" in route_data and len(route_data["paths"]) > 0 and "points" in route_data["paths"][0]:
            polyline = route_data["paths"][0]["points"]
            # Optionally log distance/time
            # distance_meters = route_data["paths"][0].get("distance", 0)
            # time_seconds = route_data["paths"][0].get("time", 0)
            # print(f"[Vietmap API] Received route. Polyline length: {len(polyline)}, Dist: {distance_meters/1000:.2f}km, Time: {time_seconds/60:.1f}min")
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
        # print(f"[Vietmap API] Response Text: {response.text[:200]}...")
        return None
    except Exception as e:
        print(f"[Vietmap API] Error: Unexpected error during API call: {e}")
        return None
# --- End Phase 15 --- 

def load_config():
    # Determine the directory of the current script
    script_dir = os.path.dirname(__file__)
    
    # Construct paths for .env and .env.example
    env_path = os.path.join(script_dir, '.env')
    env_example_path = os.path.join(script_dir, '.env.example')
    
    loaded_path = None
    # --- Corrected Loading Logic --- 
    # 1. Try to load .env first
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        loaded_path = env_path
    # 2. If .env doesn't exist, try loading .env.example as a fallback
    elif os.path.exists(env_example_path):
        load_dotenv(dotenv_path=env_example_path)
        loaded_path = env_example_path
        print(f"Warning: .env file not found. Loaded configuration from {loaded_path}") # Warn if using example
    # 3. If neither exists, print a warning
    else:
        print(f"Warning: Neither .env nor .env.example file found in {script_dir}. Using default or environment variables.")

    if loaded_path:
        print(f"Loaded environment variables from: {loaded_path}")
    # --- End Corrected Loading Logic ---

    config = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST', 'localhost'),
        "rabbitmq_port": int(os.getenv('RABBITMQ_PORT', 5672)),
        "rabbitmq_user": os.getenv('RABBITMQ_USER'),
        "rabbitmq_pass": os.getenv('RABBITMQ_PASS'),
        # Redis Config (Phase 8)
        "redis_host": os.getenv('REDIS_HOST', 'localhost'),
        "redis_port": int(os.getenv('REDIS_PORT', 6379)),
        "redis_db": int(os.getenv('REDIS_DB', 0)),
        # Vietmap API Config (Phase 15: Re-added)
        "vietmap_api_key": os.getenv('VIETMAP_API_KEY'),
        "vietmap_route_url": os.getenv('VIETMAP_ROUTE_URL', 'https://maps.vietmap.vn/api/route'), # Default added in function
        # --- RabbitMQ Configs ---
        # Trip Update Consumer (Existing)
        "trip_update_exchange": os.getenv('TRIP_UPDATE_EXCHANGE', 'trip_update_exchange'),
        "trip_update_routing_key": os.getenv('TRIP_UPDATE_ROUTING_KEY', 'trip.created'), # Listen for 'created' status
        "trip_update_queue_name": os.getenv('VEHICLE_TRIP_QUEUE', 'vehicle_trip_updates'),
        # Location Update Consumer (Existing - verify keys)
        "location_update_exchange": os.getenv('VEHICLE_LOCATION_UPDATE_EXCHANGE', 'vehicle_updates_exchange'), # Match IoT sim
        "location_update_binding_key": os.getenv('VEHICLE_LOCATION_UPDATE_ROUTING_KEY', 'vehicle.location.update'), # Match IoT sim
        "location_update_queue_name": os.getenv('VEHICLE_LOCATION_QUEUE', 'vehicle_location_updates'),
        # Trip Completion Publisher (Existing - To Warehouse)
        "warehouse_ops_exchange": os.getenv('WAREHOUSE_OPS_EXCHANGE', 'warehouse_ops_exchange'),
        "trip_completed_routing_key": os.getenv('WAREHOUSE_TRIP_COMPLETED_ROUTING_KEY', 'trip.completed'),
        # Batch Ready Consumer (Existing)
        "batch_ready_exchange": os.getenv('VEHICLE_OPS_EXCHANGE', 'vehicle_ops_exchange'), # Renamed for clarity
        "batch_ready_binding_key": os.getenv('BATCH_READY_BINDING_KEY', 'batch.ready'),
        "batch_ready_queue_name": os.getenv('BATCH_READY_QUEUE', 'vehicle_batch_ready'),
        # Vehicle Arrived Publisher (Existing - To Warehouse)
        "vehicle_arrived_routing_key": os.getenv('VEHICLE_ARRIVED_ROUTING_KEY', 'vehicle.arrived.pickup'),
        # Assignment Request Publisher (Phase 8 - To IoT Sim)
        "assignment_request_exchange": os.getenv('VEHICLE_ASSIGNMENT_REQUEST_EXCHANGE', 'vehicle_assignment_exchange'), # Match IoT sim
        "assignment_request_routing_key": os.getenv('VEHICLE_ASSIGNMENT_REQUEST_ROUTING_KEY', 'vehicle.assignment.request'), # Match IoT sim
        # Assignment Response Consumer (Phase 8 - From IoT Sim)
        "assignment_response_exchange": os.getenv('VEHICLE_ASSIGNMENT_RESPONSE_EXCHANGE', 'vehicle_assignment_exchange'), # Match IoT sim
        "assignment_response_queue": os.getenv('VEHICLE_ASSIGNMENT_RESPONSE_QUEUE', 'vehicle_assignment_response_queue'),
        "assignment_response_binding_key": os.getenv('VEHICLE_ASSIGNMENT_RESPONSE_ROUTING_KEY', 'vehicle.assignment.response'), # Match IoT sim
        # Assignment Worker Config (Phase 8)
        "assignment_retry_interval_seconds": int(os.getenv('ASSIGNMENT_RETRY_INTERVAL_SECONDS', 30)) # Retry interval from phase8.txt
    }
    print(config["rabbitmq_user"], config["rabbitmq_pass"])
    return config

def get_rabbitmq_connection(config):
    connection_params = {
        "host": config["rabbitmq_host"],
        "port": config["rabbitmq_port"],
        "heartbeat": 600,
        "blocked_connection_timeout": 300
    }

    # Check if both user and pass are provided and not empty/None
    print("LIfebow log --------------------")
    print(config)
    if config.get("rabbitmq_user") and config.get("rabbitmq_pass"):
        credentials = pika.PlainCredentials(config["rabbitmq_user"], config["rabbitmq_pass"])
        connection_params["credentials"] = credentials
        print("[RabbitMQ Connection] Attempting connection WITH credentials.")
    else:
        print("[RabbitMQ Connection] Attempting connection WITHOUT credentials (user/pass not provided or empty).")

    parameters = pika.ConnectionParameters(**connection_params)
    return pika.BlockingConnection(parameters)

def declare_rabbitmq_topology(channel, config, context):
    """Declares exchanges/queues. Context: 'trip_consumer', 'location_consumer', 'batch_ready_consumer', 'assignment_response_consumer', 'publisher'."""
    print(f"Declaring topology for context: {context}")
    declared_queue_name = None

    if context == 'trip_consumer':
        exchange = config["trip_update_exchange"]
        queue_name_cfg = config["trip_update_queue_name"]
        routing_key = config["trip_update_routing_key"]
        exchange_type = 'topic'
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        result = channel.queue_declare(queue=queue_name_cfg, durable=True)
        declared_queue_name = result.method.queue
        channel.queue_bind(queue=declared_queue_name, exchange=exchange, routing_key=routing_key)
        print(f"[Topology] Trip Consumer: Bound queue {declared_queue_name} to exchange {exchange} with key {routing_key}")

    elif context == 'location_consumer':
        exchange = config["location_update_exchange"]
        queue_name_cfg = config["location_update_queue_name"]
        binding_key = config["location_update_binding_key"]
        exchange_type = 'topic'
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        result = channel.queue_declare(queue=queue_name_cfg, durable=True)
        declared_queue_name = result.method.queue
        channel.queue_bind(queue=declared_queue_name, exchange=exchange, routing_key=binding_key)
        print(f"[Topology] Location Consumer: Bound queue {declared_queue_name} to exchange {exchange} with key {binding_key}")

    elif context == 'batch_ready_consumer':
        exchange = config["batch_ready_exchange"]
        queue_name_cfg = config["batch_ready_queue_name"]
        binding_key = config["batch_ready_binding_key"]
        exchange_type = 'topic'
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        result = channel.queue_declare(queue=queue_name_cfg, durable=True)
        declared_queue_name = result.method.queue
        channel.queue_bind(queue=declared_queue_name, exchange=exchange, routing_key=binding_key)
        print(f"[Topology] Batch Ready Consumer: Bound queue {declared_queue_name} to exchange {exchange} with key {binding_key}")

    elif context == 'assignment_response_consumer': # Phase 8
        exchange = config["assignment_response_exchange"]
        queue_name_cfg = config["assignment_response_queue"]
        binding_key = config["assignment_response_binding_key"]
        exchange_type = 'topic'
        # Exchange might be declared by publisher/IoT sim, but declare here to be safe
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        result = channel.queue_declare(queue=queue_name_cfg, durable=True)
        declared_queue_name = result.method.queue
        channel.queue_bind(queue=declared_queue_name, exchange=exchange, routing_key=binding_key)
        print(f"[Topology] Assignment Response Consumer: Bound queue {declared_queue_name} to exchange {exchange} with key {binding_key}")

    elif context == 'publisher':
        # Declare all exchanges this service might publish to
        exchanges_to_declare = [
            config["warehouse_ops_exchange"],        # For trip completed, vehicle arrived
            config["assignment_request_exchange"]   # For assignment requests (Phase 8)
        ]
        for exchange in exchanges_to_declare:
            channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)
            print(f"[Topology] Publisher: Declared exchange {exchange}")
        declared_queue_name = None # No queue needed for publisher setup

    else:
        raise ValueError(f"Invalid context specified for topology declaration: {context}")

    return declared_queue_name # Return the actual queue name declared by RabbitMQ


# --- Publisher Setup (Slightly Refactored) ---
_publisher_channel = None
_publisher_connection = None
_publisher_lock = threading.Lock()

# Phase 8: Renamed and slightly updated
def get_rabbitmq_publisher_channel(config):
    """Gets a dedicated channel for publishing messages, ensuring exchanges are declared."""
    global _publisher_channel, _publisher_connection
    with _publisher_lock:
        # Check if channel is open and connection is open
        if _publisher_channel and _publisher_channel.is_open and \
           _publisher_connection and _publisher_connection.is_open:
            return _publisher_channel

        print("[Publisher] Establishing/Verifying connection for publishing...")
        try:
            # Close previous channel/connection if they exist but are closed
            if _publisher_channel and not _publisher_channel.is_open:
                _publisher_channel = None
            if _publisher_connection and not _publisher_connection.is_open:
                 print("[Publisher] Previous connection closed, will reconnect.")
                 _publisher_connection = None
                 _publisher_channel = None # Channel is invalid if connection is closed

            # Establish new connection if needed
            if not _publisher_connection:
                _publisher_connection = get_rabbitmq_connection(config)
                print("[Publisher] Established new connection.")

            # Get channel and declare publisher topology
            _publisher_channel = _publisher_connection.channel()
            declare_rabbitmq_topology(_publisher_channel, config, 'publisher') # Declare all publisher exchanges
            print("[Publisher] Established/Verified channel and declared exchanges.")
            return _publisher_channel
        except Exception as e:
            print(f"[Publisher] Failed to get publisher channel: {e}")
            # Clean up potentially bad connection/channel
            if _publisher_channel: 
                try: 
                    _publisher_channel.close()
                except Exception: 
                    pass
            if _publisher_connection: 
                try: 
                    _publisher_connection.close()
                except Exception: 
                    pass
            _publisher_channel = None
            _publisher_connection = None
            return None

# --- Publisher Functions ---
def publish_trip_completed(config, trip_id, batch_id, completion_status="Completed"):
    """Publishes a message indicating a trip has been completed to WAREHOUSE_OPS_EXCHANGE."""
    publisher_channel = get_rabbitmq_publisher_channel(config)
    if not publisher_channel:
        print("[Publisher] Cannot publish trip completed: No publisher channel.")
        return

    message = {
        "event_type": "trip_completed",
        "trip_order_id": trip_id,
        "batch_id": batch_id,
        "status": completion_status,
        "completed_at": datetime.now(timezone.utc).isoformat()
    }
    message_body = json.dumps(message)

    try:
        publisher_channel.basic_publish(
            exchange=config["warehouse_ops_exchange"],
            routing_key=config["trip_completed_routing_key"],
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2, # persistent
                content_type='application/json'
            )
        )
        print(f"[Publisher] Published trip completed message for Trip ID: {trip_id} to exchange {config['warehouse_ops_exchange']}")
    except Exception as e:
        print(f"[Publisher] Error publishing trip completed message: {e}")

def publish_vehicle_arrived(config, trip_id, batch_id, vehicle_id, warehouse_id):
    """Publishes a message indicating a vehicle has arrived for pickup to WAREHOUSE_OPS_EXCHANGE."""
    publisher_channel = get_rabbitmq_publisher_channel(config)
    if not publisher_channel:
        print("[Publisher] Cannot publish vehicle arrived: No publisher channel.")
        return

    message = {
        "event_type": "vehicle_arrived_for_pickup",
        "trip_order_id": trip_id,
        "batch_id": batch_id,
        "vehicle_id": vehicle_id,
        "warehouse_id": warehouse_id,
        "arrived_at": datetime.now(timezone.utc).isoformat()
    }
    message_body = json.dumps(message)

    try:
        publisher_channel.basic_publish(
            exchange=config["warehouse_ops_exchange"], # Publish to warehouse ops exchange
            routing_key=config["vehicle_arrived_routing_key"],
            body=message_body,
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
        )
        print(f"[Publisher] Published Vehicle Arrived message for Trip: {trip_id}, Batch: {batch_id} to exchange {config['warehouse_ops_exchange']}")
    except Exception as e:
        print(f"[Publisher] Error publishing Vehicle Arrived message: {e}")

# Phase 11: Updated Assignment Request Publisher
def publish_assignment_request(config, trip_id, batch_id, vehicle_id, trip_details):
    """
    Publishes an assignment request to the VEHICLE_ASSIGNMENT_EXCHANGE for the IoT simulator.
    Phase 11: Infers warehouse IDs from coordinates in trip_details.
    """
    publisher_channel = get_rabbitmq_publisher_channel(config)
    if not publisher_channel:
        print("[Publisher] Cannot publish assignment request: No publisher channel.")
        return False

    # Phase 11: Get coords and infer IDs
    r = get_redis_connection(config) # Need redis connection here
    source_lat = trip_details.get("source_latitude")
    source_lon = trip_details.get("source_longitude")
    dest_lat = trip_details.get("destination_latitude")
    dest_lon = trip_details.get("destination_longitude")

    inferred_source_warehouse_id = None
    inferred_destination_warehouse_id = None

    if r:
        if source_lat is not None and source_lon is not None:
            inferred_source_warehouse_id = find_warehouse_by_coords(r, source_lat, source_lon)
        if dest_lat is not None and dest_lon is not None:
            inferred_destination_warehouse_id = find_warehouse_by_coords(r, dest_lat, dest_lon)
    else:
        print("[Publisher] Warning: Redis unavailable, cannot infer warehouse IDs for assignment request.")

    correlation_id = str(uuid.uuid4()) 

    message = {
        "event_type": "vehicle_assignment_request",
        "trip_order_id": trip_id,
        "batch_id": batch_id,
        "vehicle_id": vehicle_id,
        # Use inferred IDs, defaulting to None or a placeholder if inference failed
        "source_warehouse_id": inferred_source_warehouse_id or "INF_FAIL", 
        "destination_warehouse_id": inferred_destination_warehouse_id or "INF_FAIL",
        "request_timestamp": datetime.now(timezone.utc).isoformat()
    }
    message_body = json.dumps(message)

    try:
        publisher_channel.basic_publish(
            exchange=config["assignment_request_exchange"],
            routing_key=config["assignment_request_routing_key"],
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2, 
                content_type='application/json',
                correlation_id=correlation_id 
            )
        )
        print(f"[Publisher] Published assignment request for Trip: {trip_id}, Vehicle: {vehicle_id} (CorrID: {correlation_id}) to exchange {config['assignment_request_exchange']}")
        return True
    except Exception as e:
        print(f"[Publisher] Error publishing assignment request: {e}")
        return False

# --- End Publisher Functions ---

# --- Callbacks ---

# Phase 8: Modified Trip Update Callback
def trip_update_callback(channel, method, properties, body, config):
    r = get_redis_connection(config)
    if not r:
        print("[Trip Consumer] No Redis connection, cannot process trip update. NACKing.")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True) # Requeue if no Redis
        return

    processed_successfully = False # Track if processing was successful before ack/nack
    try:
        # Handle body potentially being bytes or str
        if isinstance(body, bytes):
            message_json = body.decode('utf-8')
        elif isinstance(body, str):
            message_json = body # Already a string
        else:
            # Unexpected type, log and try to proceed cautiously or raise
            print(f"[Trip Consumer] Warning: Unexpected body type {type(body)}. Attempting str conversion.")
            message_json = str(body)

        message = json.loads(message_json)
        print(f"[Trip Consumer] Received Trip Update (Tag: {method.delivery_tag}):")
        # print(json.dumps(message, indent=2)) # Verbose

        trip_id = message.get("trip_order_id")
        trip_status = message.get("status")

        if trip_id and trip_status in ["Created", "Pending Assignment"]:
            print(f"[Trip Consumer] Trip {trip_id} needs assignment (Status: {trip_status}). Processing...")
            
            # 1. Add to pending assignment set (Phase 8)
            if not add_trip_to_pending_assignment(r, trip_id, message_json):
                 print(f"[Trip Consumer] Failed to add {trip_id} to Redis pending set. Assignment may fail.")
                 # Continue processing, maybe updating status is still useful
            
            # 2. Optionally update status/timestamp in Redis hash
            # (planned_route is now set by dispatch_service)
            trip_key = f"{TRIP_ORDER_KEY_PREFIX}{trip_id}"
            try:
                # Phase 16 Fix: Check key type before HSET to prevent WRONGTYPE
                key_type = r.type(trip_key) # Already a string due to decode_responses=True

                if key_type == 'none' or key_type == 'hash':
                    # Type is OK (or key doesn't exist), proceed with HSET
                    # Update status to ensure it reflects 'Pending Assignment'
                    # Update timestamp
                    update_mapping = {
                        "status": "Pending Assignment", 
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }
                    updated_count = r.hset(trip_key, mapping=update_mapping) # Use hset mapping
                    if updated_count > 0:
                         print(f"[Trip Consumer] Updated status/timestamp for TripOrder {trip_id} in Redis.")
                    # else: key might not exist yet, HSET creates it. Or no fields were changed.

                else: # Key exists but has the wrong type
                    print(f"[Trip Consumer] Warning: Expected key '{trip_key}' to be a Hash or non-existent, but found type '{key_type}'. Skipping status/timestamp update.")

            except redis.exceptions.RedisError as e:
                # This catch block might still be hit for other Redis errors (e.g., connection)
                # Or if r.type() itself failed (less likely but possible)
                print(f"[Trip Consumer] Redis error interacting with key '{trip_key}' for Trip {trip_id}: {e}")

            processed_successfully = True # Mark as processed if we got this far
        else:
            print(f"[Trip Consumer] Ignoring Trip {trip_id} with status: {trip_status}.")
            processed_successfully = True # Ack non-assignable trips too

    except json.JSONDecodeError as e:
        print(f"[Trip Consumer] Error decoding JSON: {e}. Acking.")
        processed_successfully = True # Ack malformed message
    except Exception as e:
        print(f"[Trip Consumer] Unexpected error processing trip message {method.delivery_tag}: {e}")
        # Let finally block handle NACK

    finally:
        # Final Ack/Nack based on flag
        try:
            if processed_successfully:
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                print(f"[Trip Consumer] Attempting NACK for message {method.delivery_tag}.")
                # NACK without requeue for unexpected errors to avoid poison messages
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                print(f"[Trip Consumer] NACKed message {method.delivery_tag} (no requeue).")
        except Exception as channel_err:
             print(f"[Trip Consumer] Error during final Ack/Nack: {channel_err}")


# Phase 8: Updated Location Callback
def vehicle_location_callback(channel, method, properties, body, config): # Added config
    r = get_redis_connection(config)
    # Don't NACK/requeue location updates if Redis is down, just log it.
    # Location updates are frequent; losing some isn't critical for this simulation.

    try:
        message = json.loads(body.decode('utf-8'))
        # print(f"[Location Consumer] Received Location Update (Tag: {method.delivery_tag}):") # Too verbose
        # print(json.dumps(message, indent=2))

        vehicle_id = message.get("vehicle_id")
        trip_id = message.get("trip_order_id") # Now included by IoT sim
        status = message.get("status")       # Now included by IoT sim

        if vehicle_id:
            if r:
                update_vehicle_location_redis(r, vehicle_id, message)
                # Location updated in Redis
                # Any further logic based on location/status?
                # e.g., check if vehicle reached destination? Complex. Skip for now.
            else:
                 print(f"[Location Consumer] No Redis connection, cannot store location for {vehicle_id}.")
        else:
            print("[Location Consumer] Received location update with missing vehicle_id.")

        # Always ACK location updates unless decoding fails.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        # print(f"[Location Consumer] Acknowledged Location message {method.delivery_tag}") # Verbose

    except json.JSONDecodeError as e:
        print(f"[Location Consumer] Error decoding JSON: {e}. Acking.")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Location Consumer] Unexpected error processing location message {method.delivery_tag}: {e}")
        # Still ACK to prevent message buildup for non-critical location updates
        try: channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception: pass


# Phase 6/8: Updated Batch Ready Callback
def batch_ready_callback(channel, method, properties, body, config):
    # This callback now needs to verify that a vehicle has been APPROVED for the trip
    # before simulating its arrival.
    r = get_redis_connection(config)
    if not r:
        print("[Batch Ready Consumer] No Redis connection, cannot process. NACKing.")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    try:
        message = json.loads(body.decode('utf-8'))
        print(f"[Batch Ready Consumer] Received Batch Ready (Tag: {method.delivery_tag}):")
        print(json.dumps(message, indent=2))

        batch_id = message.get("batch_id")
        warehouse_id = message.get("warehouse_id")
        trip_id = message.get("trip_order_id") # Dispatch should include this

        if not batch_id or not warehouse_id or not trip_id:
            print("[Batch Ready Consumer] Invalid message: missing batch_id, warehouse_id, or trip_id. Acking.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Phase 8 Change: Look up the *approved* assignment for this trip_id
        assigned_vehicle_id = None
        assign_cursor = '0'
        while assign_cursor != 0:
            assign_cursor, assign_keys = r.scan(assign_cursor, match=f"{VEHICLE_ASSIGNMENT_KEY_PREFIX}*", count=100)
            for assign_key in assign_keys:
                assign_data = r.hgetall(assign_key)
                # Check for matching trip_id AND status 'approved'
                if assign_data.get('trip_order_id') == trip_id and assign_data.get('approval_status') == 'approved':
                    assigned_vehicle_id = assign_data.get('vehicle_id')
                    break
            if assigned_vehicle_id: break

        if assigned_vehicle_id:
             print(f"[Batch Ready Consumer] Found APPROVED assignment: Vehicle {assigned_vehicle_id} for Trip {trip_id}.")
             # Simulate vehicle travelling and arriving (or assume message means vehicle is ready nearby)
             print(f"[Batch Ready Consumer] Simulating vehicle {assigned_vehicle_id} arrival at Warehouse {warehouse_id} for Batch {batch_id} pickup...")
             time.sleep(random.uniform(2, 6)) # Simulate travel/docking time

             # Publish Vehicle Arrived message (Phase 6)
             publish_vehicle_arrived(config, trip_id, batch_id, assigned_vehicle_id, warehouse_id)
        else:
             print(f"[Batch Ready Consumer] No approved assignment found for Trip {trip_id}. Cannot simulate vehicle arrival.")
             # If no approved assignment exists yet (or was rejected), this message shouldn't proceed.
             # NACK without requeue to prevent loops. The assignment worker might assign later.
             channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
             print(f"[Batch Ready Consumer] NACKed Batch Ready message {method.delivery_tag} (no requeue) - Waiting for approved assignment.")
             return # Exit before ACK

        # Acknowledge the message ONLY if an approved vehicle was found and processed
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[Batch Ready Consumer] Acknowledged Batch Ready message {method.delivery_tag}")

    except json.JSONDecodeError as e:
        print(f"[Batch Ready Consumer] Error decoding JSON: {e}. Acking.")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except redis.exceptions.RedisError as e:
        print(f"[Batch Ready Consumer] Redis error processing batch ready message {method.delivery_tag}: {e}. NACKing (requeue)." )
        try: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True) # Requeue on Redis error
        except Exception: pass
    except Exception as e:
        print(f"[Batch Ready Consumer] Unexpected error processing batch ready message {method.delivery_tag}: {e}")
        try:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            print(f"[Batch Ready Consumer] NACKed message {method.delivery_tag} (no requeue).")
        except Exception as channel_err:
             print(f"[Batch Ready Consumer] Error NACKing message: {channel_err}")


# --- End Callbacks ---

# --- Consumer Thread Functions ---
def run_consumer(config, queue_context, callback_func):
    """Generic function to run a blocking consumer in a loop."""
    consumer_connection = None
    consumer_channel = None # Keep track of channel too
    queue_name = f"UNKNOWN_QUEUE ({queue_context})" # Placeholder
    while True:
        try:
            # Establish connection and channel if needed
            if consumer_connection is None or consumer_connection.is_closed:
                print(f"[{queue_context} Consumer] Connecting to RabbitMQ...")
                consumer_connection = get_rabbitmq_connection(config)
                consumer_channel = consumer_connection.channel()
                print(f"[{queue_context} Consumer] Connected.")
                # Declare topology specific to this consumer
                queue_name = declare_rabbitmq_topology(consumer_channel, config, queue_context)
                consumer_channel.basic_qos(prefetch_count=1) # Process one message at a time

                # Ensure callback has config (using lambda)
                on_message_callback = lambda ch, method, properties, body: callback_func(ch, method, properties, body, config)

                consumer_channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=on_message_callback
                )
                print(f"[{queue_context} Consumer] Started consuming from queue: '{queue_name}'. Waiting for messages...")
            
            # Start consuming (blocks here until connection/channel issue or stop)
            if consumer_channel: # Ensure channel exists before consuming
                consumer_channel.start_consuming()
            else:
                # This case should ideally not be reached if connection logic is correct
                print(f"[{queue_context} Consumer] Error: Channel is None before starting consumption. Retrying connection.")
                time.sleep(5)
                continue # Skip to next loop iteration to reconnect

        # --- Exception Handling --- 
        except pika.exceptions.ConnectionClosedByBroker:
            print(f"[{queue_context} Consumer] Connection closed by broker. Retrying in 5 seconds...")
            consumer_connection = None
            consumer_channel = None
            time.sleep(5)
        except pika.exceptions.AMQPChannelError as err:
            print(f"[{queue_context} Consumer] Caught a channel error: {err}. Reconnecting...")
            if consumer_connection and not consumer_connection.is_closed:
                try:
                    consumer_connection.close()
                except Exception as close_err:
                    print(f"[{queue_context} Consumer] Error closing connection after channel error: {close_err}") 
            consumer_connection = None
            consumer_channel = None
            time.sleep(5)
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[{queue_context} Consumer] Connection error: {e}. Retrying in 5 seconds...")
            if consumer_connection and not consumer_connection.is_closed:
                try:
                    consumer_connection.close()
                except Exception as close_err:
                    print(f"[{queue_context} Consumer] Error closing connection after connection error: {close_err}") 
            consumer_connection = None
            consumer_channel = None
            time.sleep(5)
        except KeyboardInterrupt:
            print(f"[{queue_context} Consumer] Interrupted. Stopping consumer...")
            if consumer_connection and not consumer_connection.is_closed:
                 try:
                     consumer_connection.close()
                 except Exception as close_err:
                     print(f"[{queue_context} Consumer] Error closing connection during interrupt: {close_err}") 
            break # Exit the while loop
        except Exception as e:
            print(f"[{queue_context} Consumer] Unexpected error in consumer loop: {type(e).__name__}: {e}. Restarting consumer in 10s.")
            if consumer_connection and not consumer_connection.is_closed:
                 try:
                     consumer_connection.close()
                 except Exception as close_err:
                     print(f"[{queue_context} Consumer] Error closing connection after unexpected error: {close_err}") 
            consumer_connection = None
            consumer_channel = None
            time.sleep(10) # Ensure sleep is present

    print(f"[{queue_context} Consumer] Consumer loop finished.")


# --- Assignment Worker Thread (Phase 8) ---
def assignment_worker(config):
    """Background thread to process trips needing vehicle assignment."""
    print("[AssignWorker] Starting assignment worker thread...")
    r = get_redis_connection(config)
    interval = config.get("assignment_retry_interval_seconds", 30)

    while True:
        if not r:
            print("[AssignWorker] No Redis connection. Worker sleeping.")
            time.sleep(interval)
            r = get_redis_connection(config) # Try reconnecting
            continue

        try:
            # Get a batch of trips needing assignment
            pending_trip_ids = get_pending_assignment_trips(r, count=5) # Process up to 5 at a time

            if not pending_trip_ids:
                # print("[AssignWorker] No trips pending assignment. Sleeping...") # Can be verbose
                pass
            else:
                print(f"[AssignWorker] Found {len(pending_trip_ids)} trips pending assignment: {pending_trip_ids}")
                for trip_id in pending_trip_ids:
                    print(f"[AssignWorker] Processing Trip: {trip_id}")
                    # Get trip details stored when it was added
                    trip_details = get_trip_pending_details(r, trip_id)
                    if not trip_details:
                        print(f"[AssignWorker] CRITICAL: Could not retrieve details for pending trip {trip_id}. Removing from set.")
                        remove_trip_from_pending_assignment(r, trip_id) # Clean up bad entry
                        continue

                    batch_id = trip_details.get("batch_id", "UNKNOWN_BATCH")

                    # 1. Find Suitable Vehicle
                    selected_vehicle_id = find_suitable_vehicle_redis(r, trip_details)

                    if selected_vehicle_id:
                        # --- Get selected vehicle's current location for route calculation ---
                        vehicle_lat = None
                        vehicle_lon = None
                        try:
                            vehicle_key = f"{VEHICLE_KEY_PREFIX}{selected_vehicle_id}"
                            vehicle_data = r.hgetall(vehicle_key)
                            vehicle_lat = vehicle_data.get('current_latitude')
                            vehicle_lon = vehicle_data.get('current_longitude')
                            if not vehicle_lat or not vehicle_lon:
                                print(f"[AssignWorker] Warning: Could not get current location for selected vehicle {selected_vehicle_id}. Cannot calculate route_to_source.")
                        except redis.exceptions.RedisError as e_fetch:
                            print(f"[AssignWorker] Warning: Redis error fetching location for selected vehicle {selected_vehicle_id}: {e_fetch}. Cannot calculate route_to_source.")
                        except Exception as e_fetch_other:
                             print(f"[AssignWorker] Warning: Unexpected error fetching location for selected vehicle {selected_vehicle_id}: {e_fetch_other}. Cannot calculate route_to_source.")
                        # --- End Location Fetch ---

                        # 2. Create Pending Assignment in Redis (Now includes route calculation)
                        if create_vehicle_assignment_redis(r, trip_id, selected_vehicle_id, batch_id, trip_details, config, vehicle_lat, vehicle_lon):
                            # 3. Publish Assignment Request to IoT Simulator
                            if publish_assignment_request(config, trip_id, batch_id, selected_vehicle_id, trip_details):
                                print(f"[AssignWorker] Successfully requested assignment for Trip: {trip_id} to Vehicle: {selected_vehicle_id}.")
                                # 4. Remove from Pending Set (as request sent)
                                # Important: Removing here means if the response is lost or fails,
                                # the trip won't be automatically retried by this worker.
                                # The response handler needs to handle re-adding on rejection.
                                remove_trip_from_pending_assignment(r, trip_id)
                            else:
                                print(f"[AssignWorker] Failed to publish assignment request for Trip: {trip_id}. Leaving in pending set for retry.")
                                # Optional: Clean up the 'pending' assignment record created?
                                # For now, leave it; let response handler deal with potential inconsistency if needed.
                        else:
                             print(f"[AssignWorker] Failed to create Redis assignment record for Trip: {trip_id}. Leaving in pending set for retry.")
                    else:
                        # No suitable vehicle found this iteration, leave it in the pending set.
                        print(f"[AssignWorker] No suitable vehicle found for Trip: {trip_id} this cycle. Will retry later.")
                        # Optional: Add a counter to prevent infinite retries?

        except Exception as e:
            print(f"[AssignWorker] Unexpected error in worker loop: {e}")
            # Avoid continuous fast loops on error
            time.sleep(interval)

        # Wait before the next check
        print(f"[AssignWorker] Worker cycle complete. Sleeping for {interval} seconds...")
        time.sleep(interval)

    print("[AssignWorker] Assignment worker thread finished.") # Should not happen unless interrupted


# --- Main Function ---
def main():
    cfg = load_config()
    print("Starting Vehicle Management Service (Phase 11 - WH ID Inference)...") # Updated startup message

    r = get_redis_connection(cfg)
    if r:
        # Seed vehicles (Phase 10)
        seed_specific_vehicles_redis(r)
        # Phase 11: Seed warehouse coordinates for lookup
        warehouse_data = load_warehouse_data_from_json()
        if warehouse_data:
             seed_warehouses_redis(r, warehouse_data)
        else:
             print("CRITICAL: Failed to load warehouse data for coordinate lookup.")
    else:
        print("CRITICAL: Redis connection failed. Seeding and assignment worker will fail.")

    # --- Start Consumers in Threads ---
    consumer_threads = []
    # Updated list to include the new consumer
    consumer_types = ['trip_consumer', 'location_consumer', 'batch_ready_consumer', 'assignment_response_consumer'] 

    # Define callbacks mapping (adjust based on actual function names and required args)
    callback_map = {
        'trip_consumer': trip_update_callback,
        'location_consumer': vehicle_location_callback,
        'batch_ready_consumer': batch_ready_callback,
        'assignment_response_consumer': assignment_response_callback
    }

    for context in consumer_types:
        callback_func = callback_map.get(context)
        if callback_func:
            thread = threading.Thread(
                target=run_consumer, 
                args=(cfg, context, callback_func), 
                name=f"{context.replace('_consumer','').capitalize()}Consumer", 
                daemon=True
            )
            consumer_threads.append(thread)
            thread.start()
        else:
            print(f"Warning: No callback function defined for consumer context '{context}'")

    # --- Start Assignment Worker Thread (Phase 8) ---
    assignment_worker_thread = threading.Thread(
        target=assignment_worker,
        args=(cfg,),
        name="AssignmentWorker",
        daemon=True
    )
    assignment_worker_thread.start()

    print("All consumer threads and assignment worker started.")

    # Keep main thread alive to allow daemon threads to run
    try:
        # Check if ANY consumer thread OR the worker thread is alive
        while any(t.is_alive() for t in consumer_threads) or assignment_worker_thread.is_alive():
            # Check individual threads for logging/errors if needed
            all_threads = consumer_threads + [assignment_worker_thread]
            for t in all_threads:
                if not t.is_alive() and t.name != threading.main_thread().name:
                     print(f"ERROR: Thread {t.name} died unexpectedly. Monitoring others.")
                     # Optionally remove dead threads from check list or implement restart logic
            
            # Sleep for a bit before checking again
            time.sleep(5) 
    except KeyboardInterrupt:
        print("Main thread interrupted by user. Shutting down...")
        # Threads are daemons, they will exit automatically when the main thread exits.
        # Add explicit cleanup/shutdown signals here if necessary for graceful shutdown.
    except Exception as e:
        print(f"Main thread encountered unexpected error: {e}")

    print("Vehicle Management Service finished.")


# Phase 10: New function to seed specific vehicles
def seed_specific_vehicles_redis(r):
    """Seeds specific numbers of Pickup and Heavy Trucks per region into Redis."""
    if not r:
        print("[Redis Seed] Cannot seed vehicles: Redis client missing.")
        return False

    print("[Redis Seed - Phase 10] Seeding specific vehicle data...")
    total_seeded = 0
    regions = ["1", "2"] # Assuming two warehouse regions/IDs

    try:
        # Clear existing vehicles first? Optional, depends if we want additive or replacement seeding.
        # Let's clear for simplicity to ensure correct counts.
        existing_vehicle_keys = list(r.scan_iter(match=f"{VEHICLE_KEY_PREFIX}*", count=500))
        if existing_vehicle_keys:
            print(f"[Redis Seed - Phase 10] Clearing {len(existing_vehicle_keys)} existing vehicle keys...")
            r.delete(*existing_vehicle_keys)

        vehicle_counter = 1 # Global counter for unique IDs/plates

        # 1. Seed Pickup Trucks (15 per region)
        for region_id in regions:
            for i in range(15):
                v_id = f"SIM_PICKUP_{region_id}_{i+1}"
                license_plate = f"PICKUP{vehicle_counter:03d}"
                vehicle_data = {
                    "vehicle_id": v_id,
                    "license_plate": license_plate,
                    "vehicle_name": f"Pickup Truck Region {region_id} #{i+1}",
                    "vehicle_type": "Pickup Truck",
                    "capacity_kg": 3500.0, # Example capacity
                    "capacity_volume": 10,
                    "load_type": "Rear Load",
                    "status": "Idle", # Initial status
                    "home_region": region_id,
                    "current_latitude": random.uniform(10.7, 10.9), # Random initial location
                    "current_longitude": random.uniform(106.6, 106.8),
                    "last_updated": datetime.now(timezone.utc).isoformat()
                }
                key = f"{VEHICLE_KEY_PREFIX}{v_id}"
                r.hset(key, mapping=vehicle_data)
                total_seeded += 1
                vehicle_counter += 1
            print(f"[Redis Seed - Phase 10] Seeded 15 Pickup Trucks for Region {region_id}")

        # 2. Seed Heavy Trucks (2 for region 1, 3 for region 2)
        heavy_truck_config = {"1": 2, "2": 3}
        for region_id, count in heavy_truck_config.items():
            for i in range(count):
                v_id = f"SIM_HEAVY_{region_id}_{i+1}"
                license_plate = f"HEAVY{vehicle_counter:03d}"
                vehicle_data = {
                    "vehicle_id": v_id,
                    "license_plate": license_plate,
                    "vehicle_name": f"Heavy Truck Region {region_id} #{i+1}",
                    "vehicle_type": "Heavy Truck",
                    "capacity_kg": 105000.0, # Example capacity
                    "capacity_volume": 300.0,
                    "load_type": "Rear Load",
                    "status": "Idle", # Initial status
                    "home_region": region_id,
                    "current_latitude": random.uniform(10.7, 10.9), # Random initial location
                    "current_longitude": random.uniform(106.6, 106.8),
                    "last_updated": datetime.now(timezone.utc).isoformat()
                }
                key = f"{VEHICLE_KEY_PREFIX}{v_id}"
                r.hset(key, mapping=vehicle_data)
                total_seeded += 1
                vehicle_counter += 1
            print(f"[Redis Seed - Phase 10] Seeded {count} Heavy Trucks for Region {region_id}")
        
        print(f"[Redis Seed - Phase 10] Successfully seeded a total of {total_seeded} vehicles.")
        return True

    except redis.exceptions.RedisError as e:
        print(f"[Redis Seed - Phase 10] Redis error during seeding: {e}")
        return False
    except Exception as e:
        print(f"[Redis Seed - Phase 10] Unexpected error during seeding: {e}")
        return False


def find_assignment_key(r, trip_id, vehicle_id):
    """Helper to find the assignment key based on trip and vehicle ID."""
    # Scan is inefficient but needed for simulation without better indexing
    try:
        cursor = '0'
        while cursor != 0:
            cursor, keys = r.scan(cursor=cursor, match=f"{VEHICLE_ASSIGNMENT_KEY_PREFIX}*", count=100)
            for key in keys:
                data = r.hgetall(key)
                if data.get("trip_order_id") == trip_id and data.get("vehicle_id") == vehicle_id:
                     return key # Found the assignment key
        return None # Not found
    except redis.exceptions.RedisError as e:
        print(f"[Redis] Error scanning for assignment key (Trip: {trip_id}, Vehicle: {vehicle_id}): {e}")
        return None
# --- End Helper ---

# Phase 8: New Callback for Assignment Responses
def assignment_response_callback(channel, method, properties, body, config):
    r = get_redis_connection(config)
    if not r:
        print("[Assign Response Consumer] No Redis connection, cannot process response. NACKing.")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    print(f"[Assign Response Consumer] Received Assignment Response (Tag: {method.delivery_tag}):")
    try:
        message = json.loads(body.decode('utf-8'))
        print(json.dumps(message, indent=2))

        trip_id = message.get("trip_order_id")
        vehicle_id = message.get("vehicle_id")
        approval_status = message.get("approval_status") # Should be "approved" or "rejected"

        if not trip_id or not vehicle_id or not approval_status:
            print("[Assign Response Consumer] Invalid response message. Acking.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[Assign Response Consumer] Processing response for Trip: {trip_id}, Vehicle: {vehicle_id}, Status: {approval_status}")

        # Update the assignment record in Redis first
        assignment_updated = update_vehicle_assignment_redis(r, trip_id, vehicle_id, approval_status)
        assignment_key = None # Initialize
        if assignment_updated:
            print(f"[Assign Response Consumer] Successfully updated Redis assignment status for Trip: {trip_id}.")
            # We need the key to update route_to_source later
            assignment_key = find_assignment_key(r, trip_id, vehicle_id)
            if not assignment_key:
                 print(f"[Assign Response Consumer] Warning: Could not find assignment key after update for Trip {trip_id}, Vehicle {vehicle_id}. Cannot set route_to_source.")
        else:
            print(f"[Assign Response Consumer] Warning: Failed to update Redis assignment status for Trip: {trip_id}.")
            # Cannot proceed if assignment status update failed

        if approval_status == "rejected":
            print(f"[Assign Response Consumer] Assignment REJECTED for Trip: {trip_id}, Vehicle: {vehicle_id}. Adding trip back to pending queue.")
            trip_details = get_trip_pending_details(r, trip_id)
            if trip_details:
                 trip_details_json = json.dumps(trip_details)
                 if not add_trip_to_pending_assignment(r, trip_id, trip_details_json):
                      print(f"[Assign Response Consumer] CRITICAL: Failed to re-add rejected Trip {trip_id} to pending queue!")
            else:
                 print(f"[Assign Response Consumer] CRITICAL: Cannot retrieve details for rejected Trip {trip_id}. Manual intervention likely required.")

        elif approval_status == "approved" and assignment_key:
            print(f"[Assign Response Consumer] Assignment APPROVED for Trip: {trip_id}, Vehicle: {vehicle_id}. Updating status.")

            # --- Phase 16 Logic --- 
            try:
                # 1. Update Assignment Status
                assignment_update_data = {
                    "approval_status": "Approved", # New status per Phase 16
                    "last_updated": datetime.now(timezone.utc).isoformat()
                }
                r.hset(assignment_key, mapping=assignment_update_data)
                print(f"[Assign Response Consumer] Updated assignment {assignment_key} status to 'En Route to Source'.")

                # 2. Update Trip Order Hash (Vehicle ID and Status)
                trip_key = f"{TRIP_ORDER_KEY_PREFIX}{trip_id}"
                trip_update_data = {
                    "vehicle_id": vehicle_id,
                    "status": "Assigned",
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
                r.hset(trip_key, mapping=trip_update_data)
                print(f"[Assign Response Consumer] Updated TripOrder {trip_key} with vehicle_id and status 'Assigned'.")

                # 3. Update Vehicle Status (Done in update_vehicle_assignment_redis)
            except redis.exceptions.RedisError as e:
                print(f"[Assign Response Consumer] Redis error during Phase 16 approved assignment status update: {e}")
            except Exception as e:
                print(f"[Assign Response Consumer] Unexpected error during Phase 16 approved assignment status update: {e}")
            # --- End Phase 16 --- 

        # Acknowledge the response message (Executed regardless of inner try/except outcome)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[Assign Response Consumer] Acknowledged response message {method.delivery_tag}")

    except json.JSONDecodeError as e:
        print(f"[Assign Response Consumer] Error decoding JSON: {e}. Acking.")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except redis.exceptions.RedisError as e:
        print(f"[Assign Response Consumer] Redis error processing assignment response {method.delivery_tag}: {e}. NACKing (requeue)." )
        try: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True) # Requeue on Redis error
        except Exception: pass
    except Exception as e:
        print(f"[Assign Response Consumer] Unexpected error processing response message {method.delivery_tag}: {e}")
        try:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            print(f"[Assign Response Consumer] NACKed message {method.delivery_tag} (no requeue).")
        except Exception as channel_err:
             print(f"[Assign Response Consumer] Error NACKing message: {channel_err}")


# --- End Callbacks ---

# --- Consumer Thread Functions ---
def run_consumer(config, queue_context, callback_func):
    """Generic function to run a blocking consumer in a loop."""
    consumer_connection = None
    consumer_channel = None # Keep track of channel too
    queue_name = f"UNKNOWN_QUEUE ({queue_context})" # Placeholder
    while True:
        try:
            # Establish connection and channel if needed
            if consumer_connection is None or consumer_connection.is_closed:
                print(f"[{queue_context} Consumer] Connecting to RabbitMQ...")
                consumer_connection = get_rabbitmq_connection(config)
                consumer_channel = consumer_connection.channel()
                print(f"[{queue_context} Consumer] Connected.")
                # Declare topology specific to this consumer
                queue_name = declare_rabbitmq_topology(consumer_channel, config, queue_context)
                consumer_channel.basic_qos(prefetch_count=1) # Process one message at a time

                # Ensure callback has config (using lambda)
                on_message_callback = lambda ch, method, properties, body: callback_func(ch, method, properties, body, config)

                consumer_channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=on_message_callback
                )
                print(f"[{queue_context} Consumer] Started consuming from queue: '{queue_name}'. Waiting for messages...")
            
            # Start consuming (blocks here until connection/channel issue or stop)
            if consumer_channel: # Ensure channel exists before consuming
                consumer_channel.start_consuming()
            else:
                # This case should ideally not be reached if connection logic is correct
                print(f"[{queue_context} Consumer] Error: Channel is None before starting consumption. Retrying connection.")
                time.sleep(5)
                continue # Skip to next loop iteration to reconnect

        # --- Exception Handling --- 
        except pika.exceptions.ConnectionClosedByBroker:
            print(f"[{queue_context} Consumer] Connection closed by broker. Retrying in 5 seconds...")
            consumer_connection = None
            consumer_channel = None
            time.sleep(5)
        except pika.exceptions.AMQPChannelError as err:
            print(f"[{queue_context} Consumer] Caught a channel error: {err}. Reconnecting...")
            if consumer_connection and not consumer_connection.is_closed:
                try:
                    consumer_connection.close()
                except Exception as close_err:
                    print(f"[{queue_context} Consumer] Error closing connection after channel error: {close_err}") 
            consumer_connection = None
            consumer_channel = None
            time.sleep(5)
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[{queue_context} Consumer] Connection error: {e}. Retrying in 5 seconds...")
            if consumer_connection and not consumer_connection.is_closed:
                try:
                    consumer_connection.close()
                except Exception as close_err:
                    print(f"[{queue_context} Consumer] Error closing connection after connection error: {close_err}") 
            consumer_connection = None
            consumer_channel = None
            time.sleep(5)
        except KeyboardInterrupt:
            print(f"[{queue_context} Consumer] Interrupted. Stopping consumer...")
            if consumer_connection and not consumer_connection.is_closed:
                 try:
                     consumer_connection.close()
                 except Exception as close_err:
                     print(f"[{queue_context} Consumer] Error closing connection during interrupt: {close_err}") 
            break # Exit the while loop
        except Exception as e:
            print(f"[{queue_context} Consumer] Unexpected error in consumer loop: {type(e).__name__}: {e}. Restarting consumer in 10s.")
            if consumer_connection and not consumer_connection.is_closed:
                 try:
                     consumer_connection.close()
                 except Exception as close_err:
                     print(f"[{queue_context} Consumer] Error closing connection after unexpected error: {close_err}") 
            consumer_connection = None
            consumer_channel = None
            time.sleep(10) # Ensure sleep is present

    print(f"[{queue_context} Consumer] Consumer loop finished.")


# --- Assignment Worker Thread (Phase 8) ---
def assignment_worker(config):
    """Background thread to process trips needing vehicle assignment."""
    print("[AssignWorker] Starting assignment worker thread...")
    r = get_redis_connection(config)
    interval = config.get("assignment_retry_interval_seconds", 30)

    while True:
        if not r:
            print("[AssignWorker] No Redis connection. Worker sleeping.")
            time.sleep(interval)
            r = get_redis_connection(config) # Try reconnecting
            continue

        try:
            # Get a batch of trips needing assignment
            pending_trip_ids = get_pending_assignment_trips(r, count=5) # Process up to 5 at a time

            if not pending_trip_ids:
                # print("[AssignWorker] No trips pending assignment. Sleeping...") # Can be verbose
                pass
            else:
                print(f"[AssignWorker] Found {len(pending_trip_ids)} trips pending assignment: {pending_trip_ids}")
                for trip_id in pending_trip_ids:
                    print(f"[AssignWorker] Processing Trip: {trip_id}")
                    # Get trip details stored when it was added
                    trip_details = get_trip_pending_details(r, trip_id)
                    if not trip_details:
                        print(f"[AssignWorker] CRITICAL: Could not retrieve details for pending trip {trip_id}. Removing from set.")
                        remove_trip_from_pending_assignment(r, trip_id) # Clean up bad entry
                        continue

                    batch_id = trip_details.get("batch_id", "UNKNOWN_BATCH")

                    # 1. Find Suitable Vehicle
                    selected_vehicle_id = find_suitable_vehicle_redis(r, trip_details)

                    if selected_vehicle_id:
                        # --- Get selected vehicle's current location for route calculation ---
                        vehicle_lat = None
                        vehicle_lon = None
                        try:
                            vehicle_key = f"{VEHICLE_KEY_PREFIX}{selected_vehicle_id}"
                            vehicle_data = r.hgetall(vehicle_key)
                            vehicle_lat = vehicle_data.get('current_latitude')
                            vehicle_lon = vehicle_data.get('current_longitude')
                            if not vehicle_lat or not vehicle_lon:
                                print(f"[AssignWorker] Warning: Could not get current location for selected vehicle {selected_vehicle_id}. Cannot calculate route_to_source.")
                        except redis.exceptions.RedisError as e_fetch:
                            print(f"[AssignWorker] Warning: Redis error fetching location for selected vehicle {selected_vehicle_id}: {e_fetch}. Cannot calculate route_to_source.")
                        except Exception as e_fetch_other:
                             print(f"[AssignWorker] Warning: Unexpected error fetching location for selected vehicle {selected_vehicle_id}: {e_fetch_other}. Cannot calculate route_to_source.")
                        # --- End Location Fetch ---

                        # 2. Create Pending Assignment in Redis (Now includes route calculation)
                        if create_vehicle_assignment_redis(r, trip_id, selected_vehicle_id, batch_id, trip_details, config, vehicle_lat, vehicle_lon):
                            # 3. Publish Assignment Request to IoT Simulator
                            if publish_assignment_request(config, trip_id, batch_id, selected_vehicle_id, trip_details):
                                print(f"[AssignWorker] Successfully requested assignment for Trip: {trip_id} to Vehicle: {selected_vehicle_id}.")
                                # 4. Remove from Pending Set (as request sent)
                                # Important: Removing here means if the response is lost or fails,
                                # the trip won't be automatically retried by this worker.
                                # The response handler needs to handle re-adding on rejection.
                                remove_trip_from_pending_assignment(r, trip_id)
                            else:
                                print(f"[AssignWorker] Failed to publish assignment request for Trip: {trip_id}. Leaving in pending set for retry.")
                                # Optional: Clean up the 'pending' assignment record created?
                                # For now, leave it; let response handler deal with potential inconsistency if needed.
                        else:
                             print(f"[AssignWorker] Failed to create Redis assignment record for Trip: {trip_id}. Leaving in pending set for retry.")
                    else:
                        # No suitable vehicle found this iteration, leave it in the pending set.
                        print(f"[AssignWorker] No suitable vehicle found for Trip: {trip_id} this cycle. Will retry later.")
                        # Optional: Add a counter to prevent infinite retries?

        except Exception as e:
            print(f"[AssignWorker] Unexpected error in worker loop: {e}")
            # Avoid continuous fast loops on error
            time.sleep(interval)

        # Wait before the next check
        print(f"[AssignWorker] Worker cycle complete. Sleeping for {interval} seconds...")
        time.sleep(interval)

    print("[AssignWorker] Assignment worker thread finished.") # Should not happen unless interrupted


# --- Main Function ---
def main():
    cfg = load_config()
    print("Starting Vehicle Management Service (Phase 11 - WH ID Inference)...") # Updated startup message

    r = get_redis_connection(cfg)
    if r:
        # Seed vehicles (Phase 10)
        seed_specific_vehicles_redis(r)
        # Phase 11: Seed warehouse coordinates for lookup
        warehouse_data = load_warehouse_data_from_json()
        if warehouse_data:
             seed_warehouses_redis(r, warehouse_data)
        else:
             print("CRITICAL: Failed to load warehouse data for coordinate lookup.")
    else:
        print("CRITICAL: Redis connection failed. Seeding and assignment worker will fail.")

    # --- Start Consumers in Threads ---
    consumer_threads = []
    # Updated list to include the new consumer
    consumer_types = ['trip_consumer', 'location_consumer', 'batch_ready_consumer', 'assignment_response_consumer'] 

    # Define callbacks mapping (adjust based on actual function names and required args)
    callback_map = {
        'trip_consumer': trip_update_callback,
        'location_consumer': vehicle_location_callback,
        'batch_ready_consumer': batch_ready_callback,
        'assignment_response_consumer': assignment_response_callback
    }

    for context in consumer_types:
        callback_func = callback_map.get(context)
        if callback_func:
            thread = threading.Thread(
                target=run_consumer, 
                args=(cfg, context, callback_func), 
                name=f"{context.replace('_consumer','').capitalize()}Consumer", 
                daemon=True
            )
            consumer_threads.append(thread)
            thread.start()
        else:
            print(f"Warning: No callback function defined for consumer context '{context}'")

    # --- Start Assignment Worker Thread (Phase 8) ---
    assignment_worker_thread = threading.Thread(
        target=assignment_worker,
        args=(cfg,),
        name="AssignmentWorker",
        daemon=True
    )
    assignment_worker_thread.start()

    print("All consumer threads and assignment worker started.")

    # Keep main thread alive to allow daemon threads to run
    try:
        # Check if ANY consumer thread OR the worker thread is alive
        while any(t.is_alive() for t in consumer_threads) or assignment_worker_thread.is_alive():
            # Check individual threads for logging/errors if needed
            all_threads = consumer_threads + [assignment_worker_thread]
            for t in all_threads:
                if not t.is_alive() and t.name != threading.main_thread().name:
                     print(f"ERROR: Thread {t.name} died unexpectedly. Monitoring others.")
                     # Optionally remove dead threads from check list or implement restart logic
            
            # Sleep for a bit before checking again
            time.sleep(5) 
    except KeyboardInterrupt:
        print("Main thread interrupted by user. Shutting down...")
        # Threads are daemons, they will exit automatically when the main thread exits.
        # Add explicit cleanup/shutdown signals here if necessary for graceful shutdown.
    except Exception as e:
        print(f"Main thread encountered unexpected error: {e}")

    print("Vehicle Management Service finished.")


if __name__ == "__main__":
    main() 