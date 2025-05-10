import time
import json
import random
import pika
from faker import Faker
import uuid

# Assuming config is importable from parent directory
import config

fake = Faker('vi_VN')

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
    # Declare the exchange
    channel.exchange_declare(exchange=config.ORDER_EXCHANGE, exchange_type='direct', durable=True)
    # Declare the queue
    channel.queue_declare(queue=config.ORDER_QUEUE, durable=True)
    # Bind the queue to the exchange
    channel.queue_bind(queue=config.ORDER_QUEUE, exchange=config.ORDER_EXCHANGE, routing_key=config.ORDER_ROUTING_KEY)

def generate_mock_address(lat, lon):
    # In DEV_MODE, we don't call Vietmap API
    # We can use Faker or just return a mock string
    return f"Mock Address near ({lat:.4f}, {lon:.4f})"

def generate_point_near_warehouse(warehouse_lat, warehouse_lon):
    # Simple random offset for mock generation
    lat_offset = random.uniform(-0.05, 0.05) # Adjust range as needed
    lon_offset = random.uniform(-0.05, 0.05)
    return warehouse_lat + lat_offset, warehouse_lon + lon_offset

def generate_order():
    order_code = str(uuid.uuid4())
    
    # Simplified logic for point generation (ignoring ratios for now)
    # Randomly pick start and end warehouse association
    start_near_wh1 = random.choice([True, False])
    end_near_wh1 = random.choice([True, False])
    
    start_lat, start_lon = generate_point_near_warehouse(
        config.WAREHOUSE_1_LAT if start_near_wh1 else config.WAREHOUSE_2_LAT,
        config.WAREHOUSE_1_LON if start_near_wh1 else config.WAREHOUSE_2_LON
    )
    end_lat, end_lon = generate_point_near_warehouse(
        config.WAREHOUSE_1_LAT if end_near_wh1 else config.WAREHOUSE_2_LAT,
        config.WAREHOUSE_1_LON if end_near_wh1 else config.WAREHOUSE_2_LON
    )

    start_address = generate_mock_address(start_lat, start_lon)
    end_address = generate_mock_address(end_lat, end_lon)

    weight_kg = random.uniform(1500, 3000) # 1.5 to 3 tons
    volume_m3 = (weight_kg / 1000) * (8/3) if weight_kg >= 1500 else (weight_kg / 1000) * (4/1.5) # Proportional volume based on weight range

    order_data = {
        "order_code": order_code,
        "start_address": start_address,
        "start_location_latitude": start_lat,
        "start_location_longitude": start_lon,
        "end_address": end_address,
        "end_location_latitude": end_lat,
        "end_location_longitude": end_lon,
        "user_name": fake.name(),
        "user_phone": fake.phone_number(),
        "receiver_name": fake.name(),
        "receiver_phone": fake.phone_number(),
        "note": fake.sentence(),
        "item_description": random.choice(["Gói hàng nhỏ", "Gói hàng lớn"]),
        "weight_kg": round(weight_kg, 2),
        "volume_m3": round(volume_m3, 2)
    }
    return order_data

def main():
    print("Starting Gen Order Service (Realtime - Dev Mode)...")
    
    if not config.DEV_MODE:
        print("Warning: Running in non-DEV mode, but this script uses mock data.")
        # Add logic here to switch to production data sources if needed
        # For now, it will still use mocks based on the implementation

    # Ensure warehouse coordinates are set in .env or config defaults
    if config.WAREHOUSE_1_LAT == 0.0 or config.WAREHOUSE_2_LAT == 0.0:
        print("Error: Warehouse coordinates not set in config. Please update .env.example and rename to .env")
        # You might want to load from datawarehouse.json here as a fallback in dev
        try:
            with open('./datawarehouse.json', 'r') as f:
                warehouses = json.load(f)
                # Access lat/lon directly from the loaded structure
                config.WAREHOUSE_1_LAT = float(warehouses[0]['latitude'])
                config.WAREHOUSE_1_LON = float(warehouses[0]['longitude'])
                config.WAREHOUSE_2_LAT = float(warehouses[1]['latitude'])
                config.WAREHOUSE_2_LON = float(warehouses[1]['longitude'])
                print("Loaded warehouse coordinates from datawarehouse.json")
        except Exception as e:
            print(f"Error loading warehouse data from JSON: {e}")
            return # Exit if coordinates aren't available

    connection = None
    while True:
        try:
            if connection is None or connection.is_closed:
                print("Connecting to RabbitMQ...")
                connection = get_rabbitmq_connection()
                channel = connection.channel()
                declare_rabbitmq_topology(channel)
                print("Connected to RabbitMQ.")

            order = generate_order()
            message_body = json.dumps(order)

            channel.basic_publish(
                exchange=config.ORDER_EXCHANGE,
                routing_key=config.ORDER_ROUTING_KEY,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode = 2, # make message persistent
                )
            )
            print(f"Published order {order['order_code']}")

        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection error: {e}. Retrying in 5 seconds...")
            if connection and not connection.is_closed:
                connection.close()
            connection = None
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            # Consider adding more specific error handling or logging
            time.sleep(config.ORDER_GENERATION_INTERVAL_MINUTES * 60)
            continue # Continue the loop even if there's a non-connection error

        print(f"Waiting for {config.ORDER_GENERATION_INTERVAL_MINUTES} minutes...")
        time.sleep(config.ORDER_GENERATION_INTERVAL_MINUTES * 60)

if __name__ == "__main__":
    main() 