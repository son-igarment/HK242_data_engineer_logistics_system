import pika
import sys
import os

# Add project root to path to import config
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

try:
    import config
except ModuleNotFoundError:
    print("Error: Could not import config.py. Make sure it exists in the project root.")
    sys.exit(1)


def setup_rabbitmq():
    connection = None
    try:
        print("Connecting to RabbitMQ...")
        credentials = None
        if config.RABBITMQ_USER and config.RABBITMQ_PASS:
            credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
        
        parameters = pika.ConnectionParameters(
            host=config.RABBITMQ_HOST,
            port=config.RABBITMQ_PORT,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        print("Connected.")

        # Declare Order Exchange and Queue
        print(f"Declaring exchange: {config.ORDER_EXCHANGE} (direct)")
        channel.exchange_declare(exchange=config.ORDER_EXCHANGE, exchange_type='direct', durable=True)
        
        print(f"Declaring queue: {config.ORDER_QUEUE} (durable)")
        channel.queue_declare(queue=config.ORDER_QUEUE, durable=True)
        
        print(f"Binding queue {config.ORDER_QUEUE} to exchange {config.ORDER_EXCHANGE} with key {config.ORDER_ROUTING_KEY}")
        channel.queue_bind(queue=config.ORDER_QUEUE, exchange=config.ORDER_EXCHANGE, routing_key=config.ORDER_ROUTING_KEY)

        # Declare Trip Update Exchange and Queue
        print(f"Declaring exchange: {config.TRIP_UPDATE_EXCHANGE} (topic)")
        channel.exchange_declare(exchange=config.TRIP_UPDATE_EXCHANGE, exchange_type='topic', durable=True)
        
        print(f"Declaring queue: {config.TRIP_UPDATE_QUEUE} (durable)")
        channel.queue_declare(queue=config.TRIP_UPDATE_QUEUE, durable=True)

        # Note: Using TRIP_UPDATE_ROUTING_KEY for binding here. 
        # If consumers need more specific topic patterns (e.g., trip_status.#, trip_status.completed), 
        # they should declare their own queues and bindings.
        # This binding ensures messages sent with the exact key are routed to this default queue.
        print(f"Binding queue {config.TRIP_UPDATE_QUEUE} to exchange {config.TRIP_UPDATE_EXCHANGE} with key {config.TRIP_UPDATE_ROUTING_KEY}")
        channel.queue_bind(queue=config.TRIP_UPDATE_QUEUE, exchange=config.TRIP_UPDATE_EXCHANGE, routing_key=config.TRIP_UPDATE_ROUTING_KEY)

        # Also load vehicle update exchange name if defined
        vehicle_update_exchange_name = os.getenv('VEHICLE_UPDATE_EXCHANGE', 'vehicle_updates_exchange')
        # Load new exchange name (Phase 5)
        warehouse_ops_exchange_name = os.getenv('WAREHOUSE_OPS_EXCHANGE', 'warehouse_ops_exchange')
        # Load new exchange name (Phase 6)
        vehicle_ops_exchange_name = os.getenv('VEHICLE_OPS_EXCHANGE', 'vehicle_ops_exchange')

        # Declare NEW Vehicle Update Exchange
        print(f"Declaring exchange: {vehicle_update_exchange_name} (topic)")
        channel.exchange_declare(exchange=vehicle_update_exchange_name, exchange_type='topic', durable=True)

        # Declare NEW Warehouse Ops Exchange (Phase 5)
        print(f"Declaring exchange: {warehouse_ops_exchange_name} (topic)")
        channel.exchange_declare(exchange=warehouse_ops_exchange_name, exchange_type='topic', durable=True)
        # Note: Consumers (warehouse_service) declare their own queues for this.

        # Declare NEW Vehicle Ops Exchange (Phase 6)
        print(f"Declaring exchange: {vehicle_ops_exchange_name} (topic)")
        channel.exchange_declare(exchange=vehicle_ops_exchange_name, exchange_type='topic', durable=True)
        # Note: Consumers (vehicle_management) declare their own queues for this.

        print("\nRabbitMQ topology setup complete:")
        print(f"  - Order Exchange: {config.ORDER_EXCHANGE} (direct)")
        print(f"  - Order Queue: {config.ORDER_QUEUE} (bound with key: {config.ORDER_ROUTING_KEY})")
        print(f"  - Trip Update Exchange: {config.TRIP_UPDATE_EXCHANGE} (topic)")
        print(f"  - Trip Update Queue: {config.TRIP_UPDATE_QUEUE} (bound with key: {config.TRIP_UPDATE_ROUTING_KEY})")
        print(f"  - Vehicle Update Exchange: {vehicle_update_exchange_name} (topic)")
        print(f"  - Warehouse Ops Exchange: {warehouse_ops_exchange_name} (topic)")
        print(f"  - Vehicle Ops Exchange: {vehicle_ops_exchange_name} (topic)")


    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error connecting to RabbitMQ at {config.RABBITMQ_HOST}:{config.RABBITMQ_PORT}: {e}")
        print("Please ensure RabbitMQ is running (e.g., via 'docker-compose up') and accessible.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if connection and not connection.is_closed:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    # Ensure .env is loaded relative to the script's location needing the root config
    from dotenv import load_dotenv
    dotenv_path = os.path.join(project_root, '.env')
    if os.path.exists(dotenv_path):
         load_dotenv(dotenv_path=dotenv_path)
         # We need to reload the config module *after* loading .env
         import importlib
         importlib.reload(config)
         print(f"Loaded environment variables from: {dotenv_path}")
    else:
        print("Warning: .env file not found in project root. Using default config values.")
        
    setup_rabbitmq() 