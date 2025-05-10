import os
from dotenv import load_dotenv

load_dotenv() # Load variables from .env file

DEV_MODE = os.getenv('DEV_MODE', 'True').lower() == 'true'

# RabbitMQ Config
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS')
ORDER_EXCHANGE = os.getenv('ORDER_EXCHANGE', 'order_exchange')
ORDER_QUEUE = os.getenv('ORDER_QUEUE', 'order_queue')
ORDER_ROUTING_KEY = os.getenv('ORDER_ROUTING_KEY', 'new_order')
TRIP_UPDATE_EXCHANGE = os.getenv('TRIP_UPDATE_EXCHANGE', 'trip_update_exchange')
TRIP_UPDATE_QUEUE = os.getenv('TRIP_UPDATE_QUEUE', 'trip_update_queue')
TRIP_UPDATE_ROUTING_KEY = os.getenv('TRIP_UPDATE_ROUTING_KEY', 'trip_status')

# Service Gen Order Config
ORDER_GENERATION_INTERVAL_MINUTES = int(os.getenv('ORDER_GENERATION_INTERVAL_MINUTES', 5))
WAREHOUSE_1_LAT = float(os.getenv('WAREHOUSE_1_LAT', 0.0)) # Placeholder, update in .env
WAREHOUSE_1_LON = float(os.getenv('WAREHOUSE_1_LON', 0.0)) # Placeholder, update in .env
WAREHOUSE_2_LAT = float(os.getenv('WAREHOUSE_2_LAT', 0.0)) # Placeholder, update in .env
WAREHOUSE_2_LON = float(os.getenv('WAREHOUSE_2_LON', 0.0)) # Placeholder, update in .env
VIETMAP_API_KEY = os.getenv('VIETMAP_API_KEY')

# Dispatch and Operation Order Config
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# Database Config (Production)
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

DATABASE_URL = None
if not DEV_MODE and all([POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
    DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}" 