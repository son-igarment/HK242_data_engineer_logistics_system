# BTL Logistics Services (Phase 1-3)

This project implements a logistics system involving order generation, dispatch, warehouse management, vehicle management, and simulated vehicle IoT updates. Services communicate via RabbitMQ, with data storage potentially in Redis.

## Prerequisites

Before you begin, ensure you have the following installed:

* **Docker & Docker Compose:** To run the infrastructure services (Redis, RabbitMQ). Installation guides: [Docker](https://docs.docker.com/get-docker/), [Docker Compose](https://docs.docker.com/compose/install/).
* **uv:** A fast Python package installer and resolver. Installation guide: [uv](https://github.com/astral-sh/uv#installation).
* **Python:** Version 3.8 or higher is recommended.

## Setup Instructions

1. **Clone the Repository:**

   ```bash
   git clone <your-repository-url>
   cd <your-repository-directory>
   ```
2. **Create `.env` Files for Each Service:**

   * Each service (`gen_order_service`, `dispatch_service`, `warehouse_service`, `vehicle_management`, `vehicle_iot_simulator`) loads its configuration from a `.env` file within its respective directory. Create these files.
   * **Dispatch Service (`dispatch_service/.env`):**

     ```bash
     touch dispatch_service/.env
     ```

     Edit `dispatch_service/.env` and add necessary variables, for example:
     ```dotenv
     # dispatch_service/.env Example
     DEV_MODE=True
     RABBITMQ_HOST=localhost
     RABBITMQ_PORT=5672
     RABBITMQ_USER=user # Or guest if not set in docker-compose
     RABBITMQ_PASS=password # Or guest if not set in docker-compose
     ORDER_EXCHANGE=order_exchange
     ORDER_QUEUE=order_queue
     ORDER_ROUTING_KEY=new_order
     TRIP_UPDATE_EXCHANGE=trip_update_exchange
     TRIP_UPDATE_QUEUE=trip_update_queue
     TRIP_UPDATE_ROUTING_KEY=trip_status
     REDIS_HOST=localhost
     REDIS_PORT=6379
     REDIS_DB=0
     # Add POSTGRES variables if/when needed for production mode
     ```
   * **Order Generation Service (`gen_order_service/.env`):**

     ```bash
     touch gen_order_service/.env
     ```

     Edit `gen_order_service/.env` and add necessary variables, for example:
     ```dotenv
     # gen_order_service/.env Example
     DEV_MODE=True
     RABBITMQ_HOST=localhost
     RABBITMQ_PORT=5672
     RABBITMQ_USER=user # Or guest if not set in docker-compose
     RABBITMQ_PASS=password # Or guest if not set in docker-compose
     ORDER_EXCHANGE=order_exchange
     ORDER_QUEUE=order_queue
     ORDER_ROUTING_KEY=new_order
     ORDER_GENERATION_INTERVAL_MINUTES=5
     WAREHOUSE_1_LAT=... # Set actual coordinates
     WAREHOUSE_1_LON=... # Set actual coordinates
     WAREHOUSE_2_LAT=... # Set actual coordinates
     WAREHOUSE_2_LON=... # Set actual coordinates
     VIETMAP_API_KEY=YOUR_API_KEY # Optional for Dev Mode
     ```
   * **Warehouse Service (`warehouse_service/.env`):**

     ```bash
     touch warehouse_service/.env
     ```

     Edit `warehouse_service/.env` and add necessary variables, for example:
     ```dotenv
     # warehouse_service/.env Example
     DEV_MODE=True
     RABBITMQ_HOST=localhost
     RABBITMQ_PORT=5672
     RABBITMQ_USER=user # Or guest if not set in docker-compose
     RABBITMQ_PASS=password # Or guest if not set in docker-compose
     TRIP_UPDATE_EXCHANGE=trip_update_exchange
     TRIP_UPDATE_ROUTING_KEY=trip_status # Key to listen for
     WAREHOUSE_SERVICE_QUEUE=warehouse_trip_updates # Unique queue name
     ```
   * **Vehicle Management Service (`vehicle_management/.env`):**

     ```bash
     touch vehicle_management/.env
     ```

     Edit `vehicle_management/.env` and add necessary variables, ensuring keys for *both* trip and location updates are present:
     ```dotenv
     # vehicle_management/.env Example
     DEV_MODE=True
     RABBITMQ_HOST=localhost
     RABBITMQ_PORT=5672
     RABBITMQ_USER=user # Or guest if not set in docker-compose
     RABBITMQ_PASS=password # Or guest if not set in docker-compose
     # Trip Updates
     TRIP_UPDATE_EXCHANGE=trip_update_exchange
     TRIP_UPDATE_ROUTING_KEY=trip_status 
     VEHICLE_TRIP_QUEUE=vehicle_trip_updates
     # Location Updates
     VEHICLE_UPDATE_EXCHANGE=vehicle_updates_exchange 
     VEHICLE_UPDATE_BINDING_KEY=vehicle.location.update
     VEHICLE_LOCATION_QUEUE=vehicle_location_updates
     ```
   * **Vehicle IoT Simulator (`vehicle_iot_simulator/.env`):**

     ```bash
     touch vehicle_iot_simulator/.env
     ```

     Edit `vehicle_iot_simulator/.env` and add necessary variables:
     ```dotenv
     # vehicle_iot_simulator/.env Example
     DEV_MODE=True
     RABBITMQ_HOST=localhost
     RABBITMQ_PORT=5672
     RABBITMQ_USER=user # Or guest if not set in docker-compose
     RABBITMQ_PASS=password # Or guest if not set in docker-compose
     VEHICLE_UPDATE_EXCHANGE=vehicle_updates_exchange
     VEHICLE_UPDATE_ROUTING_KEY=vehicle.location.update
     UPDATE_INTERVAL_SECONDS=10
     NUM_SIMULATED_VEHICLES=5
     ```
   * **Note:** Ensure RabbitMQ user/pass match `docker-compose.yml` if set.
3. **Install Python Dependencies using `uv`:**

   * It's recommended to use a virtual environment (create one in the project root):
     ```bash
     # Create a virtual environment (optional but recommended)
     python -m venv .venv 
     source .venv/bin/activate # On Windows use `.venv\Scripts\activate`
     ```
   * Install dependencies using `uv` from the project root:
     ```bash
     uv pip install -r requirements.txt
     ```

## Running the Application

1. **Start Infrastructure Services:**

   * Make sure Docker Desktop is running.
   * In your **project root directory**, run:
     ```bash
     docker-compose up -d
     ```
   * This will start Redis, RabbitMQ, and Redis Commander in the background.

2. **Setup RabbitMQ Topology (Optional but Recommended):**

   * Navigate to the `rabbitmq_server` directory:
     ```bash
     cd rabbitmq_server
     ```
   * Run the setup script using `uv` (ensure your virtual environment is active if used):
     ```bash
     # Make sure your .venv is active first if you created one in the root
     # e.g., source ../.venv/bin/activate
     uv run python setup_topology.py 
     ```
   * Navigate back to the project root:
     ```bash
     cd ..
     ```
   * *Note: This script needs access to configuration, ensure its internal logic correctly finds the necessary `.env` files or connection details.* 

3. **Start Python Services:**

   * Open **five separate terminals**.
   * In each terminal, **navigate to the respective service directory** (and ensure your virtual environment, if created in the root, is activated).

   * **Terminal 1 (Dispatch Service):**
     ```bash
     cd dispatch_service
     # Activate venv: source ../.venv/bin/activate (or equivalent)
     uv run python main.py
     ```
   * **Terminal 2 (Order Generation Service):**
     ```bash
     cd gen_order_service
     # Activate venv: source ../.venv/bin/activate (or equivalent)
     uv run python main.py
     ```
   * **Terminal 3 (Warehouse Service):**
     ```bash
     cd warehouse_service
     # Activate venv: source ../.venv/bin/activate (or equivalent)
     uv run python main.py
     ```
   * **Terminal 4 (Vehicle Management Service):**
     ```bash
     cd vehicle_management
     # Activate venv: source ../.venv/bin/activate (or equivalent)
     uv run python main.py
     ```
   * **Terminal 5 (Vehicle IoT Simulator):**
     ```bash
     cd vehicle_iot_simulator
     # Activate venv: source ../.venv/bin/activate (or equivalent)
     uv run python main.py
     ```

## Accessing Dashboards

* **RabbitMQ Management UI:** `http://localhost:15672`
  * Login with the username/password configured in `docker-compose.yml` (default: `guest`/`guest` if not overridden).
* **Redis Commander UI:** `http://localhost:8081`
  * Allows you to browse the data stored in Redis.

## Monitoring Messages (Optional)

* The monitor script might need adaptation for configuration loading and potentially updates to listen to all relevant exchanges.
* Assuming it exists as `monitor.py` in `rabbitmq_server`:
  1. Open a new terminal.
  2. Navigate to the `rabbitmq_server` directory:
     ```bash
     cd rabbitmq_server
     ```
  3. Run the monitor script using `uv` (activate venv first):
     ```bash
     # Activate venv: source ../.venv/bin/activate (or equivalent)
     uv run python monitor.py
     ```
  4. Press `Ctrl+C` to stop monitoring.
  5. Navigate back to the root:
     ```bash
     cd ..
     ```

## Stopping the Application

1. **Stop Python Services:** Press `Ctrl+C` in the terminals where the services are running.
2. **Stop Docker Containers:** (Run from project root)
   ```bash
   docker-compose down
   ```
   * Add `-v` if you also want to remove the Docker volumes: `docker-compose down -v`

## Project Structure (Approximate)

```
. (Project Root)
├── .gitignore           # Git ignore patterns
├── .venv/               # Python virtual environment (Optional)
├── __pycache__/         # Python bytecode cache (Ignored by Git)
├── dispatch_service/    # Service for receiving and processing orders
│   ├── .env             # Local environment variables for this service
│   ├── __init__.py
│   ├── config.py        # Service-specific config loading (if exists)
│   └── main.py
├── docker-compose.yml   # Defines Docker services (Redis, RabbitMQ, etc.)
├── gen_order_service/   # Service for generating orders
│   ├── .env             # Local environment variables for this service
│   ├── __init__.py
│   ├── config.py        # Service-specific config loading (if exists)
│   └── main.py
├── genesisData/         # Directory for initial/mock data files
│   ├── datawarehouse.json # (Example location)
│   └── vehicle.json       # (Example location)
├── phase1.txt           # Initial requirements document (if still present)
├── phase2.txt           # Phase 2 requirements document
├── phase3.txt           # Phase 3 requirements document
├── rabbitmq_server/     # Utilities for RabbitMQ (May need config adjustment)
│   ├── __init__.py
│   ├── monitor.py       # Script to monitor RabbitMQ messages
│   └── setup_topology.py # Script to setup RabbitMQ exchanges/queues
├── README.md            # This file
├── requirements.txt     # Python dependencies
├── warehouse_service/   # Service for warehouse management based on trips
│   ├── .env             # Local environment variables for this service
│   ├── __init__.py
│   └── main.py
├── vehicle_iot_simulator/ # Simulates IoT device updates
│   ├── .env             # Local environment variables for this service
│   ├── __init__.py
│   └── main.py
├── vehicle_management/  # Service for vehicle management based on trips
│   ├── .env             # Local environment variables for this service
│   ├── __init__.py
│   └── main.py
└── ...                  # Other project files (e.g., db.sql, vietmapapi.txt if present)
```
