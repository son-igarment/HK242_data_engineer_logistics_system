
# RabbitMQ Message Flow Documentation

This document describes how services within the BTL Logistics project communicate using RabbitMQ message queuing. It outlines the exchanges, queues, routing keys, and the producers/consumers involved in each message flow.

## Core RabbitMQ Concepts Used

*   **Exchange:** Receives messages from producers and routes them to one or more queues based on type and routing keys/bindings.
    *   `direct`: Routes messages to queues whose binding key exactly matches the message's routing key.
    *   `topic`: Routes messages to queues based on pattern matching between the routing key and the queue's binding key (using wildcards like `*` and `#`).
*   **Queue:** A buffer that stores messages until they are consumed by a service. Queues are bound to exchanges.
*   **Routing Key:** A label attached to a message by the producer. The exchange uses this key to decide how to route the message.
*   **Binding Key:** A pattern used when binding a queue to an exchange. It determines which messages (based on their routing key) should be delivered to that queue.
*   **Producer:** A service that sends (publishes) messages to an exchange.
*   **Consumer:** A service that subscribes to a queue and receives (consumes) messages from it.

## Exchanges Overview

The system primarily uses the following exchanges:

1.  `order_exchange` (Type: `direct`): Used for routing new orders.
2.  `trip_update_exchange` (Type: `topic`): Used for broadcasting trip status updates.
3.  `vehicle_updates_exchange` (Type: `topic`): Used for vehicle IoT status/location updates.

*(These names are configurable via `.env` files but represent the defaults used in the code.)*

## Detailed Message Flows

### Flow 1: New Order Creation

*   **Purpose:** To send newly generated orders from the generation service to the dispatch service for processing.
*   **Producer:** `gen_order_service`
*   **Exchange:** `order_exchange` (direct)
*   **Routing Key Used:** `new_order` (Configured via `ORDER_ROUTING_KEY` in `gen_order_service/.env`)
*   **Consumer:** `dispatch_service`
*   **Queue:** `order_queue` (Configured via `ORDER_QUEUE` in `dispatch_service/.env`)
*   **Binding:** `order_queue` is bound to `order_exchange` with the binding key `new_order`.
*   **Diagram:**
    ```
    [gen_order_service] --(routing_key='new_order')--> [order_exchange (direct)] --(binding='new_order')--> [order_queue] --> [dispatch_service]
    ```

### Flow 2: Trip Status Updates

*   **Purpose:** To notify interested services (warehouse, vehicle management) about updates related to trip orders created by the dispatch service.
*   **Producer:** `dispatch_service`
*   **Exchange:** `trip_update_exchange` (topic)
*   **Routing Key Used:** `trip_status` (Configured via `TRIP_UPDATE_ROUTING_KEY` in `dispatch_service/.env`)
*   **Consumers:**
    *   `warehouse_service`
    *   `vehicle_management`
*   **Queues:**
    *   `warehouse_trip_updates` (Configured via `WAREHOUSE_SERVICE_QUEUE` in `warehouse_service/.env`)
    *   `vehicle_trip_updates` (Configured via `VEHICLE_TRIP_QUEUE` in `vehicle_management/.env`)
    *   *(Note: `rabbitmq_server/setup_topology.py` also declares a queue named `trip_update_queue` bound with this key, potentially for general monitoring or other future use.)*
*   **Bindings:**
    *   `warehouse_trip_updates` is bound to `trip_update_exchange` with binding key `trip_status` (from `warehouse_service/.env` `TRIP_UPDATE_ROUTING_KEY`).
    *   `vehicle_trip_updates` is bound to `trip_update_exchange` with binding key `trip_status` (from `vehicle_management/.env` `TRIP_UPDATE_ROUTING_KEY`).
*   **Diagram:**
    ```
                                                       +--(binding='trip_status')--> [warehouse_trip_updates] --> [warehouse_service]
                                                       |
    [dispatch_service] --(routing_key='trip_status')--> [trip_update_exchange (topic)] --+
                                                       |
                                                       +--(binding='trip_status')--> [vehicle_trip_updates] --> [vehicle_management (Consumer 1)]
                                                       |
                                                       +--(binding='trip_status')--> [trip_update_queue] --> (Optional Monitor/Other)

    ```

### Flow 3: Vehicle Location/Status Updates

*   **Purpose:** To send simulated real-time updates about vehicle status (location, speed, etc.) from IoT devices to the vehicle management service.
*   **Producer:** `vehicle_iot_simulator`
*   **Exchange:** `vehicle_updates_exchange` (topic)
*   **Routing Key Used:** `vehicle.location.update` (Configured via `VEHICLE_UPDATE_ROUTING_KEY` in `vehicle_iot_simulator/.env`)
*   **Consumer:** `vehicle_management`
*   **Queue:** `vehicle_location_updates` (Configured via `VEHICLE_LOCATION_QUEUE` in `vehicle_management/.env`)
*   **Binding:** `vehicle_location_updates` is bound to `vehicle_updates_exchange` with binding key `vehicle.location.update` (from `vehicle_management/.env` `VEHICLE_UPDATE_BINDING_KEY`).
*   **Diagram:**
    ```
    [vehicle_iot_simulator] --(routing_key='vehicle.location.update')--> [vehicle_updates_exchange (topic)] --(binding='vehicle.location.update')--> [vehicle_location_updates] --> [vehicle_management (Consumer 2)]
    ```

## Summary Table

| Exchange                   | Type   | Producer(s)             | Routing Key(s) Used        | Consumer(s)         | Bound Queue(s)               | Binding Key(s)             |
| :------------------------- | :----- | :---------------------- | :------------------------- | :------------------ | :--------------------------- | :------------------------- |
| `order_exchange`           | direct | `gen_order_service`     | `new_order`                | `dispatch_service`  | `order_queue`                | `new_order`                |
| `trip_update_exchange`     | topic  | `dispatch_service`      | `trip_status`              | `warehouse_service` | `warehouse_trip_updates`     | `trip_status`              |
|                            |        |                         |                            | `vehicle_management`  | `vehicle_trip_updates`       | `trip_status`              |
|                            |        |                         |                            | *(Monitor/Other)*   | `trip_update_queue`          | `trip_status`              |
| `vehicle_updates_exchange` | topic  | `vehicle_iot_simulator` | `vehicle.location.update`  | `vehicle_management`  | `vehicle_location_updates` | `vehicle.location.update`  |

## Configuration Notes

*   The specific names for exchanges, queues, and routing/binding keys are defined in the `.env` file within each respective service's directory.
*   The `rabbitmq_server/setup_topology.py` script can be run to declare the exchanges and some primary queues/bindings beforehand, ensuring they exist when services start. Services also typically declare the topology elements they need upon connection for robustness.
