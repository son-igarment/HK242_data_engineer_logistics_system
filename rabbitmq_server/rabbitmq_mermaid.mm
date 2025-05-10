graph TD
    subgraph gen_order_service
        GOS[gen_order_service]
    end

    subgraph dispatch_service
        DS[dispatch_service]
        Q_ORDER(fa:fa-database ORDER_QUEUE)
    end

    subgraph vehicle_management
        VM[vehicle_management]
        Q_VM_TRIP(fa:fa-database VEHICLE_TRIP_QUEUE)
        Q_VM_LOC(fa:fa-database VEHICLE_LOCATION_QUEUE)
        Q_VM_BATCH(fa:fa-database BATCH_READY_QUEUE)
        Q_VM_ASSIGN_RESP(fa:fa-database VEHICLE_ASSIGNMENT_RESPONSE_QUEUE)
    end

    subgraph vehicle_iot_simulator
        VIOTS[vehicle_iot_simulator]
        Q_VIOTS_ASSIGN_REQ(fa:fa-database VEHICLE_ASSIGNMENT_REQUEST_QUEUE)
    end

    subgraph GPSRealtimeHandle
        GPS[GPSRealtimeHandle Backend]
        Q_GPS_LOC(fa:fa-database GPS_BACKEND_LOCATION_QUEUE)
    end

    subgraph RabbitMQ_Exchanges
        direction LR
        %% Use standard rectangle nodes for exchanges
        EX_ORDER["ORDER_EXCHANGE<br/>(direct)"]
        EX_TRIP_UPDATE["TRIP_UPDATE_EXCHANGE<br/>(topic)"]
        EX_LOC_UPDATE["VEHICLE_LOCATION_UPDATE_EXCHANGE<br/>(topic)"]
        EX_ASSIGN["VEHICLE_ASSIGNMENT_EXCHANGE<br/>(topic)"]
        EX_VEH_OPS["VEHICLE_OPS_EXCHANGE<br/>(topic)"]
        EX_WH_OPS["WAREHOUSE_OPS_EXCHANGE<br/>(topic)"]
    end


    GOS -- ORDER_ROUTING_KEY --> EX_ORDER
    DS -- TRIP_UPDATE_ROUTING_KEY --> EX_TRIP_UPDATE
    VIOTS -- VEHICLE_LOCATION_UPDATE_ROUTING_KEY --> EX_LOC_UPDATE
    VIOTS -- VEHICLE_ASSIGNMENT_RESPONSE_ROUTING_KEY --> EX_ASSIGN
    VM -- WAREHOUSE_TRIP_COMPLETED_ROUTING_KEY --> EX_WH_OPS
    VM -- VEHICLE_ARRIVED_ROUTING_KEY --> EX_WH_OPS
    VM -- VEHICLE_ASSIGNMENT_REQUEST_ROUTING_KEY --> EX_ASSIGN


    EX_ORDER -- ORDER_ROUTING_KEY --> Q_ORDER --> DS
    EX_TRIP_UPDATE -- TRIP_UPDATE_ROUTING_KEY --> Q_VM_TRIP --> VM
    EX_LOC_UPDATE -- VEHICLE_LOCATION_UPDATE_ROUTING_KEY --> Q_VM_LOC --> VM
    EX_LOC_UPDATE -- VEHICLE_LOCATION_UPDATE_ROUTING_KEY --> Q_GPS_LOC --> GPS
    EX_ASSIGN -- VEHICLE_ASSIGNMENT_REQUEST_ROUTING_KEY --> Q_VIOTS_ASSIGN_REQ --> VIOTS
    EX_ASSIGN -- VEHICLE_ASSIGNMENT_RESPONSE_ROUTING_KEY --> Q_VM_ASSIGN_RESP --> VM
    EX_VEH_OPS -- BATCH_READY_BINDING_KEY --> Q_VM_BATCH --> VM
    %% Note: Warehouse service bindings to EX_WH_OPS are assumed but not shown



    classDef service fill:#f9f,stroke:#333,stroke-width:2px;
    classDef exchange fill:#ccf,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5;
    classDef queue fill:#lightgrey,stroke:#333,stroke-width:1px;
    class GOS,DS,VM,VIOTS,GPS service;
    class EX_ORDER,EX_TRIP_UPDATE,EX_LOC_UPDATE,EX_ASSIGN,EX_VEH_OPS,EX_WH_OPS exchange;
    class Q_ORDER,Q_VM_TRIP,Q_VM_LOC,Q_VM_BATCH,Q_VM_ASSIGN_RESP,Q_VIOTS_ASSIGN_REQ,Q_GPS_LOC queue;
