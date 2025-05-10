-- Drop tables in reverse order of dependency (or use CASCADE)
-- Using CASCADE for simplicity here, but be careful in production.
DROP TABLE IF EXISTS VehicleAssignments CASCADE;
DROP TABLE IF EXISTS BatchOrderItems CASCADE;
DROP TABLE IF EXISTS OrderStatusHistory CASCADE;
DROP TABLE IF EXISTS VehicleStatusHistory CASCADE;
DROP TABLE IF EXISTS WarehouseStatusHistory CASCADE;
DROP TABLE IF EXISTS WarehouseStorage CASCADE;
DROP TABLE IF EXISTS TripOrders CASCADE;
DROP TABLE IF EXISTS OrderBatches CASCADE;
DROP TABLE IF EXISTS Orders CASCADE;
DROP TABLE IF EXISTS Vehicles CASCADE;
DROP TABLE IF EXISTS Warehouses CASCADE;

-- Warehouses Table (Base information)
CREATE TABLE Warehouses (
    warehouse_id SERIAL PRIMARY KEY, -- Unique ID for the warehouse
    warehouse_name VARCHAR(255) NOT NULL, -- Warehouse name
    warehouse_address TEXT NOT NULL, -- Detailed address of the warehouse
    region VARCHAR(50), -- Added missing region column
    is_regional_hub BOOLEAN DEFAULT FALSE, -- Added missing hub column, assuming default is False
    latitude DECIMAL(9, 6), -- Latitude
    longitude DECIMAL(9, 6), -- Longitude
    total_capacity_volume DECIMAL(12, 3) NOT NULL CHECK (total_capacity_volume > 0), -- Total capacity by volume (m³)
    num_loading_bays INT NOT NULL CHECK (num_loading_bays >= 0), -- Number of loading bays/docks
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE Warehouses IS 'Stores static information about warehouses. Dynamic status is in WarehouseStatusHistory.';
COMMENT ON COLUMN Warehouses.is_regional_hub IS 'Indicates if the warehouse primarily functions as a regional transit hub (True) or a local/spoke warehouse (False).';
COMMENT ON COLUMN Warehouses.total_capacity_volume IS 'Unit: cubic meters (m³). Represents the fixed total capacity.';
COMMENT ON COLUMN Warehouses.region IS 'Geographical region of the warehouse.';

-- Warehouse Status History Table
CREATE TABLE WarehouseStatusHistory (
    history_id SERIAL PRIMARY KEY,
    warehouse_id INT NOT NULL REFERENCES Warehouses(warehouse_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When this status record was created / became effective
    available_capacity_volume DECIMAL(12, 3) NOT NULL CHECK (available_capacity_volume >= 0), -- Available capacity at this time
    available_loading_bays INT NOT NULL CHECK (available_loading_bays >= 0), -- Available bays at this time
    notes TEXT NULL, -- Optional notes about the status change
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE WarehouseStatusHistory IS 'Tracks the historical status (availability) of warehouses over time.';
COMMENT ON COLUMN WarehouseStatusHistory.status_timestamp IS 'Timestamp when the recorded status became effective.';

-- Vehicles Table (Base information)
CREATE TABLE Vehicles (
    vehicle_id SERIAL PRIMARY KEY, -- Unique ID for the vehicle
    license_plate VARCHAR(20) UNIQUE NOT NULL, -- Vehicle license plate
    vehicle_name VARCHAR(255) NOT NULL, -- Vehicle name
    capacity_kg DECIMAL(10, 2) CHECK (capacity_kg > 0), -- Capacity by weight (kg)
    capacity_volume DECIMAL(10, 3) CHECK (capacity_volume > 0), -- Capacity by volume (m³)
    load_type VARCHAR(20) CHECK (load_type IN ('Side Load', 'Rear Load')), -- Loading type
    vehicle_type VARCHAR(50) NOT NULL CHECK (vehicle_type IN ('Warehouse-Warehouse', 'Customer-Warehouse')), -- Vehicle transport type
    home_region VARCHAR(50), -- Region where the vehicle usually operates/parks
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE Vehicles IS 'Stores static information about transport vehicles. Dynamic status and location are in VehicleStatusHistory.';

-- TripOrders Table (Transport commands)
CREATE TABLE TripOrders (
    trip_order_id SERIAL PRIMARY KEY, -- Unique ID for the transport command
    source_address TEXT NOT NULL, -- Starting point address of the trip
    source_latitude DECIMAL(9, 6),
    source_longitude DECIMAL(9, 6),
    destination_address TEXT NOT NULL, -- Ending point address of the trip
    destination_latitude DECIMAL(9, 6),
    destination_longitude DECIMAL(9, 6),
    trip_type VARCHAR(50) NOT NULL CHECK (trip_type IN ('Pickup', 'Local Transfer', 'Delivery', 'Return')), -- Type of trip
    trip_order_status VARCHAR(50) NOT NULL DEFAULT 'Pending Assignment' CHECK (trip_order_status IN ('Pending Assignment', 'Assigned', 'In Progress', 'Completed', 'Cancelled')), -- Status of the trip itself
    arrive_time_to_source TIMESTAMP WITH TIME ZONE, -- Time when the vehicle arrives at the source address
    wait_time_at_source INT, -- Wait time at the source address
    dock_time_at_source INT, -- Dock time at the source address
    estimated_arrival_at_destination TIMESTAMP WITH TIME ZONE, -- ETA at the destination address
    end_time TIMESTAMP WITH TIME ZONE, -- Time when the trip was completed
    planned_route TEXT, -- Route description
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE TripOrders IS 'Stores information about transport commands (trips). Details which orders/batches are included can be derived from OrderStatusHistory and BatchOrderItems.';

-- Orders Table (Base information)
CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY, -- Unique ID for the order
    order_code VARCHAR(50) UNIQUE, -- Order code for reference
    source_address TEXT NOT NULL,
    source_latitude DECIMAL(9, 6),
    source_longitude DECIMAL(9, 6),
    destination_address TEXT NOT NULL,
    destination_latitude DECIMAL(9, 6),
    destination_longitude DECIMAL(9, 6),
    item_description TEXT, -- Basic description of the goods
    weight_kg DECIMAL(10, 2), -- Estimated/actual weight
    volume_m3 DECIMAL(10, 3), -- Estimated/actual volume
    estimated_arrival_at_destination TIMESTAMP WITH TIME ZONE, -- ETA at the destination address
    order_status VARCHAR(50) NOT NULL DEFAULT 'Open' CHECK (order_status IN ('Open', 'In progress', 'Ready for Pickup', 'In Delivery', 'Completed', 'Cancelled', 'Returned')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE Orders IS 'Stores static information about individual orders. Dynamic status and location are in OrderStatusHistory.';

-- Warehouse Storage Table
CREATE TABLE WarehouseStorage (
    storage_id SERIAL PRIMARY KEY,
    warehouse_id INT NOT NULL REFERENCES Warehouses(warehouse_id) ON DELETE CASCADE ON UPDATE CASCADE,
    order_id INT NOT NULL REFERENCES Orders(order_id) ON DELETE CASCADE ON UPDATE CASCADE,
    input_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    output_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    weight_kg DECIMAL(10, 2) NOT NULL CHECK (weight_kg > 0),
    volume_m3 DECIMAL(10, 3) NOT NULL CHECK (volume_m3 > 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_warehouse_storage_warehouse_id_order_id ON WarehouseStorage(warehouse_id, order_id);

-- OrderBatches Table (Grouping mechanism)
CREATE TABLE OrderBatches (
    batch_id SERIAL PRIMARY KEY, -- Unique ID for the batch
    batch_code VARCHAR(50) UNIQUE, -- Readable batch code
    batch_status VARCHAR(50) NOT NULL DEFAULT 'Open' CHECK (batch_status IN ('Open', 'Ready for Pickup', 'In Transit', 'Completed', 'Cancelled')), -- Status of the batch itself
    location_type VARCHAR(20) NULL CHECK (location_type IN ('Warehouse', 'Vehicle')), -- Type of location associated with the status
    location_id INT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE OrderBatches IS 'Represents a grouping of orders. Orders are linked via BatchOrderItems and their history via OrderStatusHistory.';
COMMENT ON COLUMN OrderBatches.batch_status IS 'Status of the batch itself: Open (adding orders), Ready for Pickup, In Transit, Completed, Cancelled.';

-- Vehicle Status History Table
CREATE TABLE VehicleStatusHistory (
    history_id SERIAL PRIMARY KEY,
    batch_id INT NOT NULL REFERENCES OrderBatches(batch_id) ON DELETE CASCADE ON UPDATE CASCADE,
    vehicle_id INT NOT NULL REFERENCES Vehicles(vehicle_id) ON DELETE CASCADE ON UPDATE CASCADE,
    trip_order_id INT NULL REFERENCES TripOrders(trip_order_id) ON DELETE SET NULL ON UPDATE CASCADE, -- Reference to the TripOrder during this status
    vehicle_history_status VARCHAR(50) NOT NULL CHECK (vehicle_history_status IN ('Offline', 'Idle', 'Assigned', 'En Route to Pickup', 'Loading', 'Delivering', 'Unloading', 'Returning', 'Maintenance')), -- Vehicle status at this time
    latitude DECIMAL(9, 6) NULL, -- Location latitude at this time
    longitude DECIMAL(9, 6) NULL, -- Location longitude at this time
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- When this status/location was recorded
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE VehicleStatusHistory IS 'Tracks the historical status and location of vehicles over time.';
COMMENT ON COLUMN VehicleStatusHistory.created_at IS 'Timestamp when the recorded status/location became effective.';
COMMENT ON COLUMN VehicleStatusHistory.vehicle_history_status IS 'Detailed vehicle status, e.g., Offline, Idle, Assigned, En Route to Pickup, Loading, Delivering, Unloading, Returning, Maintenance.';
COMMENT ON COLUMN VehicleStatusHistory.trip_order_id IS 'Reference to the associated TripOrder during this status, if applicable.';

-- Order Status History Table
CREATE TABLE OrderStatusHistory (
    history_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES Orders(order_id) ON DELETE CASCADE ON UPDATE CASCADE,
    trip_order_id INT NULL REFERENCES TripOrders(trip_order_id) ON DELETE SET NULL ON UPDATE CASCADE, -- Reference to the TripOrder when status is related to a specific trip
    location_id INT NULL, -- ID of the Warehouse, OrderBatch, or Vehicle (maps to respective table PKs), NULL otherwise
    location_type VARCHAR(20) NULL CHECK (location_type IN ('Warehouse', 'OrderBatch', 'Vehicle', 'Customer', 'Source')), -- Type of location associated with the status
    order_history_status VARCHAR(50) NOT NULL, -- Order status, e.g., Created, ReceivedAtWarehouse, Batched, AssignedToTrip, OutForDelivery, Delivered, AttemptFailed, Cancelled
    notes TEXT NULL, -- Optional context for the status change
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE OrderStatusHistory IS 'Tracks the historical status and location changes of orders over time.';
COMMENT ON COLUMN OrderStatusHistory.created_at IS 'Timestamp when the recorded status became effective.';
COMMENT ON COLUMN OrderStatusHistory.order_history_status IS 'E.g., Created, ReceivedAtWarehouse, Batched, AssignedToTrip, OutForDelivery, Delivered, AttemptFailed, Cancelled';
COMMENT ON COLUMN OrderStatusHistory.location_type IS 'Indicates the type of location related to the status (Warehouse, OrderBatch, Vehicle, Customer, Source).';
COMMENT ON COLUMN OrderStatusHistory.location_id IS 'Refers to the specific ID of the Warehouse, OrderBatch, or Vehicle if location_type is set.';
COMMENT ON COLUMN OrderStatusHistory.trip_order_id IS 'Reference to the associated TripOrder, relevant for statuses like AssignedToTrip, OutForDelivery.';

-- Junction table BatchOrderItems (Direct link for current orders in a batch)
CREATE TABLE BatchOrderItems (
    batch_id INT NOT NULL REFERENCES OrderBatches(batch_id) ON DELETE CASCADE ON UPDATE CASCADE,
    order_id INT NOT NULL REFERENCES Orders(order_id) ON DELETE CASCADE ON UPDATE CASCADE,
    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- Track when the order was added to this batch instance
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_id, order_id) -- An order is in a batch only once
);

COMMENT ON TABLE BatchOrderItems IS 'Directly lists which Orders are currently assigned to OrderBatches. Provides a snapshot view complementary to OrderStatusHistory.';
COMMENT ON COLUMN BatchOrderItems.added_at IS 'Timestamp when the order was added to this specific batch instance.';

-- VehicleAssignments Table (Linking Vehicles to TripOrders)
CREATE TABLE VehicleAssignments (
    assignment_id SERIAL PRIMARY KEY, -- Unique ID for the vehicle assignment instance
    batch_id INT NOT NULL REFERENCES OrderBatches(batch_id) ON DELETE CASCADE ON UPDATE CASCADE,
    vehicle_id INT NOT NULL REFERENCES Vehicles(vehicle_id) ON DELETE RESTRICT ON UPDATE CASCADE, -- Prevent deleting a Vehicle if it has assignments
    trip_order_id INT NOT NULL REFERENCES TripOrders(trip_order_id) ON DELETE CASCADE ON UPDATE CASCADE,
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    approval_status VARCHAR(20) NOT NULL DEFAULT 'Pending' CHECK (approval_status IN ('Pending', 'Approved', 'Rejected')), -- Driver's approval status
    estimated_arrival_at_source TIMESTAMP WITH TIME ZONE, -- ETA at the source address
    route_to_source TEXT, -- Route to the source address encoded polyline
    notes TEXT, -- Additional notes
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE VehicleAssignments IS 'Stores information about assigning a specific vehicle to a trip order and the driver response.';


-- === Indexes ===
-- Indexes speed up data retrieval. Primary Keys are automatically indexed.

-- Warehouses
CREATE INDEX idx_warehouses_region ON Warehouses(region);
CREATE INDEX idx_warehouses_is_regional_hub ON Warehouses(is_regional_hub);

-- WarehouseStatusHistory
CREATE INDEX idx_warehouse_status_history_latest ON WarehouseStatusHistory (warehouse_id, status_timestamp DESC);
CREATE INDEX idx_warehouse_status_history_fk ON WarehouseStatusHistory(warehouse_id); -- For FK joins

-- Vehicles
CREATE INDEX idx_vehicles_region ON Vehicles(home_region);

-- VehicleStatusHistory
CREATE INDEX idx_vehicle_status_history_latest ON VehicleStatusHistory (vehicle_id, created_at DESC);
CREATE INDEX idx_vehicle_status_history_fk ON VehicleStatusHistory(vehicle_id); -- For FK joins
CREATE INDEX idx_vehicle_status_history_trip_fk ON VehicleStatusHistory(trip_order_id); -- For FK joins

-- TripOrders
CREATE INDEX idx_triporders_type ON TripOrders(trip_type);
CREATE INDEX idx_triporders_status ON TripOrders(trip_order_status);

-- OrderBatches
CREATE INDEX idx_orderbatches_status ON OrderBatches(batch_status);

-- Orders
CREATE INDEX idx_orders_created_at ON Orders(created_at);

-- OrderStatusHistory
CREATE INDEX idx_order_status_history_latest ON OrderStatusHistory (order_id, created_at DESC);
CREATE INDEX idx_order_status_history_fk ON OrderStatusHistory(order_id); -- For FK joins
CREATE INDEX idx_order_status_history_trip_fk ON OrderStatusHistory(trip_order_id); -- For FK joins

-- BatchOrderItems
CREATE INDEX idx_batchorderitems_batch_id ON BatchOrderItems(batch_id); -- For lookups by batch

-- VehicleAssignments
CREATE INDEX idx_vehicleassignments_trip_order_id ON VehicleAssignments(trip_order_id);
CREATE INDEX idx_vehicleassignments_vehicle_id ON VehicleAssignments(vehicle_id);
CREATE INDEX idx_vehicleassignments_approval_status ON VehicleAssignments(approval_status);