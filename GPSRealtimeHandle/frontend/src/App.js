import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup, Polyline } from 'react-leaflet';
import L from 'leaflet';
import axios from 'axios';
import 'leaflet/dist/leaflet.css';

// Fix for default marker icons in Leaflet
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: require('leaflet/dist/images/marker-icon-2x.png'),
  iconUrl: require('leaflet/dist/images/marker-icon.png'),
  shadowUrl: require('leaflet/dist/images/marker-shadow.png'),
});

// Create custom icons for different vehicle states
const onRouteIcon = new L.Icon({
  iconUrl: '/images/car.jpg',
  iconSize: [20, 20],
  iconAnchor: [16, 16],
  popupAnchor: [0, -16],
});

const offRouteIcon = new L.Icon({
  iconUrl: '/images/car.jpg',
  iconSize: [20, 20],
  iconAnchor: [16, 16],
  popupAnchor: [0, -16],
  className: 'off-route-marker' // Add custom class for styling
});

// --- Define the Warehouse Icon using L.Icon ---
const warehouseIconUrl = '/images/warehouse.jpg'; // Path relative to the public folder root
const warehouseIcon = new L.Icon({ // Create the Leaflet Icon instance
    iconUrl: warehouseIconUrl,
    iconSize: [32, 32],       // Adjust size as needed
    iconAnchor: [16, 32],    // Point of the icon which corresponds to marker's location (bottom center)
    popupAnchor: [0, -32]    // Point from which the popup should open relative to the iconAnchor
});
// --- END WAREHOUSE ICON ---

// --- HARDCODED WAREHOUSE LOCATIONS ---
// Replace these with your actual hardcoded points
const hardcodedWarehouses = [
    { id: 'WH001', name: 'Warehouse A (HCMC)', lat: 10.874788, lon: 106.778426},
    { id: 'WH002', name: 'Warehouse B (HCMC)', lat: 10.814834, lon: 106.601116 },
    // Add more warehouse objects here as needed
    // { id: 'WH003', name: 'Warehouse C', lat: ..., lon: ... },
];
// --- END HARDCODED DATA ---

function App() {
  const [vehicles, setVehicles] = useState([]);
  const [selectedVehicle, setSelectedVehicle] = useState(null);
  const [vehicleRoute, setVehicleRoute] = useState(null);
  const [stats, setStats] = useState({
    total: 0,
    onRoute: 0,
    offRoute: 0
  });

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await axios.get('http://localhost:8000/vehicles');
        setVehicles(response.data.vehicles);
        setStats({
          total: response.data.total_vehicles,
          onRoute: response.data.vehicles_on_route,
          offRoute: response.data.vehicles_off_route
        });
      } catch (error) {
        console.error('Error fetching vehicle data:', error);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleVehicleClick = async (vehicleId) => {
    try {
      const [vehicleResponse, routeResponse] = await Promise.all([
        axios.get(`http://localhost:8000/vehicles/${vehicleId}`),
        axios.get(`http://localhost:8000/routes/${vehicleId}`)
      ]);
      setSelectedVehicle(vehicleResponse.data);
      setVehicleRoute(routeResponse.data);
    } catch (error) {
      console.error('Error fetching vehicle details:', error);
    }
  };

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="container mx-auto px-4 py-8">
        <h1 className="text-3xl font-bold mb-8">Vehicle Tracking System</h1>
        
        {/* Stats Section */}
        <div className="grid grid-cols-3 gap-4 mb-8">
          <div className="bg-white p-4 rounded-lg shadow">
            <h2 className="text-xl font-semibold">Total Vehicles</h2>
            <p className="text-3xl">{stats.total}</p>
          </div>
          <div className="bg-green-100 p-4 rounded-lg shadow">
            <h2 className="text-xl font-semibold">On Route</h2>
            <p className="text-3xl">{stats.onRoute}</p>
          </div>
          <div className="bg-red-100 p-4 rounded-lg shadow">
            <h2 className="text-xl font-semibold">Off Route</h2>
            <p className="text-3xl">{stats.offRoute}</p>
          </div>
        </div>

        {/* Map Section */}
        <div className="grid grid-cols-3 gap-8">
          <div className="col-span-2">
            <div className="bg-white p-4 rounded-lg shadow h-[600px]">
              <MapContainer
                center={[10.762622, 106.660172]}
                zoom={13}
                style={{ height: '100%', width: '100%' }}
              >
                <TileLayer
                  url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                  attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                />
                {vehicles.map((vehicle) => (
                  <Marker
                    key={vehicle.vehicle_id}
                    position={[vehicle.latitude, vehicle.longitude]}
                    icon={vehicle.is_off_route ? offRouteIcon : onRouteIcon}
                    eventHandlers={{
                      click: () => handleVehicleClick(vehicle.vehicle_id),
                    }}
                  >
                    <Popup>
                      <div className={`p-2 ${vehicle.is_off_route ? 'bg-red-100' : 'bg-green-100'} rounded`}>
                        <h3 className="font-bold">Vehicle {vehicle.vehicle_id}</h3>
                        <p>Status: <span className={vehicle.is_off_route ? 'text-red-600 font-bold' : 'text-green-600 font-bold'}>
                          {vehicle.is_off_route ? 'OFF ROUTE' : 'ON ROUTE'}
                        </span></p>
                        <p>Speed: {vehicle.speed} km/h</p>
                        <p>Heading: {vehicle.heading}°</p>
                      </div>
                    </Popup>
                  </Marker>
                ))}
                {/* === Render Warehouse Markers (Added Here) === */}
                {hardcodedWarehouses.map((warehouse) => {
                    // Ensure you have latitude and longitude in your hardcoded data
                    const lat = parseFloat(warehouse.lat);
                    const lon = parseFloat(warehouse.lon);

                    // Simple check if coordinates are valid numbers
                    if (!isNaN(lat) && !isNaN(lon)) {
                       // console.log(`Rendering warehouse ${warehouse.id} at [${lat}, ${lon}]`); // For debugging
                       return (
                         <Marker
                            key={warehouse.id}      // Unique key for React
                            position={[lat, lon]}   // Position from hardcoded data
                            icon={warehouseIcon}    // Use the warehouse icon instance
                         >
                           <Popup>
                             <b>Warehouse ID:</b> {warehouse.id || 'N/A'}<br/>
                             {warehouse.name && <><b>Name:</b> {warehouse.name}<br/></>}
                             {/* Add other details if available in your hardcoded data */}
                           </Popup>
                         </Marker>
                       );
                    } else {
                       console.warn("Skipping warehouse marker due to invalid coords:", warehouse);
                       return null; // Important: return null if coords are invalid
                    }
                })}
                {/* === End Warehouse Markers === */}
                {vehicleRoute && (
                  <Polyline
                    positions={vehicleRoute.map(point => [point.latitude, point.longitude])}
                    color="blue"
                    weight={3}
                  />
                )}
              </MapContainer>
            </div>
          </div>

          {/* Vehicle Details Section */}
          <div className="bg-white p-4 rounded-lg shadow">
            <h2 className="text-xl font-semibold mb-4">Vehicle Details</h2>
            {selectedVehicle ? (
              <div className={`p-4 rounded ${selectedVehicle.is_off_route ? 'bg-red-50 border-l-4 border-red-500' : 'bg-green-50 border-l-4 border-green-500'}`}>
                <h3 className="font-bold mb-2">Vehicle {selectedVehicle.vehicle_id}</h3>
                <p className="mb-2">
                  Status: <span className={selectedVehicle.is_off_route ? 'text-red-600 font-bold' : 'text-green-600 font-bold'}>
                    {selectedVehicle.is_off_route ? 'OFF ROUTE' : 'ON ROUTE'}
                  </span>
                </p>
                <p>Location: {selectedVehicle.latitude}, {selectedVehicle.longitude}</p>
                <p>Speed: {selectedVehicle.speed} km/h</p>
                <p>Heading: {selectedVehicle.heading}°</p>
                <p>Odometer: {selectedVehicle.odometer} km</p>
                <p>Last Update: {new Date(selectedVehicle.timestamp).toLocaleString()}</p>
                {vehicleRoute && (
                  <div className="mt-4">
                    <h4 className="font-semibold">Route Information</h4>
                    <p>Total Points: {vehicleRoute.length}</p>
                  </div>
                )}
              </div>
            ) : (
              <p>Select a vehicle to view details</p>
            )}
          </div>
        </div>
      </div>

      {/* Add custom styles for off-route markers */}
      <style>
        {`
          .off-route-marker {
            filter: hue-rotate(300deg) saturate(200%) brightness(0.8);
            animation: pulse 2s infinite;
          }
          
          @keyframes pulse {
            0% {
              transform: scale(1);
            }
            50% {
              transform: scale(1.2);
            }
            100% {
              transform: scale(1);
            }
          }
        `}
      </style>
    </div>
  );
}

export default App; 