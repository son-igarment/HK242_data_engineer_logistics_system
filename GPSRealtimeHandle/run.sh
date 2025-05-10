#!/bin/bash

# Ensure RabbitMQ is running (e.g., via Docker: docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management)
# This script assumes RabbitMQ is already running.

echo "Starting Vehicle Location Updater..."
cd vehicle-location-updater || exit
uv run vehicle_updater.py &
VEHICLE_UPDATER_PID=$!
cd ..
echo "Vehicle Location Updater started with PID $VEHICLE_UPDATER_PID"

echo "Starting Backend Service..."
cd backend || exit
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!
cd ..
echo "Backend Service started with PID $BACKEND_PID"

echo "Starting Frontend Service..."
cd frontend || exit
npm start &
FRONTEND_PID=$!
cd ..
echo "Frontend Service started with PID $FRONTEND_PID"

echo "All services started."
echo "Vehicle Updater PID: $VEHICLE_UPDATER_PID"
echo "Backend PID: $BACKEND_PID"
echo "Frontend PID: $FRONTEND_PID"

echo "To stop services, use 'kill <PID>' for each service, or pkill -f 'python vehicle_updater.py', pkill -f 'uvicorn main:app', pkill -f 'react-scripts start'"

# Keep script running to easily kill all background jobs with Ctrl+C if needed
# Or use 'wait' to wait for background processes (optional)
# wait $VEHICLE_UPDATER_PID $BACKEND_PID $FRONTEND_PID
