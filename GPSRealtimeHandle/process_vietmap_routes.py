import requests
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import os
from dotenv import load_dotenv
import time
from pathlib import Path

# Load environment variables
load_dotenv()

class VietmapAPI:
    def __init__(self):
        self.api_key = '506862bb03a3d71632bdeb7674a3625328cb7e5a9b011841'
        if not self.api_key:
            raise ValueError("VIETMAP_API_KEY not found in environment variables")
        
        self.base_url = 'https://maps.vietmap.vn/api'
        self.headers = {
            'Content-Type': 'application/json',
            'apikey': self.api_key
        }
        self.max_retries = 3
        self.retry_delay = 1  # seconds

    def get_route(self, 
                 start_lat: float, 
                 start_lng: float, 
                 end_lat: float, 
                 end_lng: float,
                 vehicle_type: str = 'car',
                 instructions: bool = True,
                 alternative: bool = False,
                 points_encoded: bool = True,
                 calc_points: bool = True,
                 debug: bool = False,
                 locale: str = 'vi',
                 elevation: bool = False,
                 optimize: bool = False,
                 type: str = 'json',
                 api_version: str = '1.1') -> Optional[Dict]:
        """
        Get route between two points using Vietmap Directions API with retry mechanism
        """
        endpoint = f"{self.base_url}/route"
        params = {
            'point': [f"{start_lat},{start_lng}", f"{end_lat},{end_lng}"],
            'vehicle': vehicle_type,
            'instructions': str(instructions).lower(),
            'alternative': str(alternative).lower(),
            'points_encoded': str(points_encoded).lower(),
            'calc_points': str(calc_points).lower(),
            'debug': str(debug).lower(),
            'locale': locale,
            'elevation': str(elevation).lower(),
            'optimize': str(optimize).lower(),
            'type': type,
            'api-version': api_version
        }

        for attempt in range(self.max_retries):
            try:
                response = requests.get(endpoint, params=params, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    print(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"All attempts failed: {str(e)}")
                    return None

    def extract_gps_points(self, route_data: Dict) -> List[Dict]:
        """
        Extract GPS points from route data with additional metadata
        """
        points = []
        if not route_data or 'paths' not in route_data or not route_data['paths']:
            return points

        path = route_data['paths'][0]
        if 'points' not in path:
            return points

        # Decode polyline points
        coordinates = self._decode_polyline(path['points'])
        
        # Calculate total distance and time
        total_distance = path.get('distance', 0)  # in meters
        total_time = path.get('time', 0)  # in milliseconds
        
        for i, (lat, lng) in enumerate(coordinates):
            # Calculate progress percentage
            progress = (i / (len(coordinates) - 1)) * 100 if len(coordinates) > 1 else 0
            
            points.append({
                'latitude': lat,
                'longitude': lng,
                'timestamp': datetime.utcnow().isoformat(),
                'sequence': i,
                'progress': round(progress, 2),
                'total_points': len(coordinates),
                'total_distance': total_distance,
                'total_time': total_time
            })
        
        return points

    def _decode_polyline(self, polyline: str) -> List[Tuple[float, float]]:
        """
        Decode Google Maps polyline string to coordinates with improved error handling
        """
        if not polyline:
            return []

        index = 0
        lat = 0
        lng = 0
        coordinates = []
        
        try:
            while index < len(polyline):
                # Latitude
                shift = 0
                result = 0
                while True:
                    if index >= len(polyline):
                        raise ValueError("Invalid polyline string")
                    b = ord(polyline[index]) - 63
                    result |= (b & 0x1f) << shift
                    shift += 5
                    index += 1
                    if b < 0x20:
                        break
                lat += ~(result >> 1) if result & 1 else result >> 1
                
                # Longitude
                shift = 0
                result = 0
                while True:
                    if index >= len(polyline):
                        raise ValueError("Invalid polyline string")
                    b = ord(polyline[index]) - 63
                    result |= (b & 0x1f) << shift
                    shift += 5
                    index += 1
                    if b < 0x20:
                        break
                lng += ~(result >> 1) if result & 1 else result >> 1
                
                coordinates.append((lat * 1e-5, lng * 1e-5))
        except Exception as e:
            print(f"Error decoding polyline: {str(e)}")
            return []
        
        return coordinates

def validate_coordinates(lat: float, lng: float) -> bool:
    """Validate if coordinates are within Vietnam's bounds"""
    return 8.5 <= lat <= 23.5 and 102.0 <= lng <= 110.0

def process_route(api: VietmapAPI, item: Dict) -> Optional[Dict]:
    """Process a single route with error handling"""
    try:
        start_lat, start_lng = item['start_lat'], item['start_lon']
        end_lat, end_lng = item['end_lat'], item['end_lon']
        
        # Validate coordinates
        if not all(validate_coordinates(lat, lng) for lat, lng in [(start_lat, start_lng), (end_lat, end_lng)]):
            print(f"Invalid coordinates for route: ({start_lat}, {start_lng}) to ({end_lat}, {end_lng})")
            return None
        
        print(f"Processing route from ({start_lat}, {start_lng}) to ({end_lat}, {end_lng})")
        
        # Get route data
        route_data = api.get_route(
            start_lat=start_lat,
            start_lng=start_lng,
            end_lat=end_lat,
            end_lng=end_lng
        )

        if not route_data:
            print(f"  ✗ Failed to get route data")
            return None

        # Extract GPS points
        gps_points = api.extract_gps_points(route_data)
        if not gps_points:
            print(f"  ✗ No GPS points extracted")
            return None
        
        print(f"  ✓ Successfully processed route with {len(gps_points)} points")
        
        return {
            'route_info': {
                'start': {'lat': start_lat, 'lng': start_lng},
                'end': {'lat': end_lat, 'lng': end_lng},
                'total_points': len(gps_points),
                'vehicle_id': item.get('vehicle_id', 'unknown')
            },
            'points': gps_points
        }
    except Exception as e:
        print(f"Error processing route: {str(e)}")
        return None

def main():
    try:
        # Initialize API client
        api = VietmapAPI()

        # Load data from data.json
        data_file = Path('data.json')
        if not data_file.exists():
            raise FileNotFoundError("data.json not found")

        with open(data_file, 'r') as f:
            data = json.load(f)

        if not data:
            raise ValueError("No data found in data.json")

        # Process each route
        results = []
        for item in data:
            result = process_route(api, item)
            if result:
                results.append(result)

        if not results:
            raise ValueError("No routes were successfully processed")

        # Save results
        output_data = {
            'processed_routes': results,
            'total_routes': len(results),
            'total_points': sum(len(r['points']) for r in results),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        output_file = Path('vietmap_route_data.json')
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\nProcessed {len(results)} routes")
        print(f"Total points: {output_data['total_points']}")
        print(f"Results saved to {output_file}")

    except Exception as e:
        print(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 