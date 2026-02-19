import os
import pandas as pd
import logging
from aim_waves.config import Config

logger = logging.getLogger(__name__)

vehicle_list = []
vehicle_size_map = {}
vehicle_batch_map = {}

def load_vehicle_data():
    global vehicle_list, vehicle_size_map, vehicle_batch_map
    
    csv_path = Config.CSV_PATH
    try:
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, encoding='utf-8')
            for _, row in df.iterrows():
                vehicle = str(row['Vehicle']).strip()
                size = str(row['Size']).strip()
                pod = str(row.get('Pod', '')).strip()
                segment = str(row.get('Segment', '')).strip()

                if vehicle and size:
                    upper_vehicle = vehicle.upper()
                    vehicle_list.append(vehicle)
                    vehicle_size_map.setdefault(upper_vehicle, []).append(size)
                    vehicle_batch_map[(upper_vehicle, size)] = {
                        "status": "INCLUDED",
                        "pod": pod,
                        "segment": segment
                    }
            logger.info(f"✅ Loaded {len(vehicle_batch_map)} vehicle/size rows from CSV.")
        else:
            logger.warning(f"⚠️ CSV file not found at {csv_path}. Vehicle data not loaded.")

    except Exception as e:
        logger.error(f"❌ Failed to load CSV: {e}")
        # traceback.print_exc()

    vehicle_list = sorted(set(vehicle_list), key=lambda x: x.upper())

# Initial load
load_vehicle_data()
