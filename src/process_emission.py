import json
import logging
import sys

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

max_co2_by_vehicle = {}

def process_emission_event(event):
    logger.info(f"Received event: {json.dumps(event)}")

    vehicle_id = event["vehicle_id"]
    current_co2 = float(event["data"]["vehicle_CO2"])

    previous_max = max_co2_by_vehicle.get(vehicle_id, float("-inf"))
    new_max = max(previous_max, current_co2)
    max_co2_by_vehicle[vehicle_id] = new_max

    result_topic = f"rover/{vehicle_id}/result"
    result_payload = {
        "vehicle_id": vehicle_id,
        "max_CO2": new_max,
        "status": "processed"
        "full_data": event["data"]
    }

    logger.info(f"Processed result: {result_payload}")
    return result_topic, result_payload
