import json
import logging
import sys

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

max_co2_by_device = {}


def process_emission_event(event):
    logger.info(f"Received event: {json.dumps(event)}")

    device_id = event["device_id"]
    device_num = int(event["device_num"])
    row_data = event["data"]

    co2_val = float(row_data["vehicle_CO2"])

    previous_max = max_co2_by_device.get(device_num, float("-inf"))
    new_max = max(previous_max, co2_val)
    max_co2_by_device[device_num] = new_max

    result_topic = f"rover/{device_num}/result"

    result_payload = {
        "device_id": device_id,
        "device_num": device_num,
        "current_CO2": co2_val,
        "max_CO2": new_max,
        "status": "processed"
    }

    logger.info(f"Processed result: {result_payload}")

    return result_topic, result_payload
