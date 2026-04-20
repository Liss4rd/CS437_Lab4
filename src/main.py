import json
import time
import logging
import sys

import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (
    PublishToIoTCoreRequest,
    QOS,
    SubscribeToIoTCoreRequest
)

from process_emission import process_emission_event

log = logging.getLogger(__name__)
logging.basicConfig(
    format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s",
    stream=sys.stdout,
    level=logging.INFO
)


class StreamHandler(awsiot.greengrasscoreipc.client.SubscribeToIoTCoreStreamHandler):
    def __init__(self, ipc_client):
        super().__init__()
        self.ipc_client = ipc_client

    def on_stream_event(self, event):
        try:
            message = event.message
            topic = message.topic_name
            payload = message.payload.decode("utf-8")

            log.info(f"Received message on topic {topic}: {payload}")

            event_payload = json.loads(payload)
            result_topic, result_payload = process_emission_event(event_payload)

            publish_request = PublishToIoTCoreRequest()
            publish_request.topic_name = result_topic
            publish_request.payload = json.dumps(result_payload).encode("utf-8")
            publish_request.qos = QOS.AT_LEAST_ONCE

            operation = self.ipc_client.new_publish_to_iot_core()
            operation.activate(publish_request)
            operation.get_response().result(timeout=10)

            log.info(f"Published result to {result_topic}: {result_payload}")

        except Exception as err:
            log.error(f"Exception in on_stream_event: {err}")

    def on_stream_error(self, error):
        log.error(f"Stream error: {error}")
        return True

    def on_stream_closed(self):
        log.info("Subscribe stream closed")


class MaxCO2Component:
    def __init__(self):
        log.info("Initializing MaxCO2 Greengrass Component...")
        self.ipc_client = awsiot.greengrasscoreipc.connect()

    def start(self):
        request = SubscribeToIoTCoreRequest()
        request.topic_name = "rover/+/data"
        request.qos = QOS.AT_LEAST_ONCE

        handler = StreamHandler(self.ipc_client)
        operation = self.ipc_client.new_subscribe_to_iot_core(handler)
        operation.activate(request)
        operation.get_response().result(timeout=10)

        log.info("Subscribed to rover/+/data successfully.")

        while True:
            time.sleep(5)


if __name__ == "__main__":
    try:
        component = MaxCO2Component()
        component.start()
    except Exception as err:
        log.error(f"Startup error: {err}")
        raise
