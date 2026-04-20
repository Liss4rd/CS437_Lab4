import sys
import json
import time
import logging

from awsgreengrasspubsubsdk.pubsub_client import AwsGreengrassPubSubSdkClient
from process_emission import process_emission_event

log = logging.getLogger(__name__)
logging.basicConfig(
    format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s",
    stream=sys.stdout,
    level=logging.INFO
)


class MaxCO2Component:
    def __init__(self, ggv2_component_config):
        log.info("Initializing MaxCO2 Greengrass Component...")

        pubsub_base_topic = ggv2_component_config["base-pubsub-topic"]
        mqtt_subscribe_topics = ggv2_component_config["mqtt-subscribe-topics"]

        log.info(f"Base Topic: {pubsub_base_topic}")
        log.info(f"Subscribing to: {mqtt_subscribe_topics}")

        self.pubsub_client = AwsGreengrassPubSubSdkClient(
            pubsub_base_topic,
            self.default_message_handler
        )

        self.pubsub_client.activate_mqtt_pubsub()

        for topic in mqtt_subscribe_topics:
            self.pubsub_client.subscribe_to_topic("mqtt", topic)

        log.info("Component initialized successfully.")

    def service_loop(self):
        while True:
            time.sleep(5)

    def default_message_handler(self, protocol, topic, message_id, status, route, message):
        try:
            log.info(f"Received message on topic {topic}: {message}")

            if isinstance(message, str):
                payload = json.loads(message)
            else:
                payload = message

            result_topic, result_payload = process_emission_event(payload)

            self.pubsub_client.publish_message(
                "mqtt",
                json.dumps(result_payload),
                topic=result_topic
            )

            log.info(f"Published result to {result_topic}: {result_payload}")

        except Exception as err:
            log.error(f"Exception in default_message_handler: {err}")


if __name__ == "__main__":
    try:
        ggv2_component_config = json.loads(sys.argv[1])
        log.info(f"Loaded config: {ggv2_component_config}")

        component = MaxCO2Component(ggv2_component_config)
        component.service_loop()

    except Exception as err:
        log.error(f"Startup error: {err}")
        raise
