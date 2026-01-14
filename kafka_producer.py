import logging
from time import sleep
from typing import List

from confluent_kafka import Producer
from streaming_data_types.fbschemas.forwarder_config_update_fc00.Protocol import (
    Protocol,
)

from forwarder_config import ForwarderConfig

logger = logging.getLogger(__name__)


class ProducerWrapper:
    """
    A wrapper class for the kafka producer.
    """

    def __init__(
        self,
        server: str,
        config_topic: str,
        data_topic: str,
        epics_protocol: Protocol = Protocol.CA,  # pyright: ignore
    ) -> None:
        self.topic = config_topic
        self.converter = ForwarderConfig(data_topic, epics_protocol)
        while not self._set_up_producer(server):
            logger.error("Failed to create producer, retrying in 30s")
            sleep(30)

    def _set_up_producer(self, server: str) -> bool:
        """
        Attempts to create a Kafka producer and consumer. Retries with a recursive call every 30s.
        """
        try:
            self.producer = Producer({"bootstrap.servers": server})
            if not self.topic_exists(self.topic):
                logger.warning(
                    f"WARNING: topic {self.topic} does not exist. It will be created by default."
                )
            return True

        except Exception:
            logger.exception(
                f"Unexpected error while creating producer or consumer: ",
            )
        return False

    def add_config(self, pvs: List[str]) -> None:
        """
        Create a forwarder configuration to add more pvs to be monitored.

        :param pvs: A list of new PVs to add to the forwarder configuration.
        """
        message_buffer = self.converter.create_forwarder_configuration(pvs)
        self.producer.produce(self.topic, message_buffer)

    def topic_exists(self, topic_name: str) -> bool:
        return topic_name in self.producer.list_topics(topic_name).topics

    def remove_config(self, pvs: List[str]) -> None:
        """
        Create a forwarder configuration to remove pvs that are being monitored.

        :param pvs: A list of PVs to remove from the forwarder configuration.
        """
        message_buffer = self.converter.remove_forwarder_configuration(pvs)
        self.producer.produce(self.topic, message_buffer)
