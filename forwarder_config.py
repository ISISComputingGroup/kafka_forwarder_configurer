from typing import List

from streaming_data_types.fbschemas.forwarder_config_update_fc00.Protocol import (
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_fc00.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_fc00 import StreamInfo, serialise_fc00


class ForwarderConfig:
    """
    Class that converts the pv information to a forwarder config message payload
    """

    def __init__(
        self,
        topic: str,
        epics_protocol: Protocol = Protocol.CA,  # pyright: ignore
        schema: str = "f144",
    ) -> None:
        self.schema = schema
        self.topic = topic
        self.epics_protocol = epics_protocol

    def _create_streams(self, pvs: List[str]) -> List[StreamInfo]:
        return [StreamInfo(pv, self.schema, self.topic, self.epics_protocol, 0) for pv in pvs]  # pyright: ignore

    def create_forwarder_configuration(self, pvs: List[str]) -> bytes:
        return serialise_fc00(UpdateType.ADD, self._create_streams(pvs))  # pyright: ignore

    def remove_forwarder_configuration(self, pvs: List[str]) -> bytes:
        return serialise_fc00(UpdateType.REMOVE, self._create_streams(pvs))  # pyright: ignore

    @staticmethod
    def remove_all_forwarder_configuration() -> bytes:
        return serialise_fc00(UpdateType.REMOVEALL, [])  # pyright: ignore
