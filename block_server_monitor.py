import json
import logging
import numpy as np
import numpy.typing as npt

from epics import PV
from threading import RLock

from ibex_non_ca_helpers.compress_hex import dehex_and_decompress

from kafka_producer import ProducerWrapper

logger = logging.getLogger(__name__)

class BlockServerMonitor:
    """
    Class that monitors the blockserver to see when the config has changed.

    Uses a Channel Access Monitor.
    """

    def __init__(self, address: str, pvprefix: str, producer: ProducerWrapper) -> None:
        self.pv_prefix = pvprefix
        self.address = address
        self.channel = PV(self.address, auto_monitor=True, callback=self.update)
        self.producer = producer
        self.last_pvs = []
        self.monitor_lock = RLock()


        connected = self.channel.wait_for_connection(timeout=5)
        if not connected: 
            logger.error(f"Unable to find pv {self.address}")
            return
        logger.info(f"Connected to {self.address}")


    def block_name_to_pv_name(self, blk: str) -> str:
        """
        Converts a block name to a PV name by adding the prefixes.

        Args:
            blk (string): The name of the block.

        Returns:
            string : the associated PV name.
        """
        return f"{self.pv_prefix}CS:SB:{blk}"


    def update_config(self, blocks: list[str]) -> None:
        """
        Updates the forwarder configuration to monitor the supplied blocks.

        Args:
            blocks (list): Blocks in the BlockServer containing PV data.

        Returns:
            None.
        """

        pvs = [self.block_name_to_pv_name(blk) for blk in blocks]
        if pvs != self.last_pvs:
            logger.info(f"Blocks configuration changed to: {pvs}")
            self.producer.remove_config(self.last_pvs)
            self.producer.add_config(pvs)
            self.last_pvs = pvs

    def update(self, value: npt.NDArray[np.uint8], **kwargs) -> None:  # noqa: ANN401
        """
        Updates the kafka config when the blockserver changes. This is called from the monitor.

        Args:
            epics_args (dict): Contains the information for the blockserver blocks PV.
            user_args (dict): Not used.

        Returns:
            None.
        """

        with self.monitor_lock:
            logger.info("new update %s ", value)
            data = dehex_and_decompress(value.tobytes())
            blocks = json.loads(data)
            self.update_config(blocks)
