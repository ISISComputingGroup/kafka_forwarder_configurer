import logging
from argparse import ArgumentParser
from os import environ
from time import sleep

from block_server_monitor import BlockServerMonitor
from inst_pvs import InstPVs
from kafka_producer import ProducerWrapper

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "-d",
        "--data",
        help="Kafka topic to send Block PV data to",
        type=str,
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Kafka topic to send forwarder config to",
        type=str,
    )
    parser.add_argument(
        "-r",
        "--runlog",
        help="Kafka topic to send run log PV data to",
        type=str,
    )
    parser.add_argument(
        "-b",
        "--broker",
        help="Location of the Kafka brokers (host:port)",
        nargs="+",
        type=str,
        default="livedata.isis.cclrc.ac.uk:31092",
    )
    parser.add_argument(
        "-p",
        "--pvprefix",
        help="PV Prefix of the block server",
        type=str,
        default=environ["MYPVPREFIX"],
    )

    args = parser.parse_args()
    KAFKA_DATA = args.data
    KAFKA_RUNLOG = args.runlog
    KAFKA_CONFIG = args.config
    KAFKA_BROKER = args.broker
    PREFIX = args.pvprefix
    block_producer = ProducerWrapper(KAFKA_BROKER, KAFKA_CONFIG, KAFKA_DATA)
    inst_producer = ProducerWrapper(KAFKA_BROKER, KAFKA_CONFIG, KAFKA_DATA)
    monitor = BlockServerMonitor(f"{PREFIX}CS:BLOCKSERVER:BLOCKNAMES", PREFIX, block_producer)
    runlog_producer = ProducerWrapper(KAFKA_BROKER, KAFKA_CONFIG, KAFKA_RUNLOG)

    dae_prefix = f"{PREFIX}DAE:"
    runlog_producer.add_config(
        [
            f"{dae_prefix}COUNTRATE",
            f"{dae_prefix}BEAMCURRENT",
            f"{dae_prefix}GOODFRAMES",
            f"{dae_prefix}RAWFRAMES",
            f"{dae_prefix}GOODUAH",
            f"{dae_prefix}MEVENTS",
            f"{dae_prefix}TOTALCOUNTS",
            f"{dae_prefix}MONITORCOUNTS",
            f"{dae_prefix}NPRATIO",
            f"{dae_prefix}PERIOD",
            f"{dae_prefix}TOTALUAMPS",
            # todo how should we do run_status/icp_event/is_running/is_waiting?
        ]
    )

    InstPVs(inst_producer).schedule()

    while True:
        sleep(0.1)
