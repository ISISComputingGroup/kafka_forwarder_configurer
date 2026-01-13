import logging
from threading import Timer


from kafka_producer import ProducerWrapper

UPDATE_FREQUENCY_S = 30.0

logger = logging.getLogger(__name__)
import mysql.connector


class InstPVs(object):
    def __init__(
        self, producer: ProducerWrapper
    ) -> None:
        self._pvs: set[str] = set()
        self._sql = mysql.connector.connect(
            database="iocdb",
            host="localhost",
            port=3306,
            user="report",
            password="$report")

        self.producer = producer

    def schedule(self) -> None:
        def action() -> None:
            self.update_pvs_from_mysql()
            self.schedule()

        self.update_pvs_from_mysql()
        job = Timer(UPDATE_FREQUENCY_S, action)
        job.start()

    def update_pvs_from_mysql(self) -> None:
        cursor = self._sql.cursor()
        query = 'SELECT pvname, value FROM iocdb.pvinfo WHERE infoname="archive";'
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            logger.error(f"No data from query ({query}")
            cursor.close()
            return

        pvs = set()
        for (basename, fields) in rows:
            assert isinstance(fields, str)
            for field in fields.split():
                if all(c in "0123456789." for c in field):
                    # This is an archiving time period, e.g. the 5.0 in
                    # info(archive, "5.0 VAL")
                    # Ignore it
                    continue
                pvs.add(f"{basename}.{field}")

        if self._pvs != pvs:
            logger.info(f"Inst configuration changed to: {pvs}")
            self.producer.remove_config(list(self._pvs - pvs))
            self.producer.add_config(list(pvs - self._pvs))
            self._pvs = pvs
        cursor.close()
