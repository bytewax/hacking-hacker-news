"""Output to Timeplus Proton."""
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from proton_driver import client
import logging

__all__ = [
    "ProtonSink",
]
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class _ProtonSinkPartition(StatelessSinkPartition):
    def __init__(self, stream: str, host: str):
        self.client=client.Client(host=host, port=8463)
        self.stream=stream
        sql=f"CREATE STREAM IF NOT EXISTS `{stream}` (raw string)"
        logger.debug(sql)
        self.client.execute(sql)

    def write_batch(self, items):
        logger.debug(f"inserting data {items}")
        rows=[]
        for item in items:
            rows.append([item]) # single column in each row
        sql = f"INSERT INTO `{self.stream}` (raw) VALUES"
        logger.debug(f"inserting data {sql}")
        self.client.execute(sql,rows)

class ProtonSink(DynamicSink):
    def __init__(self, stream: str, host: str):
        self.stream = stream
        self.host = host if host is not None and host != "" else "127.0.0.1"
    
    """Write each output item to Proton on that worker.

    Items consumed from the dataflow must look like a string. Use a
    proceeding map step to do custom formatting.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    """

    def build(self, worker_index, worker_count):
        """See ABC docstring."""
        return _ProtonSinkPartition(self.stream, self.host)