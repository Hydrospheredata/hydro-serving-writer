import logging, os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

import grpc
import time, datetime, logging
from concurrent import futures

from hydro_serving_grpc import writer
from hydro_serving_writer.grpc_.service import WriterServicer
from hydro_serving_writer.utils.config import get_config

_ONE_DAY = datetime.timedelta(days=1)


def _wait_forever(server):
    try:
        while True:
            time.sleep(_ONE_DAY.total_seconds())
    except KeyboardInterrupt:
        server.stop(None)


def serve(port: int, max_workers: int):
    logger.info("Starting server at [::]:{:d}".format(port))
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers))
    writer.add_WriterServicer_to_server(WriterServicer(), server)
    server.add_insecure_port('[::]:{:d}'.format(port))
    server.start()
    _wait_forever(server) 


if __name__ == "__main__":
    config = get_config()
    if config.get("DEBUG"):
        logger.setLevel(logging.DEBUG)
    
    serve(
        port=config.get("SERVER_PORT"), 
        max_workers=config.get("SERVER_MAX_WORKERS"),
    )