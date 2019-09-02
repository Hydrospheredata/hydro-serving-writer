import logging 
import pyarrow as pa
from hydro_serving_grpc import writer
from google.protobuf.empty_pb2 import Empty
from hydro_serving_writer.buffer import ModelVersionBufferPool

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class WriterServicer(writer.WriterServicer):

    def WriteOne(self, request, context):
        logger.debug("New request:\n{}".format(request))
        ModelVersionBufferPool.write_one(request)
        return Empty()

    def WriteMany(self, request, context):
        logger.debug("New request:\n{}".format(request))
        ModelVersionBufferPool.write_many(request)
        return Empty()