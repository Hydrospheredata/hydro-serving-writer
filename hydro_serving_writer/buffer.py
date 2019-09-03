import os, logging, datetime
from typing import Dict, Optional, List
from collections import defaultdict
from functools import reduce
from concurrent import futures
import pyarrow as pa, pyarrow.parquet as pq
import google
from hydro_serving_grpc import writer
from hydro_serving_writer.storage import Storage
from hydro_serving_writer.utils.config import get_config

__all__ = ["ModelVersionBufferPool"]

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('matplotlib').setLevel(logging.WARNING) 
logger = logging.getLogger()


schema = pa.schema([
    ('model_version', pa.int64()), 
    ('ts', pa.int64()),
    ('uid', pa.int64()),
    ('feature', pa.string()),
    ('value', pa.float64()),
    ('description', pa.string()),
    ('check', pa.bool_()),
])


class ModelVersionBufferPool:
    pool = {}
    config = get_config()

    @classmethod
    def get_buffer(cls, model_version):
        if ModelVersionBufferPool.pool.get(model_version):
            return ModelVersionBufferPool.pool.get(model_version)
        ModelVersionBufferPool.pool[model_version] = ModelVersionBuffer(
            model_version, ModelVersionBufferPool.config.get("BUFFER_SIZE"))
        return ModelVersionBufferPool.pool[model_version]
    
    @staticmethod
    def write_one(one_request: writer.WriteOneRequest):
        buffer = ModelVersionBufferPool.get_buffer(one_request.model_version)
        buffer.write(one_request.ByteSize(), one_request.SerializeToString())

    @staticmethod
    def write_many(many_request: writer.WriteManyRequest):
        for one_request in many_request.requests:
            ModelVersionBufferPool.write_one(one_request)

class ModelVersionBuffer:
    def __init__(self, model_version, buffer_size=1024*1024*16, *args, **kwargs):
        self.__version = model_version
        self.__buffer_size = buffer_size
        self._buffer = None
        self._output_stream = None
        logger.debug("Initialized new {}".format(self))

    def __repr__(self):
        return "ModelVersionBuffer(version={}, buffer_size={})".format(
            self.__version, self.__buffer_size)
    
    @property
    def output_stream(self) -> pa.BufferOutputStream: 
        if not hasattr(self, '_buffer') or self._buffer is None:
            self._allocate_buffer()
        if not hasattr(self, '_output_stream') or self._output_stream is None:
            self._output_stream = pa.output_stream(self._buffer)
        return self._output_stream

    def _allocate_buffer(self, size=None):
        self._buffer = pa.allocate_buffer(size or self.__buffer_size)
        logger.debug("Allocated new buffer of size {}".format(self.__buffer_size))
        
    def _request_can_be_written(self, length: int) -> bool:
        if length > self.__buffer_size:
            raise ValueError("Message is greater than buffer size")
        return self.output_stream.tell() + length + 4 < self.__buffer_size

    def _read_request_length(self, stream: pa.BufferReader) -> Optional[int]: 
        if stream.tell() + 4 <= self.__buffer_size: 
            return int.from_bytes(stream.read(4), "big")
    
    def read(self) -> List[writer.WriteOneRequest]:
        """ Parse binary data from buffer into ProtoBuf message """
        
        stream = pa.input_stream(self._buffer)
        length = self._read_request_length(stream)
        requests, request = [], writer.WriteOneRequest()
        while length and length < self.__buffer_size - stream.tell():
            request.ParseFromString(stream.read(length))
            requests.append(request)
            length = self._read_request_length(stream)
        logger.debug("Read buffer")
        return requests

    def write(self, length: int, body: bytes):
        """ Write request into buffer """

        if not self._request_can_be_written(length):
            requests = self.flush()

            timestamp = int(datetime.datetime.utcnow().timestamp())
            source_path = "{}.parquet".format(timestamp)
            destination_path = "dump/model_version={}/timestamp={}.parquet".format(
                self.__version, timestamp)

            with futures.ProcessPoolExecutor(max_workers=1) as executor:

                # Dump buffer to parquet file
                future = executor.submit(
                    self.dump, filename=source_path, requests=requests)

                # Upload parquet file to Storage
                future.add_done_callback(
                    lambda _: Storage().upload_file(
                        source_path=source_path,
                        destination_path=destination_path,
                    )
                )

                # Remove dumped parquet file from local storage
                future.add_done_callback(
                    lambda _: os.remove(source_path)
                )

        self.output_stream.write(length.to_bytes(4, "big"))
        self.output_stream.write(body)
        logger.debug("Written {} bytes".format(length + 4))

    def flush(self) -> str: 
        """ Flush requests from buffer to .parquet """

        pb_requests = self.read()
        del self._output_stream
        del self._buffer

        logger.debug("Flushed all data and deleted allocated buffer")
        return pb_requests

    def dump(self, filename, requests):
        """ Dump requests to parquet and upload them to storage """

        def flatten(requests, *args):
            return map(lambda obj: reduce(getattr, args, obj), requests)

        data = dict()
        for name, pa_type in zip(schema.names, schema.types):
            if name in ('ts', 'uid'): 
                data[name] = pa.array(flatten(requests, 'trace_data', name), type=pa_type)
            else: 
                data[name] = pa.array(flatten(requests, name), type=pa_type)
        
        table = pa.table(data, schema)
        pq.write_table(table, filename)
        logger.info("Dumped all data from {} to {}".format(self, filename))
