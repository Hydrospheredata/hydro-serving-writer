import boto3 
import os, logging
from hydro_serving_writer.utils.config import get_config

__all__ = ["Storage"]

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Storage:

    def __init__(self, *args, **kwargs):
        self.config = get_config()
        self.type = self.config.get("STORAGE_TYPE")
        self.bucket = self.config.get("STORAGE_BUCKET")
        self.s3 = boto3.resource(
            's3', 
            aws_access_key_id=self.config.get("STORAGE_ACCESS_KEY"),
            aws_secret_access_key=self.config.get("STORAGE_SECRET_ACCESS_KEY")
        )

    def upload_file(self, source_path, destination_path):
        """
        Upload file to bucket. 

        Parameters
        ----------
        source_path: str
            Path to target file, which have to be uploaded.
        destination_path: str
            Relative path in the bucket, where file should be uploaded. 
        """
        logger.info("Uploading file {} to {}".format(source_path, destination_path))
        if self.type == "s3": 
            return self._upload_file_s3(source_path, destination_path)
        raise ValueError("{} does not support file uploads".format(self))
    
    def _upload_file_s3(self, source_path, destination_path):
        self.s3.meta.client.upload_file(
            Bucket=self.bucket, 
            Key=destination_path, 
            Filename=source_path, 
        )
    
    def __repr__(self):
        return "Storage(type={})".format(self.type)