import os

from .base import env
from .s3 import S3Config


class ClHouseConfig:
    HOST = env.str("CLICK_HOST", default="localhost")
    PORT = env.int("CLICK_PORT", default=7000)
    USER = env.str('CLICK_USER', default='default')
    PASSWORD = env.str('CLICK_PASSWORD', default='')
    RECEIVE_TIMEOUT = env.int("CLICK_TIMEOUT", default=1000)
    FORMAT_BACKUP_FILE = "CSV"
    COMPRESSION_BACKUP = "gzip"
    
    @classmethod
    def get_path_to_s3_function(cls, file_name: str):
        return os.path.join(
            S3Config.HOST, 
            S3Config.BUCKET_NAME,
            f"{file_name}.{cls.FORMAT_BACKUP_FILE.lower()}.{cls.COMPRESSION_BACKUP}"
        )
    
    @classmethod
    def get_connection_data(cls):
        return {
            'host': cls.HOST,
            'port': cls.PORT, 
            'user': cls.USER, 
            'password': cls.PASSWORD,
            'sync_request_timeout': cls.RECEIVE_TIMEOUT
        }
