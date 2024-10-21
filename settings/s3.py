from .base import env


class S3Config:
    HOST = env("S3_HOST", default="http://172.18.0.3:9000")
    ACCESS_KEY = env.str("S3_ACCESS_KEY", default="minio")
    SECRET_KEY = env.str("S3_SECRET_KEY", default="minio124")
    BUCKET_NAME = env.str("S3_BUCKET_NAME_BACKUP", default="backup")
