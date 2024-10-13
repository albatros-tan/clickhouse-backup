import os

from .base import env, BASE_PATH


class AppConfig:
    PATH_TO_TABLE = os.path.join(BASE_PATH, 'app/schema.json')
    COUNT_THREADS = env.int("COUNT_THREADS", default=2)
    