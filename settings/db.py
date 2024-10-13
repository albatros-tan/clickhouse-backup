#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Настройки подключения к БД Postgresql 

@author: ant
"""
import os

from .base import env, BASE_PATH


class DbConfig:
    CONNECTION_SETTINGS = {
        'dsn': env.str('DB_URL', default='postgresql://develop:postgresdev12@127.0.0.1:5432/dev'),
        'min_size': env.int('DB_POOL_MIN_SIZE', default=1),
        'max_size': env.int('DB_POOL_MAX_SIZE', default=2),
        'max_inactive_connection_lifetime': env.float('DB_POOL_MAX_INACTIVE_CONNECTION_LIFETIME', default=300),
        'timeout': env.int('DB_TIMEOUT', default=60),
        'statement_cache_size': env.int('DB_STATEMENT_CACHE_SIZE', default=1024),
    }
    DEFAULT_LIMIT = env.int('DB_DEFAULT_LIMIT', default=100)
    PATH_TO_MIGRATIONS = os.path.join(BASE_PATH, 'migrations/sql')

