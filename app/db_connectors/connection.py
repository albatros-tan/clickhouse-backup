from asyncpg import Connection
from asyncpg.pool import Pool, create_pool
from ujson import dumps, loads


async def _init_connection(con: Connection):
    
    def _encoder(value):
        return b'\x01' + dumps(value).encode('utf-8')
    
    def _decoder(value):
        return loads(value[1:].decode('utf-8'))
    
    await con.set_type_codec('jsonb', encoder=_encoder, decoder=_decoder, schema='pg_catalog', format='binary')


async def create_psql_pool(dsn, **kwargs) -> Pool:
    return await create_pool(dsn=dsn, init=_init_connection, **kwargs)
