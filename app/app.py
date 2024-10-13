import os
import asyncio
import json
from typing import Union, List
from asyncio import Semaphore
from uuid import UUID, uuid4
from asyncpg.pool import Pool
from asynch import connect

from .db_connectors.connection import create_psql_pool
from .db_connectors.clickhouse_connector import ClickhouseTable
from .db_connectors.postgresql_connector import PsQLTable
from .types import InfoMessages, TablesShemaEntity
from settings.db import DbConfig
from settings.app import AppConfig
from settings.clickhouse import ClHouseConfig
from settings.log import app_log


class App:
    psql_pool: Pool = None
    
    async def _init(self):
        self.psql_pool = await create_psql_pool(**DbConfig.CONNECTION_SETTINGS)
        
    async def _close(self):
        await self.psql_pool.close()
        
    async def _execute(self, **kwargs):
        raise NotImplementedError("The _execute method must be defined")
        
    async def run(self, **kwargs):
        await self._init()
        await self._execute(**kwargs)
        await self._close()
        
        
class Table:
    click_connect: connect = None
    
    def __init__(self, table_name: str, partition_key: Union[str, None], semaphore: Semaphore, psql: Pool):
        self.table_name = table_name
        self.partition_key = partition_key
        self.semaphore = semaphore
        self.psql = psql
        
    async def _init(self):
        self.click_connect = await connect(**ClHouseConfig.get_connection_data())
        self.click_table = ClickhouseTable(self.table_name, self.click_connect)
        self.psql_table = PsQLTable(psql_pool=self.psql)
    
    async def _close(self):
        del self.click_table
        del self.psql_table
        await self.click_connect.close()
        
    async def create_backup_schema(self, backup_guid: UUID):
        data = [
            TablesShemaEntity(
                backup_guid=backup_guid,
                table_name=self.table_name,
                field_name=key,
                field_type=value
            )for key, value in self.table_schema.items()
        ]
        try:
            await self.psql_table.insert_schema_table(data)
        except Exception as err:
            app_log.error(err, exc_info=True) 
            raise
    
    async def check_relevance_backup_schema(self, backup_guid: UUID) -> bool:
        try:
            self.table_schema = await self.click_table.get_schema_table()
            backup_schema = await self.psql_table.get_last_backup_schema(self.table_name)
        except Exception as err:
            app_log.error(err, exc_info=True)
            raise
        if self.table_schema == backup_schema:
            return True
        else:
            app_log.info(InfoMessages.NOT_RELEVANTE_SCHEMA.value.format(table_name=self.table_name))
            return False
        
    async def make_keys_list_for_backup(self) -> List[Union[str, None]]:
        """
        если Partition_key = None, то смотрим count() по всей таблице и сверяем с
        psql.backup_history count послежнего бэкапа,
        если Partition_key != None, то смотрим count() с группировкой по полю = 
        partition key. На выход подается пассив тех значений partition_key поля,
        count() которых не соответствует соответствующему count в psql.backup_history
        """
        return [None]
        
    async def backup(self, backup_guid: UUID, force: bool = None):
        await self._init()
        if not force:
            check = await self.check_relevance_backup_schema(backup_guid)
            if check == False:
                await self.create_backup_schema(backup_guid)
        await self._close()
        
        
class Backup(App):
    path_to_schema = AppConfig.PATH_TO_TABLE
    
    def __init__(self):
        self.backup_guid = uuid4()
        with open(self.path_to_schema, "r") as f:
            self.tables_for_backup = json.load(f)
        self.semaphore = asyncio.Semaphore(value=AppConfig.COUNT_THREADS)
        
    def find_partition_key(self, table_name: str) -> Union[str, None]:
        for item in self.tables_for_backup:
            if table_name == item['table_name']:
                return item['partition_key']
        return None
    
    async def _execute(self, table: str = None, force: bool = None):
        if table is None:
            tables = [
                Table(
                    table_name=item['table_name'],
                    partition_key=item['partition_key'],
                    semaphore=self.semaphore,
                    psql=self.psql_pool
                ) for item in self.tables_for_backup
            ]
        else:
            partition_key = self.find_partition_key(table_name=table)
            tables = [
                Table(table_name=table, partition_key=partition_key, semaphore=self.semaphore, psql=self.psql_pool)
            ]
            
        # Для дебага
        table = tables[0]
        await table.backup(self.backup_guid, force)
                               
    
class Restore(App):
    
    async def _execute(self, table: str = None, backup_guid: UUID = None):
        ...
    
    
class Listing(App):
    
    async def _execute(self, table: str = None, gt: str = None):
        print(table, gt)
