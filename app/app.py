import os
import asyncio
import json
import time
from datetime import datetime
from typing import Union, List, Dict
from asyncio import Semaphore
from uuid import UUID, uuid4
from asyncpg.pool import Pool
from asynch import connect, create_pool
from asynch.pool import Pool as ClickPool

from .db_connectors.connection import create_psql_pool
from .db_connectors.clickhouse_connector import ClickhouseTable
from .db_connectors.postgresql_connector import PsQLTable
from .types import InfoMessages, TablesShemaEntity, S3FunctionParameters
from .types import DBLevelLog,  BackupHistoryEntity, BackupLogEntity
from settings.db import DbConfig
from settings.app import AppConfig
from settings.s3 import S3Config
from settings.clickhouse import ClHouseConfig
from settings.log import app_log


class App:
    psql_pool: Pool = None
    
    async def _init(self):
        self.psql_pool = await create_psql_pool(**DbConfig.CONNECTION_SETTINGS)
        self.click_connect = await create_pool(maxsize=ClHouseConfig.POOL_MAXSIZE, **ClHouseConfig.get_connection_data())
        
        
    async def _close(self):
        await self.psql_pool.close()
        self.click_connect.close()
        await self.click_connect.wait_closed()
        
    async def _execute(self, **kwargs):
        raise NotImplementedError("The _execute method must be defined")
        
    async def run(self, **kwargs):
        await self._init()
        await self._execute(**kwargs)
        await self._close()
        
        
class Table:
    click_connect: connect = None
    table_schema = None
    
    def __init__(
            self, 
            table_name: str, 
            partition_key: Union[str, None], 
            semaphore: Semaphore, 
            psql: Pool,
            click_pool: ClickPool,
            datetime_backup: str
        ):
        self.table_name = table_name
        self.partition_key = partition_key
        self.semaphore = semaphore
        self.psql = psql
        self.click_connect = click_pool
        self.datetime_backup = datetime_backup
        
    async def _init(self):
        self.click_table = ClickhouseTable(self.table_name, self.click_connect)
        self.psql_table = PsQLTable(psql_pool=self.psql)
    
    async def _close(self):
        del self.click_table
        del self.psql_table
        
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
        
    async def make_keys_list_for_backup(self, force: bool, key_name: str) -> Union[List[Dict], None]:
        """
        если Partition_key = None, то смотрим count() по всей таблице и сверяем с
        psql.backup_history count послежнего бэкапа,
        если Partition_key != None, то смотрим count() с группировкой по полю = 
        partition key. 
        Если force = True, на выход подается массив всех пар {partition_key: count()}
        Если force = False, запускается сверка с количеством записей в предыдущем бэкапе
                            на выход подается пассив тех пар {partition_key: count()},
                            count() которых не соответствует соответствующему count 
                            в psql.backup_history
        Для случая, когда self.partition_key = None метод вернет [{'-': count()}]
        
        """
        try:
            if self.partition_key is None or key_name=='-':
                parts = [{key_name: "-", 'count': await self.click_table.get_count_records()}]
                # parts = {'-': await self.click_table.get_count_records()}
            else:
                parts = await self.click_table.get_count_records_by_pkey(self.partition_key)
        except Exception as err:
            app_log.error(err, exc_info=True)
            raise
        if force:
            return parts
        try:
            last_parts = await self.psql_table.get_count_records_in_last_backup(self.table_name)
        except Exception as err:
            app_log.error(err, exc_info=True)
            raise
        return [item for item in parts if item['count'] != last_parts.get(str(item[key_name]), 0)]
        
    def generate_backup_file_name(self, pkey_value: str) -> str:
        table_label = self.table_name.replace("_", "-")
        pkey_label = pkey_value.replace("-", "")
        return f"{table_label}-{pkey_label}-{self.datetime_backup}"
    
    async def backup_record(self, key_value: str, count: int, backup_guid: str):
        app_log.info(InfoMessages.START_TASK.value.format(table_name=self.table_name, key_value=key_value))
        file_name=self.generate_backup_file_name(key_value)
        path_to_file = ClHouseConfig.get_path_to_s3_function(file_name=file_name)
        s3_parameters = S3FunctionParameters(
            path_to_file=path_to_file, 
            s3_access_key=S3Config.ACCESS_KEY, 
            s3_secret_key=S3Config.SECRET_KEY, 
            format_file=ClHouseConfig.FORMAT_BACKUP_FILE, 
            fields=', '.join([f"{key} {val}" for key, val in self.table_schema.items()]),
            compression=ClHouseConfig.COMPRESSION_BACKUP
        )
        await self.semaphore.acquire()
        t_start = time.time()
        try:
            await self.click_table.create_backup(self.partition_key, key_value, s3_parameters)
        except Exception as err:
            event_error = True
            app_log.error(
                InfoMessages.TASK_ERROR.value.format(table_name=self.table_name, partition_value=key_value),
                exc_info=True
            )
            error_message = err.__str__()
        else:
            event_error = False           
        self.semaphore.release()
        
        if event_error:
            data = BackupLogEntity(
                backup_guid=backup_guid,
                table_name=self.table_name, 
                partition_key=key_value, 
                event=error_message,
                level=DBLevelLog.ERROR
            )
            coro = self.psql_table.insert_backup_log
        else:
            data = BackupHistoryEntity(
                backup_guid=backup_guid, 
                table_name=self.table_name, 
                partition_key=key_value, 
                count=count, 
                file_name=file_name,
                execution_time = time.time() - t_start
            )
            coro = self.psql_table.insert_backup_history
        
        try:
            await coro(data)
        except:
            app_log.warning(InfoMessages.PSQL_ERROR.value, exc_info=True)
    
    async def backup(self, backup_guid: UUID, force: bool = None):
        await self._init()
        if not force:
            check = await self.check_relevance_backup_schema(backup_guid)
            if check == False:
                force = True
        else:
            self.table_schema = await self.click_table.get_schema_table()
        key_name = '-' if self.partition_key is None else self.partition_key
        parts_to_backup = await self.make_keys_list_for_backup(force, key_name)
        if parts_to_backup:
            await self.create_backup_schema(backup_guid)
            tasks = [
                asyncio.create_task(
                    self.backup_record(
                        key_value=str(part[key_name]),
                        count=part['count'], 
                        backup_guid=str(backup_guid)
                    ),
                    name=f"{self.table_name}-{part[key_name]}"
                ) for part in parts_to_backup
            ]
            done, pending = await asyncio.wait(tasks)
        
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
        app_log.info(InfoMessages.START_BACKUP.value.format(backup_guid=self.backup_guid))
        datetime_backup = datetime.now().strftime("%Y%m%d%H%M%S")
        if table is None:
            tables = [
                Table(
                    table_name=item['table_name'],
                    partition_key=item['partition_key'],
                    semaphore=self.semaphore,
                    psql=self.psql_pool,
                    click_pool=self.click_connect,
                    datetime_backup=datetime_backup
                ) for item in self.tables_for_backup
            ]
        else:
            partition_key = self.find_partition_key(table_name=table)
            tables = [
                Table(
                    table_name=table,
                    partition_key=partition_key, 
                    semaphore=self.semaphore, 
                    psql=self.psql_pool,
                    click_pool=self.click_connect,
                    datetime_backup=datetime_backup
                )
            ]
            
        tasks = [
            asyncio.create_task(table.backup(self.backup_guid, force), name=table.table_name) for table in tables
        ]
        
        try:
            done, _ = await asyncio.wait(tasks)
        except Exception:
            app_log.error(InfoMessages.ERROR_BACKUP.value.format(backup_guid=self.backup_guid), exc_info=True)
            raise
        else:
            app_log.info(InfoMessages.COMPLETE_BACKUP.value.format(backup_guid=self.backup_guid))   

    
class Restore(App):
    
    async def _execute(self, table: str = None, backup_guid: UUID = None):
        ...
    
    
class Listing(App):
    
    async def _execute(self, table: str = None, gt: str = None):
        print(table, gt)
