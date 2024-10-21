from typing import Dict, List, Union
from asynch.proto.connection import Connection
from asynch.pool import Pool
from asynch.cursors import DictCursor

from .exceptions import ErrorGettingTableDescription, ErrorGettingDataCount, ErrorBackup
from ..types import S3FunctionParameters


class ClickhouseConnector:
    
    def __init__(self, click_connect: Pool):
        self.click_connect = click_connect
        
    async def fetchall(self, sql) -> List[Dict]:
        async with self.click_connect.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sql)
                result = await cursor.fetchall()
        return result
    
    async def fetchone(self, sql) -> Dict:
        async with self.click_connect.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sql)
                result = await cursor.fetchone()
        return result
    
    async def fetchval(self, sql) -> Union[str, int, float]:
        result = await self.fetchone(sql)
        return list(result.values())[0]
    
    async def execute(self, sql) -> int:
        async with self.click_connect.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sql)
            

class ClickhouseTable(ClickhouseConnector):
    
    def __init__(self, table_name: str, click_connect: Pool):
        super().__init__(click_connect)
        self.table_name = table_name
        
    async def get_schema_table(self) -> Dict:
        sql = f"describe table {self.table_name};"
        try:
            result = await self.fetchall(sql)
        except Exception as e:
            raise ErrorGettingTableDescription(e.__str__()) from e
        return {item['name']: item['type'] for item in result}
        
    async def get_count_records(self) -> int:
        sql = f"select count() as count from {self.table_name}"
        try:
            result = await self.fetchval(sql)
        except Exception as e:
            raise ErrorGettingDataCount(e.__str__()) from e
        return result
    
    async def get_count_records_by_pkey(self, partition_key: str) -> List[Dict]:
        sql = f"select {partition_key}, count() as count from {self.table_name} group by {partition_key};"
        try:
            result = await self.fetchall(sql)
        except Exception as e:
            raise ErrorGettingDataCount(e.__str__()) from e
        return result
    
    async def create_backup(
        self, partition_key: str, pkey_value: Union[str, int, float], s3_parameters: S3FunctionParameters
    ):
        sql = f"insert into function s3{s3_parameters.compile_param()} select * from {self.table_name}"
        if partition_key != "-":
            sql += f" where {partition_key} = '{pkey_value}';"
        try:
            await self.execute(sql)
        except Exception as e:
            raise ErrorBackup(e.__str__()) from e
            

