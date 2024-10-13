from typing import Dict
from asynch.proto.connection import Connection
from asynch.cursors import DictCursor

from .exceptions import ErrorGettingTableDescription


class ClickhouseConnector:
    
    def __init__(self, click_connect: Connection):
        self.click_connect = click_connect
        
    async def fetchall(self, sql):
        async with self.click_connect.cursor(cursor=DictCursor) as cursor:
            await cursor.execute(sql)
            result = await cursor.fetchall()
        return result


class ClickhouseTable(ClickhouseConnector):
    
    def __init__(self, table_name: str, click_connect: Connection):
        super().__init__(click_connect)
        self.table_name = table_name
        
    async def get_schema_table(self) -> Dict:
        sql = f"describe table {self.table_name};"
        try:
            result = await self.fetchall(sql)
        except Exception as e:
            raise ErrorGettingTableDescription(e.__str__())
        return {item['name']: item['type'] for item in result}
        
    async def get_count_records(self, key_name: str = None, key_value: str = None):
        ...
        

