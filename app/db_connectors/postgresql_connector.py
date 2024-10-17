from typing import Dict, List, Union
import json
from asyncpg.pool import Pool

from .exceptions import ErrorGettingBackupSchema, ErrorInsertingSchemaBackup, ErrorGettingDataCount
from .exceptions import ErrorInsertingBackupHistory, ErrorInsertingBackupLog
from ..types import TablesShemaEntity, BackupHistoryEntity, BackupLogEntity


def get_last_backup_guid(table_name: str):
    return f"""
    with last_backup as 
    (
    	select backup_guid
    	from clickhouse_backup.backup_history
    	where table_name='{table_name}'
    	group by backup_guid, created 
    	order by created desc
    	limit 1 
    )"""


def get_last_backup_sqhema_query(table_name: str):
    return f"""{get_last_backup_guid(table_name)}
    select json_agg(json_build_object(field_name, field_type)) as schema 
    from clickhouse_backup.tables_schema ts
    right join last_backup on ts.backup_guid  = last_backup.backup_guid;
    """
    

def get_count_records_in_last_backup_query(table_name: str):
    return f"""{get_last_backup_guid(table_name)}
    select bh.partition_key, bh.count from clickhouse_backup.backup_history bh
    right join last_backup on bh.backup_guid  = last_backup.backup_guid
    where bh.table_name = '{table_name}';
    """
    

def insert_schema_table_query(data: List[TablesShemaEntity]):
    sql = "INSERT INTO clickhouse_backup.tables_schema (backup_guid, table_name, field_name, field_type) VALUES "
    for item in data:
        sql = sql + item.get_string_values() + ',\n'
    sql = sql[:-2] + ";"
    return sql


def insert_backup_history_query(data: BackupHistoryEntity):
    sql = """INSERT INTO clickhouse_backup.backup_history 
            (backup_guid, table_name, partition_key, count, file_name, execution_time) VALUES
          """
    sql += data.get_string_values() + ";"
    return sql


def insert_backup_log_query(data: BackupLogEntity):
    sql = "INSERT INTO clickhouse_backup.backup_log (backup_guid, table_name, partition_key, event, level) VALUES "
    sql += data.get_string_values() + ";"
    return sql


class PsQLTable:
    
    def __init__(self,  psql_pool: Pool):
        self.psql_pool = psql_pool
        
    async def get_last_backup_schema(self, table_name: str) -> Dict:
        sql = get_last_backup_sqhema_query(table_name)
        try:
            result = await self.psql_pool.fetchrow(sql)
            schema = json.loads(result['schema']) if result["schema"] else []
        except Exception as e:
            raise ErrorGettingBackupSchema(e.__str()) from e
        result = {}
        for item in schema:
            result.update(item)
        return result
    
    async def insert_schema_table(self, data: List[TablesShemaEntity]):
        sql = insert_schema_table_query(data)
        try:
            await self.psql_pool.execute(sql)
        except Exception as e:
            raise ErrorInsertingSchemaBackup(e.__str__()) from e
    
    async def get_count_records_in_last_backup(self, table_name: str) -> Dict:
        sql = get_count_records_in_last_backup_query(table_name)
        try:
            result = await self.psql_pool.fetch(sql)
        except Exception as e:
            raise ErrorGettingDataCount(e.__str__()) from e
        return {item['partition_key']: item['count'] for item in result}
    
    async def insert_backup_history(self, data: BackupHistoryEntity):
        sql = insert_backup_history_query(data)
        try:
            await self.psql_pool.execute(sql)
        except Exception as e:
            raise ErrorInsertingBackupHistory(e.__str__()) from e
            
    async def insert_backup_log(self, data: BackupLogEntity):
        sql = insert_backup_log_query(data)
        try:
            await self.psql_pool.execute(sql)
        except Exception as e:
            raise ErrorInsertingBackupLog(e.__str__()) from e
    