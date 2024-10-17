import copy
from typing import Optional, Union
from uuid import UUID
from enum import Enum
from dataclasses import dataclass


class DBLevelLog(Enum):
    WARNING = "warning"
    ERROR = "error"


@dataclass
class BackupHistoryEntity:
    backup_guid: UUID
    table_name: str
    partition_key: str
    count: str
    file_name: str
    execution_time: int = None
    
    def extract_data(self):
        if self.execution_time is None:
            raise KeyError("execution_time cannot be None")
        return self.__dict__
    
    def get_string_values(self) -> str:
        return f"""('{str(self.backup_guid)}', '{self.table_name}', '{self.partition_key}', 
                    {self.count}, '{self.file_name}', {self.execution_time})"""
    
    
@dataclass
class BackupLogEntity:
    backup_guid: UUID
    table_name: str
    partition_key: str
    event: str
    level: DBLevelLog
    
    def extract_data(self):
        res = copy.copy(self.__dict__)
        res["level"] = res["level"].value
        return res
    
    def get_string_values(self) -> str:
        return f"""('{str(self.backup_guid)}', '{self.table_name}', '{self.partition_key}',
                    '{self.event}', '{self.level.value}')"""
    
    
@dataclass
class TablesShemaEntity:
    backup_guid: UUID
    table_name: str
    field_name: str
    field_type: str
    
    def extract_data(self):
        return self.__dict__
    
    def get_string_values(self) -> str:
        return f"('{str(self.backup_guid)}', '{self.table_name}', '{self.field_name}', '{self.field_type}')"
    
    
@dataclass
class S3FunctionParameters:
    path_to_file: str
    s3_access_key: str
    s3_secret_key: str
    format_file: str
    fields: str
    compression: Optional[str] = None
    
    def compile_param(self) -> tuple:
        return (
            self.path_to_file, 
            self.s3_access_key, 
            self.s3_secret_key, 
            self.format_file, 
            self.fields, 
            '' if self.compression is None else self.compression
        )


class InfoMessages(Enum):
    NOT_RELEVANTE_SCHEMA = "The schema of the {table_name} table is not relevant"
    START_TASK = "Task is started: {table_name} - {key_value}"
    START_BACKUP = "The backup task is running, backup guid: {backup_guid}"
    COMPLETE_BACKUP = "The backup task is completed, backup guid: {backup_guid}"
    ERROR_BACKUP = "Backup task {backup_guid} stopped with an error"
    TASK_ERROR = "The backup attempt {table_name} : {partition_value} failed with an error"
    PSQL_ERROR = "Error writing to the postgresql"
