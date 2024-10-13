import copy
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


class InfoMessages(Enum):
    NOT_RELEVANTE_SCHEMA = "The schema of the {table_name} table is not relevant"
