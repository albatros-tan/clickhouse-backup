# Exceptions for clickhouse connector
class ErrorGettingTableDescription(Exception):
    pass


# Exceptions for postgresql connetor
class ErrorGettingBackupSchema(Exception):
    pass


class ErrorInsertingSchemaBackup(Exception):
    pass

