# Exceptions for clickhouse connector
class ErrorGettingTableDescription(Exception):
    pass


class ErrorGettingDataCount(Exception):
    pass


class ErrorBackup(Exception):
    pass


# Exceptions for postgresql connetor
class ErrorGettingBackupSchema(Exception):
    pass


class ErrorInsertingSchemaBackup(Exception):
    pass


class ErrorInsertingBackupHistory(Exception):
    pass


class ErrorInsertingBackupLog(Exception):
    pass

