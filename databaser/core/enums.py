class ConstraintTypesEnum(object):
    PRIMARY_KEY = "PRIMARY KEY"
    FOREIGN_KEY = "FOREIGN KEY"
    UNIQUE = "UNIQUE"

    types = [
        PRIMARY_KEY,
        FOREIGN_KEY,
        UNIQUE,
    ]

    @classmethod
    def get_types_str(cls):
        return ", ".join(cls.types)

    @classmethod
    def get_types_comma(cls):
        return ", ".join(map(lambda key: f"'{key}'", cls.types))


class DataTypesEnum:
    """
    Postgres data types enumerate
    """
    SMALLINT = 'smallint'
    INTEGER = 'integer'
    BIGINT = 'bigint'
    SMALLSERIAL = 'smallserial'
    SERIAL = 'serial'
    BIGSERIAL = 'bigserial'

    CHARACTER_VARYING = 'character varying'
    TEXT = 'text'

    NUMERAL = (
        SMALLINT,
        INTEGER,
        BIGINT,
        SMALLSERIAL,
        SERIAL,
        BIGSERIAL,
    )


class TransferringStagesEnum:
    PREPARE_DST_DB_STRUCTURE = 1
    TRUNCATE_DST_DB_TABLES = 2
    FILLING_TABLES_ROWS_COUNTS = 3
    CREATING_TEMP_TABLES = 4
    PREPARING_AND_TRANSFERRING_DATA = 5
    TRANSFER_KEY_TABLE = 6
    COLLECT_COMMON_TABLES_RECORDS_IDS = 7
    COLLECT_GENERIC_TABLES_RECORDS_IDS = 8
    TRANSFERRING_COLLECTED_DATA = 9
    DROPPING_TEMP_TABLES = 10
    UPDATE_SEQUENCES = 11

    values = {
        PREPARE_DST_DB_STRUCTURE: 'Prepare destination database structure',
        TRUNCATE_DST_DB_TABLES: 'Truncate destination database tables',
        FILLING_TABLES_ROWS_COUNTS: 'Filling tables rows counts',
        CREATING_TEMP_TABLES: 'Creating temp tables',
        PREPARING_AND_TRANSFERRING_DATA: 'Preparing and transferring data',
        TRANSFER_KEY_TABLE: 'Transfer key table',
        COLLECT_COMMON_TABLES_RECORDS_IDS: 'Collect common tables records ids',
        COLLECT_GENERIC_TABLES_RECORDS_IDS: 'Collect generic tables records ids',
        TRANSFERRING_COLLECTED_DATA: 'Transferring collected data',
        DROPPING_TEMP_TABLES: 'Dropping temp tables',
        UPDATE_SEQUENCES: "Update sequences",
    }


class LogLevelEnum:
    NOTSET = 'NOTSET'
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'

    values = {
        NOTSET: 'notset',
        DEBUG: 'debug',
        INFO: 'info',
        WARNING: 'warning',
        ERROR: 'error',
        CRITICAL: 'critical',
    }
