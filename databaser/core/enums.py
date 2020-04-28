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
    PREPARING_AND_TRANSFERRING_DATA = 4
    TRANSFER_KEY_TABLE = 5
    COLLECT_COMMON_TABLES_RECORDS_IDS = 6
    COLLECT_GENERIC_TABLES_RECORDS_IDS = 7
    TRANSFERRING_COLLECTED_DATA = 8
    UPDATE_SEQUENCES = 9

    values = {
        PREPARE_DST_DB_STRUCTURE: "Prepare destination database structure",
        TRUNCATE_DST_DB_TABLES: "Truncate destination database tables",
        FILLING_TABLES_ROWS_COUNTS: "Filling tables rows counts",
        PREPARING_AND_TRANSFERRING_DATA: "Preparing and transferring data",
        TRANSFER_KEY_TABLE: "Transfer key table",
        COLLECT_COMMON_TABLES_RECORDS_IDS: "Collect common tables records ids",
        COLLECT_GENERIC_TABLES_RECORDS_IDS: "Collect generic tables records ids",
        TRANSFERRING_COLLECTED_DATA: "Transferring collected data",
        UPDATE_SEQUENCES: "Update sequences",
    }
