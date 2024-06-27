
from databaser.core.enums import (
    LogLevelEnum,
)
from databaser.core.settings_helpers import get_str_environ_parameter, get_bool_environ_parameter, \
    get_iterable_environ_parameter, get_extensible_iterable_environ_parameter, get_int_environ_parameter

# Logger
LOG_LEVEL = get_str_environ_parameter(
    name='DATABASER_LOG_LEVEL',
    default=LogLevelEnum.INFO,
)
LOG_DIRECTORY = get_str_environ_parameter(
    name='DATABASER_LOG_DIRECTORY',
)
LOG_FILENAME = get_str_environ_parameter(
    name='DATABASER_LOG_FILENAME',
)


# Src database connection params
SRC_DB_HOST = get_str_environ_parameter(
    name='DATABASER_SRC_DB_HOST',
)
SRC_DB_PORT = get_str_environ_parameter(
    name='DATABASER_SRC_DB_PORT',
)
SRC_DB_SCHEMA = get_str_environ_parameter(
    name='DATABASER_SRC_DB_SCHEMA',
    default='public',
)
SRC_DB_NAME = get_str_environ_parameter(
    name='DATABASER_SRC_DB_NAME',
)
SRC_DB_USER = get_str_environ_parameter(
    name='DATABASER_SRC_DB_USER',
)
SRC_DB_PASSWORD = get_str_environ_parameter(
    name='DATABASER_SRC_DB_PASSWORD',
)

# Dst database connection params
DST_DB_HOST = get_str_environ_parameter(
    name='DATABASER_DST_DB_HOST',
)
DST_DB_PORT = get_str_environ_parameter(
    name='DATABASER_DST_DB_PORT',
)
DST_DB_SCHEMA = get_str_environ_parameter(
    name='DATABASER_DST_DB_SCHEMA',
    default='public',
)
DST_DB_NAME = get_str_environ_parameter(
    name='DATABASER_DST_DB_NAME',
)
DST_DB_USER = get_str_environ_parameter(
    name='DATABASER_DST_DB_USER',
)
DST_DB_PASSWORD = get_str_environ_parameter(
    name='DATABASER_DST_DB_PASSWORD',
)

# Test mode parameters
TEST_MODE = get_bool_environ_parameter(
    name='DATABASER_TEST_MODE',
)

KEY_TABLE_NAME = get_str_environ_parameter(
    name='DATABASER_KEY_TABLE_NAME',
)
KEY_COLUMN_NAMES = get_iterable_environ_parameter(
    name='DATABASER_KEY_COLUMN_NAMES',
)
KEY_COLUMN_VALUES = get_iterable_environ_parameter(
    name='DATABASER_KEY_COLUMN_VALUES',
    type_=int,
)
KEY_TABLE_HIERARCHY_COLUMN_NAME = get_str_environ_parameter(
    name='DATABASER_KEY_TABLE_HIERARCHY_COLUMN_NAME',
)

EXCLUDED_TABLES = get_extensible_iterable_environ_parameter(
    name='DATABASER_EXCLUDED_TABLES',
)
TABLES_WITH_GENERIC_FOREIGN_KEY = get_iterable_environ_parameter(
    name='DATABASER_TABLES_WITH_GENERIC_FOREIGN_KEY',
)

TABLES_LIMIT_PER_TRANSACTION = get_int_environ_parameter(
    name='DATABASER_TABLES_LIMIT_PER_TRANSACTION',
    default=100,
)

IS_TRUNCATE_TABLES = get_bool_environ_parameter(
    name='DATABASER_IS_TRUNCATE_TABLES',
)
TABLES_TRUNCATE_INCLUDED = get_iterable_environ_parameter(
    name='DATABASER_TABLES_TRUNCATE_INCLUDED',
)
TABLES_TRUNCATE_EXCLUDED = get_iterable_environ_parameter(
    name='DATABASER_TABLES_TRUNCATE_EXCLUDED',
)

FULL_TRANSFER_TABLES = get_iterable_environ_parameter(
    name='DATABASER_FULL_TRANSFER_TABLES',
)

if not any(
    [
        SRC_DB_HOST,
        SRC_DB_PORT,
        SRC_DB_NAME,
        SRC_DB_USER,
        SRC_DB_PASSWORD,
        DST_DB_HOST,
        DST_DB_PORT,
        DST_DB_NAME,
        DST_DB_USER,
        DST_DB_PASSWORD,
        KEY_TABLE_NAME,
        KEY_COLUMN_NAMES,
        KEY_COLUMN_VALUES,
    ]
):
    raise ValueError('You must send all params!')

VALIDATE_DATA_BEFORE_TRANSFERRING = get_bool_environ_parameter(
    name='VALIDATE_DATA_BEFORE_TRANSFERRING',
)

USE_DATABASE_FOR_STORE_INTERMEDIATE_VALUES = get_bool_environ_parameter(
    name='DATABASER_USE_DATABASE_FOR_STORE_INTERMEDIATE_VALUES'
)

COLLECTOR_CHUNK_SIZE = get_int_environ_parameter(
    name='DATABASER_COLLECTOR_CHUNK_SIZE', default=60000
)

ASYNC_SEPARATION_COEFFICIENT = get_int_environ_parameter(
    name='DATABASER_ASYNC_SEPARATION_COEFFICIENT', default=-1
)
