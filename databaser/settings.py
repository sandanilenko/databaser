import logging
import os
from distutils.util import (
    strtobool,
)

from core.helpers import (
    logger,
)

# Logger
LOG_LEVEL = os.environ.get('DATABASER_LOG_LEVEL', 'INFO')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Src database connection params
SRC_DB_HOST = os.environ.get('DATABASER_SRC_DB_HOST')
SRC_DB_PORT = os.environ.get('DATABASER_SRC_DB_PORT')
SRC_DB_SCHEMA = os.environ.get('DATABASER_SRC_DB_SCHEMA', 'public')
SRC_DB_NAME = os.environ.get('DATABASER_SRC_DB_NAME')
SRC_DB_USER = os.environ.get('DATABASER_SRC_DB_USER')
SRC_DB_PASSWORD = os.environ.get('DATABASER_SRC_DB_PASSWORD')

# Dst database connection params
DST_DB_HOST = os.environ.get('DATABASER_DST_DB_HOST')
DST_DB_PORT = os.environ.get('DATABASER_DST_DB_PORT')
DST_DB_SCHEMA = os.environ.get('DATABASER_DST_DB_SCHEMA', 'public')
DST_DB_NAME = os.environ.get('DATABASER_DST_DB_NAME')
DST_DB_USER = os.environ.get('DATABASER_DST_DB_USER')
DST_DB_PASSWORD = os.environ.get('DATABASER_DST_DB_PASSWORD')

# Test mode parameters
TEST_MODE = bool(strtobool(os.environ.get('DATABASER_TEST_MODE') or 'False'))

if TEST_MODE:
    logger.warning('TEST MODE ACTIVATED!!!')

KEY_TABLE_NAME = os.environ.get('DATABASER_KEY_TABLE_NAME')
KEY_COLUMN_NAMES = os.environ.get('DATABASER_KEY_COLUMN_NAMES', '').replace(' ', '').split(',')
KEY_COLUMN_IDS = os.environ.get('DATABASER_KEY_COLUMN_IDS', '').replace(' ', '').split(',')

EXCLUDED_TABLES = os.environ.get('DATABASER_EXCLUDED_TABLES', '').split(',')
TABLES_WITH_GENERIC_FOREIGN_KEY = os.environ.get(
    'DATABASER_TABLES_WITH_GENERIC_FOREIGN_KEY',
    '',
).split(',')

TABLES_LIMIT_PER_TRANSACTION = int(
    os.environ.get('DATABASER_TABLES_LIMIT_PER_TRANSACTION', 100)
)

TRUNCATE_EXCLUDED_TABLES = os.environ.get(
    'DATABASER_TABLES_TRUNCATE_EXCLUDED',
    '',
).split(',')

if not any(
    [
        SRC_DB_HOST,
        SRC_DB_PORT,
        SRC_DB_NAME,
        SRC_DB_USER,
        SRC_DB_PASSWORD,
        DST_DB_NAME,
        DST_DB_USER,
        DST_DB_PASSWORD,
        KEY_COLUMN_IDS,
    ]
):
    raise Exception('You must send all params!')

KEY_COLUMN_IDS = tuple(map(int, KEY_COLUMN_IDS))

VALIDATE_DATA_BEFORE_TRANSFERRING = os.environ.get(
    'VALIDATE_DATA_BEFORE_TRANSFERRING',
    False,
)
