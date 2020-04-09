import logging
import os
from distutils.util import (
    strtobool,
)

from core.helpers import (
    logger,
)

# Logger
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Src database connection params
SRC_DB_HOST = os.environ.get('SRC_DB_HOST')
SRC_DB_PORT = os.environ.get('SRC_DB_PORT')
SRC_DB_SCHEMA = os.environ.get('SRC_DB_SCHEMA', 'public')
SRC_DB_NAME = os.environ.get('SRC_DB_NAME')
SRC_DB_USER = os.environ.get('SRC_DB_USER')
SRC_DB_PASSWORD = os.environ.get('SRC_DB_PASSWORD')

# Dst database connection params
DST_DB_HOST = os.environ.get('DST_DB_HOST')
DST_DB_PORT = os.environ.get('DST_DB_PORT')
DST_DB_SCHEMA = os.environ.get('DST_DB_SCHEMA', 'public')
DST_DB_NAME = os.environ.get('DST_DB_NAME')
DST_DB_USER = os.environ.get('DST_DB_USER')
DST_DB_PASSWORD = os.environ.get('DST_DB_PASSWORD')

# Test mode parameters
TEST_MODE = bool(strtobool(os.environ.get('TEST_MODE') or 'False'))

if TEST_MODE:
    logger.warning('TEST MODE ACTIVATED!!!')

ENT_IDS = os.environ.get('ENT_IDS', '').split(',')
EXCLUDED_TABLES = os.environ.get('EXCLUDED_TABLES', '').split(',')
TABLES_WITH_GENERIC_FOREIGN_KEY = os.environ.get(
    'TABLES_WITH_GENERIC_FOREIGN_KEY',
    '',
).split(',')

TRUNCATE_EXCLUDED_TABLES = os.environ.get(
    'TRUNCATE_EXCLUDED_TABLES',
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
        ENT_IDS,
    ]
):
    raise Exception('You must send all params!')

ENT_IDS = tuple(map(int, ENT_IDS))

VALIDATE_DATA_BEFORE_TRANSFERRING = os.environ.get(
    'VALIDATE_DATA_BEFORE_TRANSFERRING',
    False,
)
