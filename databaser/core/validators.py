from asyncpg.pool import (
    Pool,
)

from core.db_entities import (
    DstDatabase,
    SrcDatabase,
)
from core.loggers import (
    StatisticManager,
)


class DataValidator:
    """
    Валидатор данных перед физическим переносом в целевую базу данных
    """
    def __init__(
        self,
        dst_database: DstDatabase,
        src_database: SrcDatabase,
        dst_pool: Pool,
        src_pool: Pool,
        statistic_manager: StatisticManager,
    ):
        self._dst_database = dst_database
        self._src_database = src_database
        self._dst_pool = dst_pool
        self._src_pool = src_pool
        self._statistic_manager = statistic_manager

    def validate(self) -> bool:
        is_valid_data = False

        return is_valid_data
