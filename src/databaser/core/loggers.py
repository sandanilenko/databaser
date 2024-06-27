from collections import (
    defaultdict,
)
from contextlib import (
    asynccontextmanager,
)
from datetime import (
    datetime,
)
from typing import (
    Iterable,
)

import psutil

from databaser.core.db_entities import (
    DBTable,
    DstDatabase,
)
from databaser.core.enums import (
    StagesEnum,
)
from databaser.core.helpers import (
    dates_to_string,
    logger,
)


class StatisticManager:
    """
    Менеджер занимающийся сборкой статистики различных этапов сборки и переноса данных
    """

    def __init__(
        self,
        database: DstDatabase,
    ):
        self._database = database

        self._time_indications = defaultdict(list)
        self._memory_usage_indications = defaultdict(list)

    def set_indication_time(self, stage):
        """
        Фиксация времени этапа
        """
        self._time_indications[stage].append(datetime.now())

    def set_indication_memory(self, stage):
        """
        Фиксация используемой оперативной памяти на этапе
        """
        self._memory_usage_indications[stage].append(
            dict(psutil.virtual_memory()._asdict())
        )

    def print_stages_indications(self):
        """
        Печать показателей этапов работы
        """
        for stage in StagesEnum.values.keys():
            if stage in self._time_indications:
                logger.info(
                    f"{StagesEnum.values.get(stage)} --- "
                    f"{dates_to_string(self._time_indications[stage])}"
                )

            if stage in self._memory_usage_indications:
                logger.info(
                    f"{StagesEnum.values.get(stage)} --- "
                    f"{self._memory_usage_indications[stage]}"
                )

    async def print_records_transfer_statistic(self):
        """
        Печать статистики перенесенных записей в целевую базу данных
        """
        tables: Iterable[DBTable] = self._database.tables.values()
        tables_counts = {
            table.name: (table.transferred_pks_count, await table.need_transfer_pks.len())
            for table in tables
        }

        sorted_tables_counts = (
            sorted(tables_counts, key=lambda t_n: tables_counts[t_n][0])
        )

        for table_name in sorted_tables_counts:
            logger.info(
                f"{table_name} --- {tables_counts[table_name][0]} / "
                f"{tables_counts[table_name][1]}"
            )


@asynccontextmanager
async def statistic_indexer(
    statistic_manager: StatisticManager,
    stage: int,
):
    """
    Контекстный менеджер сбора статистики этапа работы
    """
    statistic_manager.set_indication_time(stage)
    statistic_manager.set_indication_memory(stage)

    yield

    statistic_manager.set_indication_time(stage)
    statistic_manager.set_indication_memory(stage)
