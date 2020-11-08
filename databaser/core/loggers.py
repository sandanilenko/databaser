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
from core.db_entities import (
    DBTable,
    DstDatabase,
)
from core.enums import (
    TransferringStagesEnum,
)
from core.helpers import (
    dates_list_to_str,
    logger,
)


class StatisticManager:
    def __init__(
        self,
        database: DstDatabase,
    ):
        self._database = database

        self._time_indications = defaultdict(list)
        self._memory_usage_indications = defaultdict(list)

    def set_indication_time(self, stage):
        """
        Add stage indication time

        Stage from TransferringStagesEnum
        """
        self._time_indications[stage].append(datetime.now())

    def set_indication_memory(self, stage):
        """
        Add stage memory usage indication
        """
        self._memory_usage_indications[stage].append(
            dict(psutil.virtual_memory()._asdict())
        )

    def print_transferring_indications(self):
        """
        Output transferring indications to log
        """
        for stage in TransferringStagesEnum.values.keys():
            if stage in self._time_indications:
                logger.info(
                    f"{TransferringStagesEnum.values.get(stage)} --- "
                    f"{dates_list_to_str(self._time_indications[stage])}"
                )

            if stage in self._memory_usage_indications:
                logger.info(
                    f"{TransferringStagesEnum.values.get(stage)} --- "
                    f"{self._memory_usage_indications[stage]}"
                )

    def print_records_transfer_statistic(self):
        """
        Output transferred tables rows count
        """
        tables: Iterable[DBTable] = self._database.tables.values()
        tables_counts = {
            table.name: (table.transferred_pks_count, len(table.need_transfer_pks))
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
    Statistic indexer context manager
    """
    statistic_manager.set_indication_time(stage)
    statistic_manager.set_indication_memory(stage)

    yield

    statistic_manager.set_indication_time(stage)
    statistic_manager.set_indication_memory(stage)
