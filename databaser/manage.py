import asyncio
from datetime import (
    datetime,
)

import asyncpg
import uvloop

import settings
from core.collectors import (
    Collector,
)
from core.db_entities import (
    DstDatabase,
    SrcDatabase,
)
from core.enums import (
    TransferringStagesEnum,
)
from core.helpers import (
    DBConnectionParameters,
    logger,
)
from core.loggers import (
    StatisticIndexer,
    StatisticManager,
)
from core.transporters import (
    Transporter,
)
from core.validators import (
    ValidatorManager,
)
from core.wrappers import (
    PostgresFDWExtensionWrapper,
)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class DatabaserManager:
    """
    Databaser manager
    """

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        self._src_db_connection_parameters = DBConnectionParameters(
            host=settings.SRC_DB_HOST,
            port=settings.SRC_DB_PORT,
            schema=settings.SRC_DB_SCHEMA,
            dbname=settings.SRC_DB_NAME,
            user=settings.SRC_DB_USER,
            password=settings.SRC_DB_PASSWORD,
        )

        self._dst_db_connection_parameters = DBConnectionParameters(
            host=settings.DST_DB_HOST,
            port=settings.DST_DB_PORT,
            schema=settings.DST_DB_SCHEMA,
            dbname=settings.DST_DB_NAME,
            user=settings.DST_DB_USER,
            password=settings.DST_DB_PASSWORD,
        )

        self._src_database = SrcDatabase(
            db_connection_parameters=self._src_db_connection_parameters,
        )

        self._dst_database = DstDatabase(
            db_connection_parameters=self._dst_db_connection_parameters,
        )

        self._statistic_manager = StatisticManager(
            database=self._dst_database,
        )

    async def _main(self):
        """
        Run async databaser
        """
        async with asyncpg.create_pool(
            self._dst_database.connection_str,
            min_size=30,
            max_size=40,
        ) as dst_pool:
            async with asyncpg.create_pool(
                self._src_database.connection_str,
                min_size=30,
                max_size=40,
            ) as src_pool:
                self._src_database.connection_pool = src_pool
                self._dst_database.connection_pool = dst_pool

                await self._src_database.prepare_table_names()

                logger.info(
                    f'src_database tables count - '
                    f'{len(self._src_database.table_names)}'
                )

                fdw_wrapper = PostgresFDWExtensionWrapper(
                    src_database=self._src_database,
                    dst_database=self._dst_database,
                    dst_pool=dst_pool,
                )
                await asyncio.wait([fdw_wrapper.disable()])

                with StatisticIndexer(
                    self._statistic_manager,
                    TransferringStagesEnum.PREPARE_DST_DB_STRUCTURE,
                ):
                    await self._dst_database.prepare_structure()

                    await self._dst_database.disable_triggers()

                with StatisticIndexer(
                    self._statistic_manager,
                    TransferringStagesEnum.TRUNCATE_DST_DB_TABLES,
                ):
                    await self._dst_database.truncate_tables()

                await asyncio.wait([fdw_wrapper.enable()])

                collector = Collector(
                    dst_database=self._dst_database,
                    src_database=self._src_database,
                    dst_pool=dst_pool,
                    src_pool=src_pool,
                    statistic_manager=self._statistic_manager,
                    key_column_ids=settings.KEY_COLUMN_VALUES,
                )

                await asyncio.wait([collector.build_key_column_ids_structure()])

                with StatisticIndexer(
                    self._statistic_manager,
                    TransferringStagesEnum.FILLING_TABLES_ROWS_COUNTS,
                ):
                    await collector.fill_tables_rows_counts()

                await collector.collect()

                transporter = Transporter(
                    dst_database=self._dst_database,
                    src_database=self._src_database,
                    dst_pool=dst_pool,
                    src_pool=src_pool,
                    statistic_manager=self._statistic_manager,
                    key_column_ids=collector.key_column_ids,
                )

                with StatisticIndexer(
                    self._statistic_manager,
                    TransferringStagesEnum.PREPARING_AND_TRANSFERRING_DATA,
                ):
                    await asyncio.wait([transporter.transfer()])

                await self._dst_database.enable_triggers()

                await asyncio.wait([fdw_wrapper.disable()])

                self._statistic_manager.print_transferring_indications()
                self._statistic_manager.print_records_transfer_statistic()

                if settings.TEST_MODE:
                    validator_manager = ValidatorManager(
                        dst_database=self._dst_database,
                        src_database=self._src_database,
                        statistic_manager=self._statistic_manager,
                        key_column_ids=collector.key_column_ids,
                    )

                    await validator_manager.validate()

    def manage(self):
        start = datetime.now()
        logger.info(f'date start - {start}')

        asyncio.run(
            self._main(),
            debug=settings.TEST_MODE,
        )

        finish = datetime.now()
        logger.info(
            f'dates start - {start}, finish - {finish}, spend time - '
            f'{finish - start}'
        )


if __name__ == '__main__':
    manager = DatabaserManager()
    manager.manage()
