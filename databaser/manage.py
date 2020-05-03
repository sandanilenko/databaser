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


async def main(
    src_db_connection_parameters: DBConnectionParameters,
    dst_db_connection_parameters: DBConnectionParameters,
):
    src_database = SrcDatabase(
        db_connection_parameters=src_db_connection_parameters,
    )

    dst_database = DstDatabase(
        db_connection_parameters=dst_db_connection_parameters,
    )

    statistic_manager = StatisticManager(dst_database, logger)

    async with asyncpg.create_pool(
        dst_database.connection_str, min_size=30, max_size=40
    ) as dst_pool:
        async with asyncpg.create_pool(
            src_database.connection_str, min_size=30, max_size=40
        ) as src_pool:
            src_database.connection_pool = src_pool
            dst_database.connection_pool = dst_pool

            await src_database.prepare_table_names()

            logger.info(
                f'src_database tables count - {len(src_database.table_names)}'
            )

            fdw_wrapper = PostgresFDWExtensionWrapper(
                src_database=src_database,
                dst_database=dst_database,
                dst_pool=dst_pool,
            )
            await asyncio.wait([fdw_wrapper.disable()])

            with StatisticIndexer(
                statistic_manager,
                TransferringStagesEnum.PREPARE_DST_DB_STRUCTURE,
            ):
                await dst_database.prepare_structure()

                await dst_database.disable_triggers()

            with StatisticIndexer(
                statistic_manager,
                TransferringStagesEnum.TRUNCATE_DST_DB_TABLES,
            ):
                await dst_database.truncate_tables()

            await asyncio.wait([fdw_wrapper.enable()])

            collector = Collector(
                dst_database=dst_database,
                src_database=src_database,
                dst_pool=dst_pool,
                src_pool=src_pool,
                statistic_manager=statistic_manager,
                key_column_ids=settings.KEY_COLUMN_IDS,
            )

            await asyncio.wait([collector.build_key_column_ids_structure()])

            with StatisticIndexer(
                statistic_manager,
                TransferringStagesEnum.FILLING_TABLES_ROWS_COUNTS,
            ):
                await collector.fill_tables_rows_counts()

            await collector.collect()

            transporter = Transporter(
                dst_database=dst_database,
                src_database=src_database,
                dst_pool=dst_pool,
                src_pool=src_pool,
                statistic_manager=statistic_manager,
                key_column_ids=collector.key_column_ids,
            )

            with StatisticIndexer(
                statistic_manager,
                TransferringStagesEnum.PREPARING_AND_TRANSFERRING_DATA,
            ):
                await asyncio.wait([transporter.transfer()])

            await dst_database.enable_triggers()

            await asyncio.wait([fdw_wrapper.disable()])

            statistic_manager.print_transferring_indications()
            statistic_manager.print_records_transfer_statistic()

            if settings.TEST_MODE:
                validator_manager = ValidatorManager(
                    dst_database=dst_database,
                    src_database=src_database,
                    statistic_manager=statistic_manager,
                    key_column_ids=collector.key_column_ids,
                )

                await validator_manager.validate()


if __name__ == '__main__':
    start = datetime.now()
    logger.info(f'date start - {start}')

    src_db_connection_parameters = DBConnectionParameters(
        host=settings.SRC_DB_HOST,
        port=settings.SRC_DB_PORT,
        schema=settings.SRC_DB_SCHEMA,
        dbname=settings.SRC_DB_NAME,
        user=settings.SRC_DB_USER,
        password=settings.SRC_DB_PASSWORD,
    )

    dst_db_connection_parameters = DBConnectionParameters(
        host=settings.DST_DB_HOST,
        port=settings.DST_DB_PORT,
        schema=settings.DST_DB_SCHEMA,
        dbname=settings.DST_DB_NAME,
        user=settings.DST_DB_USER,
        password=settings.DST_DB_PASSWORD,
    )

    asyncio.run(
        main(
            src_db_connection_parameters=src_db_connection_parameters,
            dst_db_connection_parameters=dst_db_connection_parameters,
        ),
        debug=settings.TEST_MODE,
    )
    finish = datetime.now()

    logger.info(
        f'dates start - {start}, finish - {finish}, spend time - '
        f'{finish - start}'
    )
