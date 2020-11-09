import asyncio
from copy import (
    copy,
)
from datetime import (
    datetime,
)
from typing import (
    List,
    Set,
    Type,
)

import asyncpg
import settings
from asyncpg import (
    UndefinedFunctionError,
)
from core.collectors import (
    BaseCollector,
    GenericTablesCollector,
    KeyTableCollector,
    SortedByDependencyTablesCollector,
    TablesWithKeyColumnSiblingsCollector,
)
from core.db_entities import (
    DBTable,
    DstDatabase,
    SrcDatabase,
)
from core.enums import (
    TransferringStagesEnum,
)
from core.helpers import (
    DBConnectionParameters,
    logger,
    make_str_from_iterable,
)
from core.loggers import (
    StatisticManager,
    statistic_indexer,
)
from core.repositories import (
    SQLRepository,
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

        self._key_column_values = set(settings.KEY_COLUMN_VALUES)

    async def _get_key_table_parents_values(
        self,
        key_table_primary_key_name: str,
        key_table_primary_key_value: int,
    ):
        """
        Get hierarchy of key table records by parent_id
        """
        async with self._src_database.connection_pool.acquire() as connection:
            get_key_table_parents_values_sql = f"""
                with recursive hierarchy("{key_table_primary_key_name}", "{settings.KEY_TABLE_HIERARCHY_COLUMN_NAME}", "level") as (
                    select "{settings.KEY_TABLE_NAME}"."{key_table_primary_key_name}", 
                        "{settings.KEY_TABLE_NAME}"."{settings.KEY_TABLE_HIERARCHY_COLUMN_NAME}", 
                        0 
                    from "{settings.KEY_TABLE_NAME}" 
                    where "{settings.KEY_TABLE_NAME}"."{key_table_primary_key_name}" = {key_table_primary_key_value}
        
                    union all
        
                    select
                        "{settings.KEY_TABLE_NAME}"."{key_table_primary_key_name}",
                        "{settings.KEY_TABLE_NAME}"."{settings.KEY_TABLE_HIERARCHY_COLUMN_NAME}",
                        "hierarchy"."level" + 1
                    from "{settings.KEY_TABLE_NAME}" 
                    join "hierarchy" on "{settings.KEY_TABLE_NAME}"."{key_table_primary_key_name}" = "hierarchy"."{settings.KEY_TABLE_HIERARCHY_COLUMN_NAME}"
                )
                select "{settings.KEY_TABLE_NAME}"."{key_table_primary_key_name}" {key_table_primary_key_name} 
                from "{settings.KEY_TABLE_NAME}" 
                join "hierarchy" on "{settings.KEY_TABLE_NAME}"."{key_table_primary_key_name}" = "hierarchy"."{key_table_primary_key_name}"
                where "{settings.KEY_TABLE_NAME}"."{key_table_primary_key_name}" <> {key_table_primary_key_value}
                order by "hierarchy"."level" desc;
            """

            async with connection.transaction():
                records = await connection.fetch(get_key_table_parents_values_sql)

        self._key_column_values.update(
            [
                record.get('id')
                for record in records
            ]
        )

        del get_key_table_parents_values_sql

    async def _build_key_column_values_hierarchical_structure(self):
        """
        Building tree of hierarchy key table records by parent_id column
        """
        logger.info("build tree of enterprises for transfer process")

        key_table: DBTable = self._dst_database.tables.get(settings.KEY_TABLE_NAME)
        hierarchy_column = await key_table.get_column_by_name(
            column_name=settings.KEY_TABLE_HIERARCHY_COLUMN_NAME,
        )

        if hierarchy_column:
            coroutines = [
                asyncio.create_task(
                    self._get_key_table_parents_values(
                        key_table_primary_key_name=key_table.primary_key.name,
                        key_table_primary_key_value=key_column_value,
                    )
                )
                for key_column_value in copy(self._key_column_values)
            ]

            if coroutines:
                await asyncio.wait(coroutines)

        logger.info(
            f"transferring enterprises - "
            f"{make_str_from_iterable(self._key_column_values, with_quotes=True)}"  # noqa
        )

    async def _set_table_counters(self, table_name: str):
        """
        Filling table max pk and count of records
        """
        async with self._src_database.connection_pool.acquire() as connection:
            table = self._dst_database.tables[table_name]

            try:
                count_table_records_sql = (
                    SQLRepository.get_count_table_records(
                        primary_key=table.primary_key,
                    )
                )
            except AttributeError as e:
                logger.warning(
                    f'{str(e)} --- _set_table_counters {"-"*10} - '
                    f"{table.name}"
                )
                raise AttributeError
            except UndefinedFunctionError:
                raise UndefinedFunctionError

            async with connection.transaction():
                res = await connection.fetchrow(count_table_records_sql)

        if res and res[0] and res[1]:
            logger.debug(
                f"table {table_name} with full count {res[0]}, "
                f"max pk - {res[1]}"
            )

            table.full_count = int(res[0])

            table.max_pk = (
                int(res[1])
                if isinstance(res[1], int)
                else table.full_count + 100000
            )

        del count_table_records_sql

    async def _set_tables_counters(self):
        logger.info(
            'start filling tables max pk and count of records..'
        )

        coroutines = [
            asyncio.create_task(
                self._set_table_counters(table_name)
            )
            for table_name in sorted(self._dst_database.tables.keys())
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        logger.info('finished filling tables max pk and count of records.')

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
                await asyncio.wait(
                    [
                        asyncio.create_task(
                            fdw_wrapper.disable()
                        ),
                    ]
                )

                async with statistic_indexer(
                    self._statistic_manager,
                    TransferringStagesEnum.PREPARE_DST_DB_STRUCTURE,
                ):
                    await self._dst_database.prepare_structure()

                await self._dst_database.disable_triggers()

                await asyncio.wait(
                    [
                        asyncio.create_task(
                            self._build_key_column_values_hierarchical_structure()  # noqa
                        ),
                    ]
                )
                async with statistic_indexer(
                    self._statistic_manager,
                    TransferringStagesEnum.TRUNCATE_DST_DB_TABLES,
                ):
                    await self._dst_database.truncate_tables()

                await asyncio.wait(
                    [
                        asyncio.create_task(
                            fdw_wrapper.enable()
                        ),
                    ]
                )

                async with statistic_indexer(
                    self._statistic_manager,
                    TransferringStagesEnum.FILLING_TABLES_ROWS_COUNTS,
                ):
                    await self._set_tables_counters()

                collector_manager = CollectorManager(
                    src_database=self._src_database,
                    dst_database=self._dst_database,
                    statistic_manager=self._statistic_manager,
                    key_column_values=self._key_column_values,
                )
                await asyncio.wait(
                    [
                        asyncio.create_task(
                            collector_manager.manage()
                        ),
                    ]
                )

                transporter = Transporter(
                    dst_database=self._dst_database,
                    src_database=self._src_database,
                    statistic_manager=self._statistic_manager,
                    key_column_values=self._key_column_values,
                )

                async with statistic_indexer(
                    self._statistic_manager,
                    TransferringStagesEnum.PREPARING_AND_TRANSFERRING_DATA,
                ):
                    await asyncio.wait(
                        [
                            asyncio.create_task(
                                transporter.transfer()
                            ),
                        ]
                    )

                await self._dst_database.enable_triggers()

                await asyncio.wait(
                    [
                        asyncio.create_task(
                            fdw_wrapper.disable()
                        ),
                    ]
                )

                self._statistic_manager.print_transferring_indications()
                self._statistic_manager.print_records_transfer_statistic()

                if settings.TEST_MODE:
                    validator_manager = ValidatorManager(
                        dst_database=self._dst_database,
                        src_database=self._src_database,
                        statistic_manager=self._statistic_manager,
                        key_column_values=self._key_column_values,
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


class CollectorManager:
    """
    Manager of collectors tables records for transferring
    """
    collectors_classes: List[Type[BaseCollector]] = [
        KeyTableCollector,
        TablesWithKeyColumnSiblingsCollector,
        SortedByDependencyTablesCollector,
        GenericTablesCollector,
    ]

    def __init__(
        self,
        src_database: SrcDatabase,
        dst_database: DstDatabase,
        statistic_manager: StatisticManager,
        key_column_values: Set[int],
    ):
        self._dst_database = dst_database
        self._src_database = src_database
        self._key_column_values = key_column_values
        self._statistic_manager = statistic_manager

    async def manage(self):
        for collector_class in self.collectors_classes:
            collector = collector_class(
                src_database=self._src_database,
                dst_database=self._dst_database,
                statistic_manager=self._statistic_manager,
                key_column_values=self._key_column_values,
            )

            await collector.collect()
