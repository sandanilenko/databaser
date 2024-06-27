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
import uvloop
from asyncpg import (
    UndefinedFunctionError,
)

from databaser.core.collectors import (
    BaseCollector,
    FullTransferCollector,
    GenericTablesCollector,
    KeyTableCollector,
    SortedByDependencyTablesCollector,
    TablesWithKeyColumnSiblingsCollector,
)
from databaser.core.db_entities import (
    DBTable,
    DstDatabase,
    SrcDatabase,
)
from databaser.core.enums import (
    StagesEnum,
)
from databaser.core.helpers import (
    DBConnectionParameters,
    logger,
    make_str_from_iterable, execute_async_function_for_collection,
)
from databaser.core.loggers import (
    StatisticManager,
    statistic_indexer,
)
from databaser.core.repositories import (
    SQLRepository,
)
from databaser.core.storages import StorageDataTable
from databaser.core.transporters import (
    Transporter,
)
from databaser.core.validators import (
    ValidatorManager,
)
from databaser.core.wrappers import (
    PostgresFDWExtensionWrapper,
)
from databaser.settings import (
    DST_DB_HOST,
    DST_DB_NAME,
    DST_DB_PASSWORD,
    DST_DB_PORT,
    DST_DB_SCHEMA,
    DST_DB_USER,
    EXCLUDED_TABLES,
    KEY_COLUMN_VALUES,
    KEY_TABLE_HIERARCHY_COLUMN_NAME,
    KEY_TABLE_NAME,
    SRC_DB_HOST,
    SRC_DB_NAME,
    SRC_DB_PASSWORD,
    SRC_DB_PORT,
    SRC_DB_SCHEMA,
    SRC_DB_USER,
    TEST_MODE, USE_DATABASE_FOR_STORE_INTERMEDIATE_VALUES,
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
            host=SRC_DB_HOST,
            port=SRC_DB_PORT,
            schema=SRC_DB_SCHEMA,
            dbname=SRC_DB_NAME,
            user=SRC_DB_USER,
            password=SRC_DB_PASSWORD,
        )

        self._dst_db_connection_parameters = DBConnectionParameters(
            host=DST_DB_HOST,
            port=DST_DB_PORT,
            schema=DST_DB_SCHEMA,
            dbname=DST_DB_NAME,
            user=DST_DB_USER,
            password=DST_DB_PASSWORD,
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

        self._key_column_values = set(KEY_COLUMN_VALUES)

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
                with recursive hierarchy("{key_table_primary_key_name}", "{KEY_TABLE_HIERARCHY_COLUMN_NAME}", "level") as (
                    select "{KEY_TABLE_NAME}"."{key_table_primary_key_name}", 
                        "{KEY_TABLE_NAME}"."{KEY_TABLE_HIERARCHY_COLUMN_NAME}", 
                        0 
                    from "{KEY_TABLE_NAME}" 
                    where "{KEY_TABLE_NAME}"."{key_table_primary_key_name}" = {key_table_primary_key_value}
        
                    union all
        
                    select
                        "{KEY_TABLE_NAME}"."{key_table_primary_key_name}",
                        "{KEY_TABLE_NAME}"."{KEY_TABLE_HIERARCHY_COLUMN_NAME}",
                        "hierarchy"."level" + 1
                    from "{KEY_TABLE_NAME}" 
                    join "hierarchy" on "{KEY_TABLE_NAME}"."{key_table_primary_key_name}" = "hierarchy"."{KEY_TABLE_HIERARCHY_COLUMN_NAME}"
                )
                select "{KEY_TABLE_NAME}"."{key_table_primary_key_name}" {key_table_primary_key_name} 
                from "{KEY_TABLE_NAME}" 
                join "hierarchy" on "{KEY_TABLE_NAME}"."{key_table_primary_key_name}" = "hierarchy"."{key_table_primary_key_name}"
                where "{KEY_TABLE_NAME}"."{key_table_primary_key_name}" <> {key_table_primary_key_value}
                order by "hierarchy"."level" desc;
            """

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
        Построение дерева иерархии записей ключвой таблицы по parent_id столбцу
        """

        logger.info("build tree of enterprises for transfer process")

        key_table: DBTable = self._dst_database.tables.get(KEY_TABLE_NAME)
        hierarchy_column = await key_table.get_column_by_name(
            column_name=KEY_TABLE_HIERARCHY_COLUMN_NAME,
        )

        if hierarchy_column:
            async def partial_getting(value: int):
                await self._get_key_table_parents_values(
                    key_table_primary_key_name=key_table.primary_key.name,
                    key_table_primary_key_value=value
                )
            await execute_async_function_for_collection(partial_getting, copy(self._key_column_values))

        logger.info(
            f"transferring enterprises - "
            f"{make_str_from_iterable(self._key_column_values, with_quotes=True)}"  # noqa
        )

    async def _set_table_counters(self, table_name: str):
        """
        Заполнение максимального pk и количество записей
        """

        async with self._src_database.connection_pool.acquire() as connection:
            table = self._dst_database.tables[table_name]
            if table.primary_key is None:
                raise UndefinedFunctionError
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
        Запуск асинхронного Databaser
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

                if USE_DATABASE_FOR_STORE_INTERMEDIATE_VALUES:
                    await StorageDataTable.init_table(self._dst_database)

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
                await fdw_wrapper.disable()

                await self._dst_database.prepare_partition_names()

                if self._dst_database.partition_names:
                    logger.info(f'dst_database partitions - {", ".join(self._dst_database.partition_names)}')
                    EXCLUDED_TABLES.extend(self._dst_database.partition_names)

                async with statistic_indexer(
                    self._statistic_manager,
                    StagesEnum.PREPARE_DST_DB_STRUCTURE,
                ):
                    await self._dst_database.prepare_structure()

                await self._dst_database.disable_triggers()

                await self._build_key_column_values_hierarchical_structure()  # noqa

                async with statistic_indexer(
                    self._statistic_manager,
                    StagesEnum.TRUNCATE_DST_DB_TABLES,
                ):
                    await self._dst_database.truncate_tables()

                await fdw_wrapper.enable()

                async with statistic_indexer(
                    self._statistic_manager,
                    StagesEnum.FILLING_TABLES_ROWS_COUNTS,
                ):
                    await self._set_tables_counters()

                collector_manager = CollectorManager(
                    src_database=self._src_database,
                    dst_database=self._dst_database,
                    statistic_manager=self._statistic_manager,
                    key_column_values=self._key_column_values,
                )

                await collector_manager.manage()

                transporter = Transporter(
                    dst_database=self._dst_database,
                    src_database=self._src_database,
                    statistic_manager=self._statistic_manager,
                    key_column_values=self._key_column_values,
                )

                async with statistic_indexer(
                    self._statistic_manager,
                    StagesEnum.PREPARING_AND_TRANSFERRING_DATA,
                ):
                    await transporter.transfer()

                await self._dst_database.enable_triggers()

                await fdw_wrapper.disable()

                self._statistic_manager.print_stages_indications()
                await self._statistic_manager.print_records_transfer_statistic()

                if USE_DATABASE_FOR_STORE_INTERMEDIATE_VALUES:
                    await StorageDataTable.drop_table()

                if TEST_MODE:
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

        uvloop.install()
        asyncio.run(
            self._main(),
            debug=TEST_MODE,
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
        FullTransferCollector,
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
