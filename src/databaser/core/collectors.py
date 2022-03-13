import asyncio
from abc import (
    ABCMeta,
    abstractmethod,
)
from copy import (
    copy,
)
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

import asyncpg

from databaser.core.db_entities import (
    DBColumn,
    DBTable,
    DstDatabase,
    SrcDatabase,
)
from databaser.core.enums import (
    StagesEnum,
)
from databaser.core.helpers import (
    logger,
    make_chunks,
    make_str_from_iterable,
    topological_sort,
)
from databaser.core.loggers import (
    StatisticManager,
    statistic_indexer,
)
from databaser.core.repositories import (
    SQLRepository,
)
from databaser.settings import (
    EXCLUDED_TABLES,
    KEY_TABLE_NAME,
    TABLES_WITH_GENERIC_FOREIGN_KEY,
)


class BaseCollector(metaclass=ABCMeta):
    CHUNK_SIZE = 60000

    # Hashes of unique SQL-queries uses for excluding duplicate of queries
    QUERY_HASHES = set()

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

    async def _get_table_column_values_part(
        self,
        table_column_values_sql: str,
        table_column_values: List[Union[str, int]],
    ):
        if table_column_values_sql:
            logger.debug(table_column_values_sql)

            async with self._src_database.connection_pool.acquire() as connection:  # noqa
                try:
                    table_column_values_part = await connection.fetch(table_column_values_sql)  # noqa
                except (asyncpg.PostgresSyntaxError, asyncpg.UndefinedColumnError) as e:
                    logger.warning(
                        f"{str(e)} --- {table_column_values_sql} --- "
                        f"_get_table_column_values_part"
                    )
                    table_column_values_part = []

                filtered_table_column_values_part = [
                    record[0]
                    for record in table_column_values_part if
                    record[0] is not None
                ]

                table_column_values.extend(filtered_table_column_values_part)

                del table_column_values_part
                del table_column_values_sql

    async def _get_table_column_values(
        self,
        table: DBTable,
        column: DBColumn,
        primary_key_values: Iterable[Union[int, str]] = (),
        where_conditions_columns: Optional[Dict[str, Iterable[Union[int, str]]]] = None,  # noqa
        is_revert=False,
    ) -> Set[Union[str, int]]:
        # если таблица находится в исключенных, то ее записи не нужно
        # импортировать
        try:
            if column.constraint_table.name in EXCLUDED_TABLES:
                return set()
        except AttributeError as e:
            logger.warning(f"{str(e)} --- _get_table_column_values")
            return set()

        # формирование запроса на получения идентификаторов записей
        # внешней таблицы
        table_column_values_sql_list = await SQLRepository.get_table_column_values_sql(
            table=table,
            column=column,
            key_column_values=self._key_column_values,
            primary_key_values=primary_key_values,
            where_conditions_columns=where_conditions_columns,
            is_revert=is_revert,
        )
        table_column_values = []

        for table_column_values_sql in table_column_values_sql_list:
            sql_query_hash = hash(table_column_values_sql)

            if sql_query_hash not in self.__class__.QUERY_HASHES:
                BaseCollector.QUERY_HASHES.add(sql_query_hash)

                await self._get_table_column_values_part(
                    table_column_values_sql=table_column_values_sql,
                    table_column_values=table_column_values,
                )

        del table_column_values_sql_list[:]

        unique_table_column_values = set(table_column_values)

        del table_column_values[:]

        return unique_table_column_values

    @abstractmethod
    def collect(self):
        """
        Run collecting tables records for transferring
        """


class KeyTableCollector(BaseCollector):
    """
    Collector of key table records
    """

    async def _prepare_key_table_values(self):
        logger.info('prepare key table values...')

        key_table = self._dst_database.tables[KEY_TABLE_NAME]

        key_table.update_need_transfer_pks(
            need_transfer_pks=self._key_column_values,
        )

        key_table.is_ready_for_transferring = True

        logger.info('preparing key table values finished!')

    async def collect(self):
        await self._prepare_key_table_values()


class TablesWithKeyColumnSiblingsCollector(BaseCollector):
    """
    Collector of records of tables with key columns and their siblings
    """

    async def _direct_recursively_preparing_foreign_table_chunk(
        self,
        table: DBTable,
        column: DBColumn,
        need_transfer_pks_chunk: Iterable[int],
        stack_tables: Set[DBTable],
    ):
        """
        Direct recursively preparing foreign table chunk
        """
        foreign_table = column.constraint_table
        foreign_table.is_checked = True

        # Если таблица с key_column, то нет необходимости пробрасывать
        # идентификаторы записей
        if table.with_key_column:
            foreign_table_pks = await self._get_table_column_values(
                table=table,
                column=column,
            )
        else:
            need_transfer_pks = (
                need_transfer_pks_chunk if
                not table.is_full_prepared else
                ()
            )

            foreign_table_pks = await self._get_table_column_values(
                table=table,
                column=column,
                primary_key_values=need_transfer_pks,
            )

        # если найдены значения внешних ключей отличающиеся от null, то
        # записи из внешней талицы с этими идентификаторами должны быть
        # импортированы
        if foreign_table_pks:
            logger.debug(
                f"table - {table.name}, column - {column.name} - reversed "
                f"collecting of fk_ids ----- {foreign_table.name}"
            )

            foreign_table_pks_difference = foreign_table_pks.difference(
                foreign_table.need_transfer_pks
            )

            # если есть разница между предполагаемыми записями для импорта
            # и уже выбранными ранее, то разницу нужно импортировать
            if foreign_table_pks_difference:
                foreign_table.update_need_transfer_pks(
                    need_transfer_pks=foreign_table_pks_difference,
                )

                await asyncio.wait(
                    [
                        asyncio.create_task(
                            self._direct_recursively_preparing_table(
                                table=foreign_table,
                                need_transfer_pks=foreign_table_pks_difference,
                                stack_tables=stack_tables,
                            )
                        ),
                    ]
                )

            del foreign_table_pks_difference

        del foreign_table_pks
        del need_transfer_pks_chunk

    async def _direct_recursively_preparing_foreign_table(
        self,
        table: DBTable,
        column: DBColumn,
        need_transfer_pks: Iterable[int],
        stack_tables: Set[DBTable],
    ):
        """
        Recursively preparing foreign table
        """
        need_transfer_pks_chunks = make_chunks(
            iterable=need_transfer_pks,
            size=self.CHUNK_SIZE,
            is_list=True,
        )

        coroutines = [
            asyncio.create_task(
                self._direct_recursively_preparing_foreign_table_chunk(
                    table=table,
                    column=column,
                    need_transfer_pks_chunk=need_transfer_pks_chunk,
                    stack_tables=stack_tables,
                )
            )
            for need_transfer_pks_chunk in need_transfer_pks_chunks
        ]

        if coroutines:
            await asyncio.wait(coroutines)

    async def _direct_recursively_preparing_table_chunk(
        self,
        table: DBTable,
        need_transfer_pks_chunk: List[int],
        stack_tables: Optional[Set[DBTable]] = None,
    ):
        """
        Recursively preparing table
        """

        logger.debug(make_str_from_iterable([t.name for t in stack_tables]))

        coroutines = [
            asyncio.create_task(
                self._direct_recursively_preparing_foreign_table(
                    table=table,
                    column=column,
                    need_transfer_pks=need_transfer_pks_chunk,
                    stack_tables=stack_tables,
                )
            )
            for column in table.not_self_fk_columns if
            not (
                column.constraint_table.with_key_column or
                column.constraint_table in stack_tables
            )
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        del need_transfer_pks_chunk

    async def _direct_recursively_preparing_table(
        self,
        table: DBTable,
        need_transfer_pks: Iterable[int],
        stack_tables: Optional[Set[DBTable]] = None,
    ):
        """
        Recursively preparing table
        """
        if stack_tables is None:
            stack_tables = set()

        if table in stack_tables:
            return

        stack_tables.add(table)

        coroutines = [
            asyncio.create_task(
                self._direct_recursively_preparing_foreign_table(
                    table=table,
                    column=column,
                    need_transfer_pks=need_transfer_pks,
                    stack_tables=stack_tables,
                )
            )
            for column in table.not_self_fk_columns if
            not (
                column.constraint_table.with_key_column or
                column.constraint_table in stack_tables or
                column.constraint_table.is_ready_for_transferring
            )
        ]

        coroutines_hierarchy = [
            asyncio.create_task(
                self._direct_recursively_preparing_foreign_table(
                    table=table,
                    column=column,
                    need_transfer_pks=need_transfer_pks,
                    stack_tables=stack_tables - {table},
                )
            )
            for column in table.self_fk_columns if
            not (
                column.constraint_table.is_ready_for_transferring
            )
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        if coroutines_hierarchy:
            await asyncio.wait(coroutines_hierarchy)

        table.is_checked = True

        del stack_tables

    async def _revert_recursively_preparing_revert_table_column_chunk(
        self,
        revert_table: DBTable,
        revert_column: DBColumn,
        need_transfer_pks_chunk: Iterable[Union[int, str]],
    ):
        """
        Recursively preparing revert table column chunk
        """
        where_conditions_columns = {
            revert_column.name: need_transfer_pks_chunk,
        }

        revert_table_pks = await self._get_table_column_values(
            table=revert_table,
            column=revert_table.primary_key,
            where_conditions_columns=where_conditions_columns,
            is_revert=True,
        )

        if revert_table_pks:
            revert_table.update_need_transfer_pks(
                need_transfer_pks=revert_table_pks,
            )

        del need_transfer_pks_chunk
        del revert_table_pks

    async def _revert_recursively_preparing_revert_table_column(
        self,
        revert_table: DBTable,
        revert_column: DBColumn,
        need_transfer_pks: Set[Union[int, str]],
    ):
        """
        Recursively preparing revert table column
        """
        need_transfer_pks_chunks = make_chunks(
            iterable=need_transfer_pks,
            size=self.CHUNK_SIZE,
            is_list=True,
        )

        coroutines = [
            asyncio.create_task(
                self._revert_recursively_preparing_revert_table_column_chunk(
                    revert_table=revert_table,
                    revert_column=revert_column,
                    need_transfer_pks_chunk=need_transfer_pks_chunk,
                )
            )
            for need_transfer_pks_chunk in need_transfer_pks_chunks
        ]

        if coroutines:
            await asyncio.wait(coroutines)

    async def _revert_recursively_preparing_revert_table(
        self,
        revert_table: DBTable,
        revert_columns: Set[DBColumn],
        need_transfer_pks: Set[Union[int, str]],
        stack_tables: Set[DBTable],
    ):
        """
        Recursively preparing revert table
        """
        if need_transfer_pks:
            coroutines = [
                asyncio.create_task(
                    self._revert_recursively_preparing_revert_table_column(
                        revert_table=revert_table,
                        revert_column=revert_column,
                        need_transfer_pks=need_transfer_pks,
                    )
                )
                for revert_column in revert_columns if
                revert_column in revert_table.highest_priority_fk_columns
            ]

            if coroutines:
                await asyncio.wait(coroutines)

            if revert_table.need_transfer_pks:
                stack_tables_copy = copy(stack_tables)

                await self._revert_recursively_preparing_table(
                    table=revert_table,
                    stack_tables=stack_tables,
                )

                await self._direct_recursively_preparing_table(
                    table=revert_table,
                    need_transfer_pks=revert_table.need_transfer_pks,
                    stack_tables=stack_tables_copy,
                )

        del need_transfer_pks
        del stack_tables

    async def _revert_recursively_preparing_table(
        self,
        table: DBTable,
        stack_tables: Optional[Set[DBTable]] = None,
    ):
        """
        Revert recursively preparing table
        """
        if stack_tables is None:
            stack_tables = set()

        if table in stack_tables:
            return

        stack_tables.add(table)

        coroutines = [
            asyncio.create_task(
                self._revert_recursively_preparing_revert_table(
                    revert_table=revert_table,
                    revert_columns=revert_columns,
                    need_transfer_pks=table.need_transfer_pks,
                    stack_tables=stack_tables,
                )
            )
            for revert_table, revert_columns in table.revert_foreign_tables.items() if  # noqa
            not (
                revert_table.with_key_column or
                revert_table == table or
                revert_table in stack_tables or
                revert_table.is_ready_for_transferring
            )
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        table.is_checked = True

    async def _prepare_tables_with_key_column(
        self,
        table: DBTable,
    ):
        """
        Preparing tables with key column and siblings
        """
        logger.info(
            f'start preparing table with key column "{table.name}"'
        )

        if table.is_ready_for_transferring:
            return

        need_transfer_pks = await self._get_table_column_values(
            table=table,
            column=table.primary_key,
        )

        table.is_checked = True

        if need_transfer_pks:
            table.update_need_transfer_pks(
                need_transfer_pks=need_transfer_pks,
            )

            await asyncio.wait(
                [
                    asyncio.create_task(
                        self._direct_recursively_preparing_table(
                            table=table,
                            need_transfer_pks=need_transfer_pks,
                        )
                    ),
                ]
            )

            await asyncio.wait(
                [
                    asyncio.create_task(
                        self._revert_recursively_preparing_table(
                            table=table,
                        )
                    ),
                ]
            )

        del need_transfer_pks

        logger.info(
            f'finished preparing table with key column "{table.name}"'
        )

    async def collect(self):
        logger.info(
            'start preparing tables with key column and their siblings..'
        )
        coroutines = [
            asyncio.create_task(
                self._prepare_tables_with_key_column(table)
            )
            for table in self._dst_database.tables_with_key_column
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        for dst_table in self._dst_database.tables.values():
            if dst_table.is_checked:
                dst_table.is_ready_for_transferring = True

        logger.info(
            'finished preparing tables with key column and their siblings..'
        )


class SortedByDependencyTablesCollector(BaseCollector):
    """
    Collector of records of tables sorted by dependency between their
    """

    async def _get_revert_table_column_values(
        self,
        table: DBTable,
        revert_table: DBTable,
        revert_column: DBColumn,
    ):
        """
        Get revert table column values
        """
        revert_table_pks = (
            revert_table.need_transfer_pks if
            not revert_table.is_full_prepared else
            ()
        )

        revert_table_column_values = await self._get_table_column_values(
            table=revert_table,
            column=revert_column,
            primary_key_values=revert_table_pks,
            is_revert=True,
        )

        if revert_table_column_values:
            table.update_need_transfer_pks(
                need_transfer_pks=revert_table_column_values,
            )

        del revert_table_column_values

    async def _prepare_revert_table(
        self,
        table: DBTable,
        revert_table: DBTable,
        revert_columns: Set[DBColumn],
    ):
        """
        Preparing revert table
        """
        logger.info(f'prepare revert table {revert_table.name}')

        if (
            revert_table.fk_columns_with_key_column and
            not table.with_key_column
        ):
            return

        if revert_table.need_transfer_pks:

            coroutines = [
                asyncio.create_task(
                    self._get_revert_table_column_values(
                        table=table,
                        revert_table=revert_table,
                        revert_column=revert_column,
                    )
                )
                for revert_column in revert_columns
            ]

            if coroutines:
                await asyncio.wait(coroutines)

    async def _prepare_unready_table(
        self,
        table: DBTable,
    ):
        """
        Preparing table records for transferring
        """
        logger.info(
            f'start preparing table "{table.name}"'
        )
        # обход таблиц связанных через внешние ключи
        where_conditions_columns = {}

        fk_columns = table.highest_priority_fk_columns

        with_full_transferred_table = False

        for fk_column in fk_columns:
            logger.debug(f'prepare column {fk_column.name}')
            fk_table = self._dst_database.tables[
                fk_column.constraint_table.name
            ]

            if fk_table.need_transfer_pks:
                if not fk_table.is_full_prepared:
                    where_conditions_columns[fk_column.name] = (
                        fk_table.need_transfer_pks
                    )
                else:
                    with_full_transferred_table = True

        if (
            fk_columns and
            not where_conditions_columns and
            not with_full_transferred_table
        ):
            return

        table_pks = await self._get_table_column_values(
            table=table,
            column=table.primary_key,
            where_conditions_columns=where_conditions_columns,
        )

        if (
            fk_columns and
            where_conditions_columns and
            not table_pks
        ):
            return

        table.update_need_transfer_pks(
            need_transfer_pks=table_pks,
        )

        logger.debug(
            f'table "{table.name}" need transfer pks - '
            f'{len(table.need_transfer_pks)}'
        )

        del table_pks

        # обход таблиц ссылающихся на текущую таблицу
        logger.debug('prepare revert tables')

        coroutines = [
            asyncio.create_task(
                self._prepare_revert_table(
                    table=table,
                    revert_table=revert_table,
                    revert_columns=revert_columns,
                )
            )
            for revert_table, revert_columns in table.revert_foreign_tables.items()  # noqa
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        if not table.need_transfer_pks:
            all_records = await self._get_table_column_values(
                table=table,
                column=table.primary_key,
            )

            table.update_need_transfer_pks(
                need_transfer_pks=all_records,
            )

            del all_records

        table.is_ready_for_transferring = True

        logger.info(
            f'finished collecting records ids of table "{table.name}"'
        )

    async def collect(self):
        logger.info('start preparing tables sorted by dependency..')

        not_transferred_tables = list(
            filter(
                lambda t: (
                    not t.is_ready_for_transferring
                    and t.name not in TABLES_WITH_GENERIC_FOREIGN_KEY
                ),
                self._dst_database.tables.values(),
            )
        )
        logger.debug(
            f'tables not transferring {str(len(not_transferred_tables))}'
        )

        dependencies_between_models = []
        for table in self._dst_database.tables_without_generics:
            for fk_column in table.not_self_fk_columns:
                dependencies_between_models.append(
                    (table.name, fk_column.constraint_table.name)
                )

        sorted_dependencies_result = topological_sort(
            dependency_pairs=dependencies_between_models,
        )
        sorted_dependencies_result.cyclic.reverse()
        sorted_dependencies_result.sorted.reverse()

        sorted_tables_by_dependency = (
            sorted_dependencies_result.cyclic + sorted_dependencies_result.sorted
        )

        without_relatives = list(
            {
                table.name
                for table in self._dst_database.tables_without_generics
            }.difference(
                sorted_tables_by_dependency
            )
        )

        sorted_tables_by_dependency = without_relatives + sorted_tables_by_dependency

        # Явно ломаю асинхронность, т.к. порядок импорта таблиц важен
        for table_name in sorted_tables_by_dependency:
            table = self._dst_database.tables[table_name]

            if not table.is_ready_for_transferring:
                await self._prepare_unready_table(
                    table=table,
                )

        logger.info('preparing tables sorted by dependency finished.')


class GenericTablesCollector(BaseCollector):
    """
    Класс комплексной транспортировки, который использует принципы обхода по
    внешним ключам и по таблицам с обратной связью
    """

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(
            *args,
            **kwargs,
        )
        # словарь с названиями таблиц и идентификаторами импортированных записей
        self._transfer_progress_dict = {}
        self.filling_tables = set()

        self.content_type_table = {}

    async def _prepare_content_type_tables(self):
        """
        Подготавливает соответствие content_type_id и наименование таблицы в БД
        """
        logger.info("prepare content type tables")

        content_type_table_list = await self._dst_database.fetch_raw_sql(
            SQLRepository.get_content_type_table_sql()
        )

        content_type_table_dict = {
            (app_label, model): table_name
            for table_name, app_label, model in content_type_table_list
        }

        content_type_list = await self._src_database.fetch_raw_sql(
            SQLRepository.get_content_type_sql()
        )

        content_type_dict = {
            (app_label, model): content_type_id
            for content_type_id, app_label, model in content_type_list
        }

        for key in content_type_table_dict.keys():
            self.content_type_table[content_type_table_dict[key]] = (
                content_type_dict[key]
            )

        del content_type_table_list[:]
        del content_type_table_dict
        del content_type_list[:]
        del content_type_dict

    async def _prepare_content_type_generic_data(
        self,
        target_table: DBTable,
        rel_table_name: str,
    ):
        if not rel_table_name:
            logger.debug('not send rel_table_name')
            return

        rel_table = self._dst_database.tables.get(rel_table_name)

        if not rel_table:
            logger.debug(f'table {rel_table_name} not found')
            return

        object_id_column = await target_table.get_column_by_name('object_id')

        if rel_table.primary_key.data_type != object_id_column.data_type:
            logger.debug(
                f'pk of table {rel_table_name} has an incompatible data type'
            )
            return

        logger.info('prepare content type generic data')

        where_conditions = {
            'object_id': rel_table.need_transfer_pks,
            'content_type_id': [self.content_type_table[rel_table.name]],
        }

        need_transfer_pks = await self._get_table_column_values(
            table=target_table,
            column=target_table.primary_key,
            where_conditions_columns=where_conditions,
        )

        logger.info(
            f'{target_table.name} need transfer pks {len(need_transfer_pks)}'
        )

        target_table.update_need_transfer_pks(
            need_transfer_pks=need_transfer_pks,
        )

        del where_conditions
        del need_transfer_pks

    async def _prepare_generic_table_data(self, target_table: DBTable):
        """
        Перенос данных из таблицы, содержащей generic foreign key
        """
        logger.info(f"prepare generic table data {target_table.name}")

        coroutines = [
            asyncio.create_task(
                self._prepare_content_type_generic_data(
                    target_table=target_table, rel_table_name=rel_table_name
                )
            )
            for rel_table_name in self.content_type_table.keys()
        ]

        if coroutines:
            await asyncio.wait(coroutines)

    async def _collect_generic_tables_records_ids(self):
        """
        Собирает идентификаторы записей таблиц, содержащих generic key
        Предполагается, что такие таблицы имеют поля object_id и content_type_id
        """
        logger.info("collect generic tables records ids")

        await asyncio.wait(
            [
                asyncio.create_task(
                    self._prepare_content_type_tables()
                ),
            ]
        )

        generic_table_names = set(TABLES_WITH_GENERIC_FOREIGN_KEY).difference(EXCLUDED_TABLES)

        coroutines = [
            asyncio.create_task(
                self._prepare_generic_table_data(
                    self._dst_database.tables.get(table_name)
                )
            )
            for table_name in filter(None, generic_table_names)
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        logger.info("finish collecting")

    async def collect(self):
        logger.info('start preparing generic tables..')

        async with statistic_indexer(
            self._statistic_manager,
            StagesEnum.COLLECT_GENERIC_TABLES_RECORDS_IDS
        ):
            await asyncio.wait(
                [
                    asyncio.create_task(
                        self._collect_generic_tables_records_ids()
                    ),
                ]
            )

        logger.info('preparing generic tables finished.')
