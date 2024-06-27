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
    Optional,
    Set,
    Union,
)

import asyncpg
from asyncpg import DatetimeFieldOverflowError

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
    topological_sort, execute_async_function_for_collection, execute_async_function_for_async_collection,
)
from databaser.core.loggers import (
    StatisticManager,
    statistic_indexer,
)
from databaser.core.repositories import (
    SQLRepository,
)
from databaser.core.storages import AbstractStorage, create_storage
from databaser.settings import (
    EXCLUDED_TABLES,
    FULL_TRANSFER_TABLES,
    KEY_TABLE_NAME,
    TABLES_WITH_GENERIC_FOREIGN_KEY, COLLECTOR_CHUNK_SIZE,
)


class BaseCollector(metaclass=ABCMeta):
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
            table_column_values: AbstractStorage,
    ):
        """
        Добавляет в хранилище результат sql запроса

        Args:
            table_column_values_sql: sql запрос
            table_column_values: Хранилище для заполнения
        """

        if table_column_values_sql:
            logger.debug(table_column_values_sql)
            try:
                async for data in self._src_database.get_iter(table_column_values_sql,
                                                              chunk_size=COLLECTOR_CHUNK_SIZE):
                    await table_column_values.insert([i for i in data if i is not None])

            except (asyncpg.PostgresSyntaxError, asyncpg.UndefinedColumnError) as e:
                logger.warning(
                    f"{str(e)} --- {table_column_values_sql[:100]} --- "
                    f"_get_table_column_values_part"
                )

    async def _get_table_column_values(
            self,
            table: DBTable,
            column: DBColumn,
            primary_key_values: Iterable[Union[int, str]] = (),
            where_conditions_columns: Optional[Dict[str, Iterable[Union[int, str]]]] = None,  # noqa
            is_revert=False,
    ) -> AbstractStorage:
        """
        Возвращает данные столбца для указанных строк

        Args:
            table: Таблица, для которой получаем данные
            column: Столбец
            primary_key_values: Id строк, для которых получаем значения
            where_conditions_columns: Дополнительные условия фильтрации
            is_revert: является ли column внешним ключом на table
        Returns:
            Хранилище с результатами. Если таблица в списке исключённых, то хранилище будет пустым
        """

        result = create_storage()
        in_excluded = False

        # если таблица находится в исключенных, то ее записи не нужно
        # импортировать
        try:
            if column.constraint_table.name in EXCLUDED_TABLES:
                in_excluded = True
        except AttributeError as e:
            logger.warning(f"{str(e)} --- _get_table_column_values")
            in_excluded = True

        if not in_excluded:
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

            for table_column_values_sql in table_column_values_sql_list:
                sql_query_hash = hash(table_column_values_sql)

                if sql_query_hash not in self.__class__.QUERY_HASHES:
                    BaseCollector.QUERY_HASHES.add(sql_query_hash)
                    try:
                        await self._get_table_column_values_part(
                            table_column_values_sql=table_column_values_sql,
                            table_column_values=result,
                        )
                    except DatetimeFieldOverflowError as e:
                        logger.warning(f"Failed to get table column value {table.name}: {e}")
                        break

        return result

    @abstractmethod
    def collect(self):
        """
        Запускает подготовку записей для переноса
        """


class KeyTableCollector(BaseCollector):
    """
    Collector of key table records
    """

    async def _prepare_key_table_values(self):
        """
        Подготавливает к переносу ключевую таблицу
        """

        logger.info('prepare key table values...')

        key_table = self._dst_database.tables[KEY_TABLE_NAME]

        await key_table.update_need_transfer_pks(
            need_transfer_pks=self._key_column_values,
        )

        key_table.is_ready_for_transferring = True

        logger.info('preparing key table values finished!')

    async def collect(self):
        await self._prepare_key_table_values()


class FullTransferCollector(BaseCollector):
    """
    Сборщик записей таблиц требующие полного переноса данных
    """

    async def _prepare_full_transfer_table(self, table: DBTable):
        """
        Обработка таблицы с полным переносом записей

        Args:
            table: таблица для полного переноса
        """

        logger.info(
            f'start preparing full transfer table "{table.name}"'
        )

        if table.is_ready_for_transferring:
            return

        need_transfer_pks = await self._get_table_column_values(
            table=table,
            column=table.primary_key,
        )

        table.is_checked = True

        await table.update_need_transfer_pks(
            need_transfer_pks=need_transfer_pks,
        )

        await need_transfer_pks.delete()

        logger.info(
            f'finished preparing full transfer table "{table.name}"'
        )

    async def collect(self):
        logger.info(
            'start preparing full transfer tables..'
        )

        tables = [table for table in self._dst_database.tables.values() if table.name in FULL_TRANSFER_TABLES]

        await execute_async_function_for_collection(self._prepare_full_transfer_table, tables)

        for table in tables:
            if table.is_checked:
                table.is_ready_for_transferring = True

        logger.info(
            'finished preparing full transfer tables..'
        )


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
        Рекурсивное получение и обработка части записей из таблицы
        Если в таблице имеется ссылка на ключевую таблицу, то фильтрация происходит по ней, игнорируя переданные id

        Args:
            table: Таблица, для которой получаем данные
            column: Столбец
            need_transfer_pks_chunk: Id строк, для которых получаем значения
            stack_tables: Набор таблиц в рекурсии над которыми ведётся работа
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
                not await table.is_full_prepared() else
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

        # если есть разница между предполагаемыми записями для импорта
        # и уже выбранными ранее, то разницу нужно импортировать
        async for chunk in foreign_table_pks.iter_difference(foreign_table.need_transfer_pks):
            await foreign_table.update_need_transfer_pks(chunk)
            await self._direct_recursively_preparing_table(
                table=foreign_table,
                need_transfer_pks=chunk,
                stack_tables=stack_tables
            )

        await foreign_table_pks.delete()

    async def _direct_recursively_preparing_foreign_table(
            self,
            table: DBTable,
            column: DBColumn,
            need_transfer_pks: Iterable[Union[int, str]],
            stack_tables: Set[DBTable],
    ):
        """
        Рекурсивное получение и обработка части записей из таблицы с разбитием на чанки

        Args:
            table: Таблица, для которой получаем данные
            column: Столбец
            need_transfer_pks: Id строк, для которых получаем значения
            stack_tables: Набор таблиц в рекурсии над которыми ведётся работа
        """

        need_transfer_pks_chunks = make_chunks(
            iterable=need_transfer_pks,
            size=COLLECTOR_CHUNK_SIZE,
            is_list=True,
        )

        async def partial_preparing(chunk: Iterable):
            await self._direct_recursively_preparing_foreign_table_chunk(
                table=table,
                column=column,
                need_transfer_pks_chunk=chunk,
                stack_tables=stack_tables,
            )
        await execute_async_function_for_collection(partial_preparing, need_transfer_pks_chunks)

    async def _direct_recursively_preparing_table(
            self,
            table: DBTable,
            need_transfer_pks: Iterable[Union[int, str]],
            stack_tables: Optional[Set[DBTable]] = None,
    ):
        """
        Рекурсивное получение и обработка части записей из таблицы.
        При этом идёт обработка таблиц, которые ссылаются на полученные данные,
        и таблиц на которые ссылаются полученные данные

        Args:
            table: Таблица, для которой получаем данные
            need_transfer_pks: Id строк, для которых получаем значения
            stack_tables: Набор таблиц в рекурсии над которыми ведётся работа
        """

        if stack_tables is None:
            stack_tables = set()

        if table in stack_tables:
            return

        stack_tables.add(table)

        async def partial_preparing(column: DBColumn):
            await self._direct_recursively_preparing_foreign_table(
                table=table,
                column=column,
                need_transfer_pks=need_transfer_pks,
                stack_tables=stack_tables,
            )
        columns = [
            column for column in table.not_self_fk_columns
            if not (
                    column.constraint_table.with_key_column or
                    column.constraint_table in stack_tables or
                    column.constraint_table.is_ready_for_transferring
            )
        ]
        await execute_async_function_for_collection(partial_preparing, columns)

        async def partial_preparing(column: DBColumn):
            await self._direct_recursively_preparing_foreign_table(
                table=table,
                column=column,
                need_transfer_pks=need_transfer_pks,
                stack_tables=stack_tables - {table},
            )
        columns = [
            column for column in table.self_fk_columns
            if not column.constraint_table.is_ready_for_transferring
        ]
        await execute_async_function_for_collection(partial_preparing, columns)

        table.is_checked = True

        del stack_tables

    async def _revert_recursively_preparing_revert_table_column_chunk(
            self,
            revert_table: DBTable,
            revert_column: DBColumn,
            need_transfer_pks_chunk: Iterable[Union[int, str]],
    ):
        """
        Рекурсивное получение и обработка части записей из таблицы

         Args:
            revert_table: Таблица, для которой получаем данные
            revert_column: Столбец, ссылающийся на ранее обрабатываемую таблицу
            need_transfer_pks_chunk: Значения столбца, для которых получаем значения
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

        await revert_table.update_need_transfer_pks(
            need_transfer_pks=revert_table_pks,
        )

        del need_transfer_pks_chunk
        await revert_table_pks.delete()

    async def _revert_recursively_preparing_revert_table_column(
            self,
            revert_table: DBTable,
            revert_column: DBColumn,
            need_transfer_pks: Iterable[Union[int, str]],
    ):
        """
        Рекурсивное получение и обработка части записей из таблицы с разбитием на чанки

        Args:
            revert_table: Таблица, для которой получаем данные
            revert_column: Столбец, ссылающийся на ранее обрабатываемую таблицу
            need_transfer_pks: Значения столбца, для которых получаем значения
        """

        need_transfer_pks_chunks = make_chunks(
            iterable=need_transfer_pks,
            size=COLLECTOR_CHUNK_SIZE,
            is_list=True,
        )

        async def partial_preparing(chunk: Iterable):
            await self._revert_recursively_preparing_revert_table_column_chunk(
                revert_table=revert_table,
                revert_column=revert_column,
                need_transfer_pks_chunk=chunk
            )

        await execute_async_function_for_collection(partial_preparing, need_transfer_pks_chunks)

    async def _revert_recursively_preparing_revert_table(
            self,
            revert_table: DBTable,
            revert_columns: Set[DBColumn],
            need_transfer_pks: Iterable[Union[int, str]],
            stack_tables: Set[DBTable],
    ):
        """
        Обработка таблиц ссылающихся на ранее обрабатываемую таблицу

        Args:
            revert_table: Таблица
            revert_columns: Столбец ссылающийся на ранее обрабатываемую таблицу
            need_transfer_pks: Значения столбца для получения строк
            stack_tables: Набор таблиц в рекурсии над которыми ведётся работа
        """

        if need_transfer_pks:
            async def partial_preparing(column: DBColumn):
                await self._revert_recursively_preparing_revert_table_column(
                    revert_column=column,
                    revert_table=revert_table,
                    need_transfer_pks=need_transfer_pks,
                )
            columns = [
                revert_column for revert_column in revert_columns
                if revert_column in revert_table.highest_priority_fk_columns
            ]

            await execute_async_function_for_collection(partial_preparing, columns)

            if await revert_table.need_transfer_pks.is_not_empty():
                stack_tables_copy = copy(stack_tables)

                await self._revert_recursively_preparing_table(
                    table=revert_table,
                    stack_tables=stack_tables,
                )

                async def partial_preparing(chunk: Iterable):
                    await self._direct_recursively_preparing_table(
                        table=revert_table,
                        need_transfer_pks=chunk,
                        stack_tables=stack_tables_copy,
                    )
                await execute_async_function_for_async_collection(partial_preparing, revert_table.need_transfer_pks)

        del need_transfer_pks
        del stack_tables

    async def _revert_recursively_preparing_table(
            self,
            table: DBTable,
            stack_tables: Optional[Set[DBTable]] = None,
    ):
        """
        Обработка таблиц ссылающихся на переданную

        Args:
            table: Таблица
            stack_tables: Набор таблиц в рекурсии над которыми ведётся работа
        """

        if stack_tables is None:
            stack_tables = set()

        if table in stack_tables:
            return

        stack_tables.add(table)

        for revert_table, revert_columns in table.revert_foreign_tables.items():
            if not (
                    revert_table.with_key_column or
                    revert_table == table or
                    revert_table in stack_tables or
                    revert_table.is_ready_for_transferring
            ):
                async def partial_preparing(chunk: Iterable):
                    await self._revert_recursively_preparing_revert_table(
                        revert_table=revert_table,
                        revert_columns=revert_columns,
                        stack_tables=stack_tables,
                        need_transfer_pks=chunk
                    )
                await execute_async_function_for_async_collection(partial_preparing, table.need_transfer_pks)

        table.is_checked = True

    async def _prepare_tables_with_key_column(
            self,
            table: DBTable,
    ):
        """
        Подготовка таблицы со ссылкой на ключевую таблицу и рекурсивная подготовка связанных данных

        Args:
             table: Таблица
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

        if await need_transfer_pks.is_not_empty():
            await table.update_need_transfer_pks(
                need_transfer_pks=need_transfer_pks,
            )

            async def partial_preparing(chunk: Iterable):
                await self._direct_recursively_preparing_table(
                    table=table,
                    need_transfer_pks=chunk
                )
            await execute_async_function_for_async_collection(partial_preparing, need_transfer_pks)

            await self._revert_recursively_preparing_table(table=table)

        await need_transfer_pks.delete()

        logger.info(
            f'finished preparing table with key column "{table.name}"'
        )

    async def collect(self):
        logger.info(
            'start preparing tables with key column and their siblings..'
        )

        await execute_async_function_for_collection(
            self._prepare_tables_with_key_column,
            self._dst_database.tables_with_key_column
        )

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
        Обработка таблицы ссылающийся на текущую таблицу

        Args:
            table: Текущая таблица
            revert_table: Таблица, которая ссылается на текущую
            revert_column: Столбец со ссылкой на текущую таблицу
        """

        async def update_chunk_of_pks(chunk):
            result = await self._get_table_column_values(
                table=revert_table,
                column=revert_column,
                primary_key_values=chunk,
                is_revert=True
            )
            await table.update_need_transfer_pks(result)
            await result.delete()

        if not await revert_table.is_full_prepared():
            await execute_async_function_for_async_collection(update_chunk_of_pks, revert_table.need_transfer_pks)
        else:
            await update_chunk_of_pks(())

    async def _prepare_revert_table(
            self,
            table: DBTable,
            revert_table: DBTable,
            revert_columns: Set[DBColumn],
    ):
        """
        Обработка таблицы ссылающийся на текущую таблицу

        Args:
            table: Текущая таблица
            revert_table: Таблица, которая ссылается на текущую
            revert_columns: Столбцы со ссылкой на текущую таблицу

        """

        logger.info(f'prepare revert table {revert_table.name}')

        if (
                revert_table.fk_columns_with_key_column and
                not table.with_key_column
        ):
            return

        if revert_table.need_transfer_pks:
            async def partial_getting(column: DBColumn):
                await self._get_revert_table_column_values(
                    table=table,
                    revert_table=revert_table,
                    revert_column=column
                )

            await execute_async_function_for_collection(partial_getting, revert_columns)

    async def _prepare_unready_table(
            self,
            table: DBTable,
    ):
        """
        Обрабатывает таблицу не имеющей связи ссылками с ключевой

        Args:
             table: Таблица
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

            if await fk_table.need_transfer_pks.is_not_empty():
                if not await fk_table.is_full_prepared():
                    where_conditions_columns[fk_column.name] = (
                        await fk_table.need_transfer_pks.all()
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
                not await table_pks.is_not_empty()
        ):
            return

        await table.update_need_transfer_pks(
            need_transfer_pks=table_pks,
        )

        logger.debug(
            f'table "{table.name}" need transfer pks - '
            f'{await table.need_transfer_pks.len()}'
        )

        await table_pks.delete()

        # обход таблиц ссылающихся на текущую таблицу
        logger.debug('prepare revert tables')

        async def partial_preparing(revert_table):
            await self._prepare_revert_table(
                table=table,
                revert_table=revert_table,
                revert_columns=table.revert_foreign_tables[revert_table],
            )

        await execute_async_function_for_collection(partial_preparing, table.revert_foreign_tables)

        if not table.need_transfer_pks:
            all_records = await self._get_table_column_values(
                table=table,
                column=table.primary_key,
            )

            await table.update_need_transfer_pks(
                need_transfer_pks=all_records,
            )

            await all_records.delete()

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
            'object_id': await rel_table.need_transfer_pks.all(),
            'content_type_id': [self.content_type_table[rel_table.name]],
        }

        need_transfer_pks = await self._get_table_column_values(
            table=target_table,
            column=target_table.primary_key,
            where_conditions_columns=where_conditions,
        )

        await target_table.update_need_transfer_pks(
            need_transfer_pks=need_transfer_pks,
        )

        await need_transfer_pks.delete()

    async def _prepare_generic_table_data(self, target_table: DBTable):
        """
        Перенос данных из таблицы, содержащей generic foreign key
        """

        logger.info(f"prepare generic table data {target_table.name}")

        async def partial_preparing(table_name: str):
            await self._prepare_content_type_generic_data(
                target_table=target_table,
                rel_table_name=table_name
            )
        await execute_async_function_for_collection(partial_preparing, self.content_type_table.keys())

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

        await execute_async_function_for_collection(self._prepare_generic_table_data, filter(None, generic_table_names))

        logger.info("finish collecting")

    async def collect(self):
        logger.info('start preparing generic tables..')

        async with statistic_indexer(
                self._statistic_manager,
                StagesEnum.COLLECT_GENERIC_TABLES_RECORDS_IDS
        ):
            await self._collect_generic_tables_records_ids()

        logger.info('preparing generic tables finished.')
