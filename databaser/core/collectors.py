import asyncio
from abc import (
    ABCMeta,
    abstractmethod,
)
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import asyncpg

import settings
from core.db_entities import (
    DBColumn,
    DBTable,
    DstDatabase,
    SrcDatabase,
)
from core.enums import (
    ConstraintTypesEnum,
    TransferringStagesEnum,
)
from core.helpers import (
    logger,
    make_chunks,
    make_str_from_iterable,
    topological_sort,
)
from core.loggers import (
    StatisticIndexer,
    StatisticManager,
)
from core.repositories import (
    SQLRepository,
)


class BaseCollector(metaclass=ABCMeta):
    CHUNK_SIZE = 70000

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
                except asyncpg.PostgresSyntaxError as e:
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
        where_conditions_columns: Optional[Dict[str, Set[Union[int, str]]]] = None,  # noqa
        is_revert=False,
    ) -> Set[Union[str, int]]:
        # если таблица находится в исключенных, то ее записи не нужно
        # импортировать
        try:
            if column.constraint_table.name in settings.EXCLUDED_TABLES:
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

        key_table = self._dst_database.tables[settings.KEY_TABLE_NAME]

        key_table.need_transfer_pks.update(self._key_column_values)

        key_table.is_ready_for_transferring = True

        logger.info('preparing key table values finished!')

    async def collect(self):
        await self._prepare_key_table_values()


class TablesWithKeyColumnSiblingsCollector(BaseCollector):
    """
    Collector of records of tables with key columns and their siblings
    """

    async def _recursively_preparing_foreign_table_chunk(
        self,
        foreign_table: DBTable,
        foreign_table_pks_chunk: List[int],
        stack_tables: Tuple[str],
        deep_without_key_table: int,
    ):
        """
        Recursively preparing foreign table chunk
        """
        dwkt = (
            deep_without_key_table - 1 if
            not foreign_table.with_key_column else
            deep_without_key_table
        )

        await self._recursively_preparing_table(
            table=foreign_table,
            need_transfer_pks=foreign_table_pks_chunk,
            stack_tables=stack_tables,
            deep_without_key_table=dwkt,
        )

        del dwkt
        del foreign_table_pks_chunk[:]

    async def _recursively_preparing_foreign_table(
        self,
        table: DBTable,
        column: DBColumn,
        need_transfer_pks: Iterable[int],
        stack_tables: Tuple[str],
        deep_without_key_table: int,
    ):
        """
        Recursively preparing foreign table
        """
        foreign_table = self._dst_database.tables[column.constraint_table.name]
        foreign_table.is_checked = True

        # если таблица уже есть в стеке импорта таблиц, то он нас не
        # интересует; если талица с key_column, то записи в любом случае
        # будут импортированы
        if (
            foreign_table in stack_tables or
            foreign_table.with_key_column
        ):
            return

        # Если таблица с key_column, то нет необходимости пробрасывать
        # идентификаторы записей
        if table.with_key_column:
            foreign_table_pks = await self._get_table_column_values(
                table=table,
                column=column,
            )
        else:
            need_transfer_pks = (
                need_transfer_pks if
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
                foreign_table.need_transfer_pks.update(
                    foreign_table_pks_difference
                )

                foreign_table_pks_difference_chunks = make_chunks(
                    iterable=foreign_table_pks_difference,
                    size=self.CHUNK_SIZE,
                    is_list=True,
                )

                coroutines = [
                    self._recursively_preparing_foreign_table_chunk(
                        foreign_table=foreign_table,
                        foreign_table_pks_chunk=foreign_table_pks_difference_chunk,  # noqa
                        stack_tables=stack_tables,
                        deep_without_key_table=deep_without_key_table,
                    )
                    for foreign_table_pks_difference_chunk in foreign_table_pks_difference_chunks  # noqa
                ]

                if coroutines:
                    await asyncio.wait(coroutines)

            del foreign_table_pks_difference

        del foreign_table_pks

    async def _recursively_preparing_table(
        self,
        table: DBTable,
        need_transfer_pks: List[int],
        stack_tables=(),
        deep_without_key_table=None,
    ):
        """
        Recursively preparing table
        """
        if not deep_without_key_table:
            logger.debug("Max deep without key table")
            return

        stack_tables += (table,)

        logger.debug(make_str_from_iterable([t.name for t in stack_tables]))

        coroutines = [
            self._recursively_preparing_foreign_table(
                table=table,
                column=column,
                need_transfer_pks=need_transfer_pks,
                stack_tables=stack_tables,
                deep_without_key_table=deep_without_key_table,
            )
            for column in table.not_self_fk_columns
        ]

        if coroutines:
            await asyncio.wait(coroutines)

    async def _recursively_preparing_table_with_key_column(
        self,
        table: DBTable,
        need_transfer_pks_chunk: List[int],
    ):
        """
        Recursively preparing table with key column
        """
        await self._recursively_preparing_table(
            table=table,
            need_transfer_pks=need_transfer_pks_chunk,
            deep_without_key_table=1,
        )

        del need_transfer_pks_chunk[:]

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
        need_transfer_pks = await self._get_table_column_values(
            table=table,
            column=table.primary_key,
        )

        table.is_ready_for_transferring = True
        table.is_checked = True

        if need_transfer_pks:
            table.need_transfer_pks.update(need_transfer_pks)

            need_transfer_pks_chunks = make_chunks(
                iterable=need_transfer_pks,
                size=self.CHUNK_SIZE,
                is_list=True,
            )

            coroutines = [
                self._recursively_preparing_table_with_key_column(
                    table=table,
                    need_transfer_pks_chunk=need_transfer_pks_chunk,
                )
                for need_transfer_pks_chunk in need_transfer_pks_chunks
            ]

            if coroutines:
                await asyncio.wait(coroutines)

        del need_transfer_pks

        logger.info(
            f'finished preparing table with key column "{table.name}"'
        )

    async def collect(self):
        logger.info(
            'start preparing tables with key column and their siblings..'
        )
        coroutines = [
            self._prepare_tables_with_key_column(table)
            for table in self._dst_database.tables_with_key_column
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        logger.info(
            'finished preparing tables with key column and their siblings..'
        )


class SortedByDependencyTablesCollector(BaseCollector):
    """
    Collector of records of tables sorted by dependency between their
    """

    async def _get_revert_table_column_values(
        self,
        revert_table: DBTable,
        foreign_column: DBColumn,
        table: DBTable,
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
            column=foreign_column,
            primary_key_values=revert_table_pks,
            is_revert=True,
        )

        if revert_table_column_values:
            table.need_transfer_pks.update(revert_table_column_values)

        del revert_table_column_values

    async def _prepare_revert_table(
        self,
        revert_table_name: str,
        table: DBTable,
    ):
        """
        Preparing revert table
        """
        constraint_types_for_importing = [
            ConstraintTypesEnum.FOREIGN_KEY,
        ]

        revert_table = self._dst_database.tables[revert_table_name]

        logger.info(f'prepare revert table {revert_table_name}')

        if (
            revert_table.fks_with_key_column and
            not table.with_key_column
        ):
            return

        if revert_table.need_transfer_pks:
            foreign_columns = revert_table.get_columns_by_constraint_types_table_name(  # noqa
                table_name=table.name,
                constraint_types=constraint_types_for_importing,
            )

            coroutines = [
                self._get_revert_table_column_values(
                    revert_table=revert_table,
                    foreign_column=foreign_column,
                    table=table,
                )
                for foreign_column in foreign_columns
            ]

            if coroutines:
                await asyncio.wait(coroutines)

        table.revert_fk_tables[revert_table_name] = True

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

        if table.unique_foreign_keys_columns:
            fk_columns = table.unique_foreign_keys_columns
        elif table.fks_with_key_column:
            fk_columns = table.fks_with_key_column
            logger.debug(
                f'table with fks with key column - '
                f'{make_str_from_iterable(table.fks_with_key_column)}'
            )
        else:
            fk_columns = table.not_self_fk_columns
            logger.debug(
                f'table without fks with key column - '
                f'{table.not_self_fk_columns}'
            )

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

        # TODO Uncomment after refactoring
        # if not fk_columns and table.is_checked:
        #     return

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

        table.need_transfer_pks.update(table_pks)

        logger.debug(
            f'table "{table.name}" need transfer pks - '
            f'{len(table.need_transfer_pks)}'
        )

        del table_pks

        # обход таблиц ссылающихся на текущую таблицу
        logger.debug('prepare revert tables')

        coroutines = [
            self._prepare_revert_table(
                revert_table_name=revert_table_name,
                table=table,
            )
            for revert_table_name, is_ready_for_transferring in table.revert_fk_tables.items()  # noqa
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        if not table.need_transfer_pks:
            all_records = await self._get_table_column_values(
                table=table,
                column=table.primary_key,
            )

            table.need_transfer_pks.update(all_records)

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
                    and t.name not in settings.TABLES_WITH_GENERIC_FOREIGN_KEY
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

        # явно ломаю асинхронность, т.к. порядок импорта таблиц важен
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

        target_table.need_transfer_pks.update(need_transfer_pks)

        del where_conditions
        del need_transfer_pks

    async def _prepare_generic_table_data(self, target_table: DBTable):
        """
        Перенос данных из таблицы, содержащей generic foreign key
        """
        logger.info(f"prepare generic table data {target_table.name}")

        coroutines = [
            self._prepare_content_type_generic_data(
                target_table=target_table, rel_table_name=rel_table_name
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

        await asyncio.wait([self._prepare_content_type_tables()])

        generic_table_names = set(
            settings.TABLES_WITH_GENERIC_FOREIGN_KEY
        ).difference(settings.EXCLUDED_TABLES)

        coroutines = [
            self._prepare_generic_table_data(
                self._dst_database.tables.get(table_name)
            )
            for table_name in filter(None, generic_table_names)
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        logger.info("finish collecting")

    async def collect(self):
        logger.info('start preparing generic tables..')

        with StatisticIndexer(
            self._statistic_manager,
            TransferringStagesEnum.COLLECT_GENERIC_TABLES_RECORDS_IDS
        ):
            await asyncio.wait([self._collect_generic_tables_records_ids()])

        logger.info('preparing generic tables finished.')
