import asyncio
from typing import (
    Set,
)

import asyncpg
from asyncpg import (
    UndefinedFunctionError,
)
from asyncpg.pool import (
    Pool,
)

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


class Collector:
    """
    Класс комплексной транспортировки, который использует принципы обхода по
    внешним ключам и по таблицам с обратной связью
    """
    CHUNK_SIZE = 70000

    def __init__(
        self,
        dst_database: DstDatabase,
        src_database: SrcDatabase,
        dst_pool: Pool,
        src_pool: Pool,
        statistic_manager: StatisticManager,
        key_column_values: Set[int],
    ):
        self._dst_database = dst_database
        self._src_database = src_database
        self._dst_pool = dst_pool
        self._src_pool = src_pool
        self.key_column_ids = key_column_values
        self._structured_ent_ids = None
        # словарь с названиями таблиц и идентификаторами импортированных записей
        self._transfer_progress_dict = {}
        self.filling_tables = set()
        self._statistic_manager = statistic_manager

        self.content_type_table = {}

    async def _fill_table_rows_count(self, table_name: str):
        async with self._src_pool.acquire() as connection:
            table = self._dst_database.tables[table_name]

            try:
                table_rows_counts_sql = (
                    SQLRepository.get_count_table_records(
                        primary_key=table.primary_key,
                    )
                )
            except AttributeError as e:
                logger.warning(
                    f'{str(e)} --- _fill_table_rows_count {"-"*10} - '
                    f"{table.name}"
                )
                raise AttributeError
            except UndefinedFunctionError:
                raise UndefinedFunctionError

            res = await connection.fetchrow(table_rows_counts_sql)

            if res and res[0] and res[1]:
                logger.debug(
                    f"table {table_name} with full count {res[0]}, "
                    f"max id - {res[1]}"
                )

                table.full_count = int(res[0])

                table.max_id = (
                    int(res[1])
                    if isinstance(res[1], int)
                    else table.full_count + 100000
                )

            del table_rows_counts_sql

    async def fill_tables_rows_counts(self):
        logger.info(
            "заполнение количества записей в табилце и максимального значения "
            "идентификатора.."
        )

        coroutines = [
            self._fill_table_rows_count(table_name)
            for table_name in sorted(self._dst_database.tables.keys())
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        logger.info("заполнение значений счетчиков завершено")

    async def _collect_key_table_ids(self):
        logger.info("transfer key table records...")

        key_table = self._dst_database.tables[settings.KEY_TABLE_NAME]

        key_table.need_imported.update(self.key_column_ids)

        key_table.is_transferred = True

        logger.info("transfer key table records finished!")

    async def _get_constraint_table_ids_part(
        self,
        constraint_table_ids_sql,
        constraint_table_ids,
    ):
        if constraint_table_ids_sql:
            logger.debug(constraint_table_ids_sql)
            async with self._src_pool.acquire() as connection:
                try:
                    c_t_ids = await connection.fetch(constraint_table_ids_sql)
                except asyncpg.PostgresSyntaxError as e:
                    logger.warning(
                        f"{str(e)} --- {constraint_table_ids_sql} --- "
                        f"_get_constraint_table_ids_part"
                    )
                    c_t_ids = []

                constraint_table_ids.extend(
                    [
                        item[0]
                        for item in filter(lambda id_: id_[0] is not None, c_t_ids)
                    ]
                )

                del c_t_ids
                del constraint_table_ids_sql

    async def _get_constraint_table_ids(
        self,
        table: DBTable,
        column: DBColumn,
        key_column_ids=(),
        table_pk_ids=(),
        where_conditions_columns=(),
        is_revert=False,
    ) -> set:
        # если таблица находится в исключенных, то ее записи не нужно
        # импортировать
        try:
            if column.constraint_table.name in settings.EXCLUDED_TABLES:
                return set()
        except AttributeError as e:
            logger.warning(f"{str(e)} --- _get_constraint_table_ids")
            return set()
        # формирование запроса на получения идентификаторов записей
        # внешней таблицы
        constraint_table_ids_sql_list = await SQLRepository.get_constraint_table_ids_sql(
            table=table,
            constraint_column=column,
            key_column_ids=key_column_ids,
            primary_key_ids=table_pk_ids,
            where_conditions_columns=where_conditions_columns,
            is_revert=is_revert,
        )
        constraint_table_ids = []

        for constraint_table_ids_sql in constraint_table_ids_sql_list:
            await self._get_constraint_table_ids_part(
                constraint_table_ids_sql, constraint_table_ids
            )

        del constraint_table_ids_sql_list[:]

        result = set(constraint_table_ids)

        del constraint_table_ids[:]

        return result

    async def _collect_revert_table_ids(self, rev_table, fk_column, table):
        rev_table_pk_ids = (
            list(rev_table.need_imported)
            if not rev_table.is_full_transferred
            else []
        )

        rev_ids = await self._get_constraint_table_ids(
            rev_table,
            fk_column,
            self.key_column_ids,
            table_pk_ids=rev_table_pk_ids,
            is_revert=True,
        )

        if rev_ids:
            table.need_imported.update(rev_ids)

        del rev_ids

    async def _collect_importing_revert_tables_data(
        self, rev_table_name, table
    ):
        constraint_types_for_importing = [ConstraintTypesEnum.FOREIGN_KEY]
        rev_table = self._dst_database.tables[rev_table_name]
        logger.info(f"prepare revert table {rev_table_name}")

        if rev_table.fks_with_key_column and not table.with_key_column:
            return

        if rev_table.need_imported:
            coroutines = [
                self._collect_revert_table_ids(rev_table, fk_column, table)
                for fk_column in rev_table.get_columns_by_constraint_table_name(
                    table.name,
                    constraint_types_for_importing,
                )
            ]

            if coroutines:
                await asyncio.wait(coroutines)

        table.revert_fk_tables[rev_table_name] = True

    async def _collect_importing_fk_tables_records_ids(
        self,
        table: DBTable,
    ):
        logger.info(
            f"start collecting records ids of table \"{table.name}\""
        )
        # обход таблиц связанных через внешние ключи
        where_conditions = {}

        if table.fks_with_key_column:
            fk_columns = table.fks_with_key_column
            logger.debug(
                f"table with fks_with_ent_id - "
                f"{make_str_from_iterable(table.fks_with_key_column)}"
            )
        else:
            fk_columns = table.not_self_fk_columns
            logger.debug(
                f"table without fks_with_ent_id - {table.not_self_fk_columns}"
            )

        unique_fks_columns = table.unique_foreign_keys_columns
        if unique_fks_columns:
            fk_columns = unique_fks_columns

        with_full_transferred_table = False

        for fk_column in fk_columns:
            logger.debug(f"prepare column {fk_column.name}")
            fk_table = self._dst_database.tables[
                fk_column.constraint_table.name
            ]

            if fk_table.need_imported:
                if not fk_table.is_full_transferred:
                    where_conditions[fk_column.name] = fk_table.need_imported
                else:
                    with_full_transferred_table = True

        if fk_columns and not where_conditions and not with_full_transferred_table:
            return

        tasks = await asyncio.wait([self._get_constraint_table_ids(
            table,
            table.primary_key,
            self.key_column_ids,
            where_conditions_columns=where_conditions,
        )])

        fk_ids = (
            tasks[0].pop().result() if (
                tasks and
                tasks[0] and
                isinstance(tasks[0], set)
            ) else
            None
        )

        if fk_columns and where_conditions and not fk_ids:
            return

        table.need_imported.update(fk_ids)

        logger.debug(
            f'table "{table.name}" need imported - {len(table.need_imported)}'
        )

        del fk_ids

        # обход таблиц ссылающихся на текущую таблицу
        logger.debug("prepare revert tables")

        rev_coroutines = [
            self._collect_importing_revert_tables_data(rev_table_name, table)
            for rev_table_name, is_transferred in table.revert_fk_tables.items()
        ]

        if rev_coroutines:
            await asyncio.wait(rev_coroutines)

        if not table.need_imported:
            all_records = await self._get_constraint_table_ids(
                table,
                table.primary_key,
                key_column_ids=self.key_column_ids,
            )

            table.need_imported.update(all_records)

            del all_records

        table.is_transferred = True

        logger.info(
            f"finished collecting records ids of table \"{table.name}\""
        )

    async def _recursively_collecting_fk_table_chunk_data(
        self,
        need_import_ids_chunk,
        deep_without_key_table,
        fk_table,
        stack_tables,
    ):
        dwe = (
            deep_without_key_table - 1
            if not fk_table.with_key_column
            else deep_without_key_table
        )

        await self._recursively_collecting_table_data(
            fk_table,
            need_import_ids_chunk,
            stack_tables,
            deep_without_key_table=dwe,
        )

        del dwe
        del need_import_ids_chunk[:]

    async def _recursively_collecting_fk_table_data(
        self,
        column: DBColumn,
        stack_tables,
        table,
        deep_without_key_table,
        pk_ids,
    ):
        fk_table = self._dst_database.tables[column.constraint_table.name]

        # если таблица уже есть в стеке импорта таблиц, то он нас не
        # интересует; если талица с key_column, то записи в любом случае
        # будут импортированы
        if fk_table in stack_tables or fk_table.with_key_column:
            return

        # Если таблица с key_column, то нет необходимости пробрасывать
        # идентификаторы записей
        if table.with_key_column:
            fk_table_ids = await self._get_constraint_table_ids(
                table,
                column,
                key_column_ids=self.key_column_ids,
            )
        else:
            pk_ids = pk_ids if not table.is_full_transferred else []
            fk_table_ids = await self._get_constraint_table_ids(
                table,
                column,
                key_column_ids=self.key_column_ids,
                table_pk_ids=pk_ids,
            )

        # если найдены значения внешних ключей отличающиеся от null, то
        # записи из внешней талицы с этими идентификаторами должны быть
        # импортированы
        if fk_table_ids:
            logger.debug(
                f"table - {table.name}, column - {column.name} - reversed "
                f"collecting of fk_ids ----- {fk_table.name}"
            )

            fk_diff = fk_table_ids.difference(fk_table.need_imported)

            # если есть разница между предполагаемыми записями для импорта
            # и уже выбранными ранее, то разницу нужно импортировать
            if fk_diff:
                fk_table.need_imported.update(fk_diff)

                need_import_ids_chunks = make_chunks(
                    iterable=fk_diff,
                    size=self.CHUNK_SIZE,
                    is_list=True,
                )

                coroutines = [
                    self._recursively_collecting_fk_table_chunk_data(
                        need_import_ids_chunk,
                        deep_without_key_table,
                        fk_table,
                        stack_tables,
                    )
                    for need_import_ids_chunk in need_import_ids_chunks
                ]

                if coroutines:
                    await asyncio.wait(coroutines)

            del fk_diff

        del fk_table_ids

    async def _recursively_collecting_table_data(
        self,
        table,
        pk_ids,
        stack_tables=(),
        deep_without_key_table=None,
    ):
        """
        :param DBTable table:
        :param set pk_ids:
        :return:
        """
        # if is_revert_rel and not deep_without_key_table:
        if not deep_without_key_table:
            logger.debug("Max deep without key table")
            return

        stack_tables += (table,)

        logger.debug(make_str_from_iterable([t.name for t in stack_tables]))

        coroutines = [
            self._recursively_collecting_fk_table_data(
                column, stack_tables, table, deep_without_key_table, pk_ids
            )
            for column in table.not_self_fk_columns
        ]

        if coroutines:
            await asyncio.wait(coroutines)

    async def _collect_recursively_ent_table(
        self,
        need_import_ids_chunk,
        table,
    ):
        await self._recursively_collecting_table_data(
            table,
            need_import_ids_chunk,
            deep_without_key_table=1,
        )

        del need_import_ids_chunk[:]

    async def _collect_importing_ent_table_records_ids(self, table):
        logger.info(
            f"start collecting records ids of table \"{table.name}\""
        )
        need_import_ids = await self._get_constraint_table_ids(
            table=table,
            column=table.primary_key,
            key_column_ids=self.key_column_ids,
        )

        if need_import_ids:
            table.need_imported.update(need_import_ids)

            need_import_ids_chunks = make_chunks(
                iterable=need_import_ids,
                size=self.CHUNK_SIZE,
                is_list=True,
            )

            coroutines = [
                self._collect_recursively_ent_table(
                    need_import_ids_chunk=need_import_ids_chunk,
                    table=table,
                )
                for need_import_ids_chunk in need_import_ids_chunks
            ]

            if coroutines:
                await asyncio.wait(coroutines)

        table.is_transferred = True

        del need_import_ids

        logger.info(
            f"finished collecting records ids of table \"{table.name}\""
        )

    async def _collect_common_tables_records_ids(self):
        """
        Метод сбора данных для дальнейшего импорта в целевую базу. Первоначально
        производится сбор данных из таблиц с key_column и всех таблиц, которые их
        окружают с глубиной рекурсивного обхода 1. Сюда входят таблицы связанные
        через внешние ключи и таблицы ссылающиеся на текущую. После чего
        производится сбор записей таблиц, из которых не был произведен сбор
        данных. Эти таблицы находятся дальше чем одна таблица от таблиц с
        key_column.
        Для верного обхода у таблиц существует параметр
        transferred_rel_tables_percent, который указывает на часть таблиц,
        которая была импортирована
        """
        logger.info("start collecting common tables records ids")

        # обход таблиц с key_column и их соседей
        coroutines = [
            self._collect_importing_ent_table_records_ids(table)
            for table in self._dst_database.tables_with_key_column
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        not_transferred_tables = list(
            filter(
                lambda t: (
                    not t.is_transferred
                    and t.name
                    not in settings.TABLES_WITH_GENERIC_FOREIGN_KEY
                ),
                self._dst_database.tables.values(),
            )
        )
        logger.debug(
            f"tables not transferring {str(len(not_transferred_tables))}"
        )

        not_transferred_relatives = []
        for table in self._dst_database.tables_without_generics:
            for fk_column in table.not_self_fk_columns:
                not_transferred_relatives.append(
                    (table.name, fk_column.constraint_table.name)
                )

        sorting_result = topological_sort(not_transferred_relatives)
        sorting_result.cyclic.reverse()
        sorting_result.sorted.reverse()

        sorted_not_transferred = sorting_result.cyclic + sorting_result.sorted

        without_relatives = list(
            {
                table.name
                for table in self._dst_database.tables_without_generics
            }.difference(
                sorted_not_transferred
            )
        )

        sorted_not_transferred = without_relatives + sorted_not_transferred

        # явно ломаю асинхронность, т.к. порядок импорта таблиц важен
        for table_name in sorted_not_transferred:
            table = self._dst_database.tables[table_name]
            if not table.is_transferred:
                await self._collect_importing_fk_tables_records_ids(
                    table
                )

        logger.info("finished collecting common tables records ids")

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
            logger.debug("not send rel_table_name")
            return

        rel_table = self._dst_database.tables.get(rel_table_name)

        if not rel_table:
            logger.debug(f"table {rel_table_name} not found")
            return

        object_id_column = await target_table.get_column_by_name("object_id")

        if rel_table.primary_key.data_type != object_id_column.data_type:
            logger.debug(
                f"PK of table {rel_table_name} has an incompatible data type"
            )
            return

        logger.info("prepare content type generic data")

        where_conditions = {
            "object_id": rel_table.need_imported,
            "content_type_id": [self.content_type_table[rel_table.name]],
        }

        need_imported = await self._get_constraint_table_ids(
            target_table,
            target_table.primary_key,
            key_column_ids=self.key_column_ids,
            where_conditions_columns=where_conditions,
        )

        logger.info(f"{target_table.name} need imported {len(need_imported)}")

        target_table.need_imported.update(need_imported)

        del where_conditions
        del need_imported

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
        with StatisticIndexer(
            self._statistic_manager,
            TransferringStagesEnum.TRANSFER_KEY_TABLE,
        ):
            await asyncio.wait([self._collect_key_table_ids()])

        with StatisticIndexer(
            self._statistic_manager,
            TransferringStagesEnum.COLLECT_COMMON_TABLES_RECORDS_IDS
        ):
            await asyncio.wait([self._collect_common_tables_records_ids()])

        with StatisticIndexer(
            self._statistic_manager,
            TransferringStagesEnum.COLLECT_GENERIC_TABLES_RECORDS_IDS
        ):
            await asyncio.wait([self._collect_generic_tables_records_ids()])
