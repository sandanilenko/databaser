import asyncio
from typing import (
    List,
    Set,
    Union,
)

from asyncpg import (
    NotNullViolationError,
    NumericValueOutOfRangeError,
    PostgresError,
    PostgresSyntaxError,
    UndefinedColumnError,
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
    logger,
    make_chunks,
)
from core.loggers import (
    StatisticManager,
    statistic_indexer,
)
from core.repositories import (
    SQLRepository,
)


class Transporter:
    """
    Класс комплексной транспортировки, который использует принципы обхода по
    внешним ключам и по таблицам с обратной связью
    """
    CHUNK_SIZE = 70000

    def __init__(
        self,
        dst_database: DstDatabase,
        src_database: SrcDatabase,
        statistic_manager: StatisticManager,
        key_column_values: Set[int],
    ):
        self._dst_database = dst_database
        self._src_database = src_database
        self.key_column_ids = key_column_values
        self._structured_ent_ids = None
        # словарь с названиями таблиц и идентификаторами импортированных записей
        self._transfer_progress_dict = {}
        self.filling_tables = set()
        self._statistic_manager = statistic_manager

        self.content_type_table = {}

    async def _transfer_table_data(self, table):
        """
        Перенос данных таблицы
        """
        logger.info(
            f"start transferring table \"{table.name}\", "
            f"need to import - {len(table.need_transfer_pks)}"
        )

        need_import_ids_chunks = make_chunks(
            iterable=table.need_transfer_pks,
            size=self.CHUNK_SIZE,
        )

        for need_import_ids_chunk in need_import_ids_chunks:
            await self._transfer_chunk_table_data(
                table=table,
                need_import_ids_chunk=need_import_ids_chunk,
            )

        logger.info(
            f"finished transferring table \"{table.name}\""
        )

    async def _transfer_chunk_table_data(
        self,
        table: DBTable,
        need_import_ids_chunk: List[Union[int, str]],
    ):
        """
        Порционный перенос данных таблицы в целевую БД
        """
        transfer_sql = SQLRepository.get_transfer_records_sql(
            table=table,
            connection_params_str=self._src_database.connection_str,
            primary_key_ids=need_import_ids_chunk,
        )

        logger.info(f'transfer chunk table data - "{table.name}"')

        transferred_ids = None
        async with self._dst_database.connection_pool.acquire() as connection:
            try:
                transferred_ids = await connection.fetch(transfer_sql)
            except (
                UndefinedColumnError,
                NotNullViolationError,
                PostgresSyntaxError,
                NumericValueOutOfRangeError,
            ) as e:
                raise PostgresError(
                    f'{str(e)}, table - {table.name}, '
                    f'sql - {transfer_sql} --- _transfer_chunk_table_data'
                )

        if transferred_ids:
            table.transferred_pks_count += len(transferred_ids)

        del transfer_sql
        del transferred_ids

    async def _transfer_collecting_data(self):
        """
        Физический импорт данных в целевую БД из БД-донора
        """
        logger.info("start transferring data to target db...")

        need_imported_tables = filter(
            lambda table: table.need_transfer_pks,
            self._dst_database.tables.values(),
        )

        coroutines = [
            self._transfer_table_data(table)
            for table in need_imported_tables
        ]

        if coroutines:
            await asyncio.gather(*coroutines)

        logger.info("finished transferring data to target db!")

    async def _update_sequences(self):
        """
        Обновление значений счетчиков на макситальные
        """
        logger.info("start updating sequences...")
        await self._dst_database.set_max_tables_sequences()
        logger.info("finished updating sequences!")

    async def transfer(self):
        """
        Переносит данный из БД донора в БД приемник
        """
        async with statistic_indexer(
            self._statistic_manager,
            TransferringStagesEnum.TRANSFERRING_COLLECTED_DATA
        ):
            await asyncio.wait(
                [
                    asyncio.create_task(
                        self._transfer_collecting_data()
                    ),
                ]
            )

        async with statistic_indexer(
            self._statistic_manager,
            TransferringStagesEnum.UPDATE_SEQUENCES
        ):
            await asyncio.wait(
                [
                    asyncio.create_task(
                        self._update_sequences()
                    ),
                ]
            )
