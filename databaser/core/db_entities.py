import asyncio
from functools import (
    lru_cache,
)
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
)

import asyncpg
from asyncpg.pool import (
    Pool,
)

import settings
from core.enums import (
    ConstraintTypesEnum,
)
from core.helpers import (
    DBConnectionParameters,
    deep_getattr,
    logger,
    make_chunks,
    make_str_from_iterable,
)
from core.repositories import (
    SQLRepository,
)
from core.strings import (
    CONNECTION_STR_TEMPLATE,
)


class BaseDatabase(object):
    """
    Base class for creating databases
    """

    def __init__(
        self,
        db_connection_parameters: DBConnectionParameters,
    ):
        self.db_connection_parameters: DBConnectionParameters = (
            db_connection_parameters
        )
        self.table_names: Optional[List[str]] = None
        self.tables: Optional[Dict[str, DBTable]] = None

        self._connection_pool: Optional[Pool] = None

    @property
    def connection_str(self) -> str:
        return CONNECTION_STR_TEMPLATE.format(
            self.db_connection_parameters
        )

    @property
    def connection_pool(self) -> Pool:
        return self._connection_pool

    @connection_pool.setter
    def connection_pool(
        self,
        pool: Pool,
    ):
        self._connection_pool = pool

    async def prepare_table_names(self):
        """
        Preparing database table names list
        """
        select_tables_names_list_sql = SQLRepository.get_select_tables_names_list_sql(  # noqa
            excluded_tables=settings.EXCLUDED_TABLES,
        )

        async with self._connection_pool.acquire() as connection:
            table_names = await connection.fetch(
                query=select_tables_names_list_sql,
            )

            self.table_names = [
                table_name_rec[0]
                for table_name_rec in table_names
            ]

    async def execute_raw_sql(
        self,
        raw_sql: str,
    ):
        """
        Async executing raw sql
        """
        connection = await asyncpg.connect(self.connection_str)

        try:
            await connection.execute(raw_sql)
        finally:
            del raw_sql
            await connection.close()

    async def fetch_raw_sql(
        self,
        raw_sql: str,
    ):
        """
        Async executing raw sql with fetching result
        """
        connection = await asyncpg.connect(self.connection_str)

        try:
            result = await connection.fetch(raw_sql)
        finally:
            await connection.close()

        del raw_sql

        return result


class SrcDatabase(BaseDatabase):
    """
    Source database
    """

    def __init__(
        self,
        db_connection_parameters: DBConnectionParameters,
    ):
        logger.info('init src database')

        super().__init__(
            db_connection_parameters=db_connection_parameters,
        )


class DstDatabase(BaseDatabase):
    """
    Destination database
    """

    def __init__(
        self,
        db_connection_parameters: DBConnectionParameters,
    ):
        super().__init__(
            db_connection_parameters=db_connection_parameters,
        )

        logger.info('init dst database')

    @property
    @lru_cache()
    def tables_without_generics(self) -> List['DBTable']:
        """
        Getting DB tables without generics
        """
        return list(
            filter(
                lambda t: (
                    t.name not in settings.TABLES_WITH_GENERIC_FOREIGN_KEY
                ),
                self.tables.values(),
            )
        )

    @property
    @lru_cache()
    def tables_with_key_column(self) -> List['DBTable']:
        """
        Getting tables without generics with key column
        """
        return list(
            filter(
                lambda t: t.with_key_column,
                self.tables_without_generics,
            )
        )

    async def _prepare_chunk_tables(
        self,
        chunk_table_names: Iterable[str],
    ):
        """
        Preparing tables of chunk table names
        """
        getting_tables_columns_sql = SQLRepository.get_table_columns_sql(
            table_names=make_str_from_iterable(
                iterable=chunk_table_names,
                with_quotes=True,
                quote='\'',
            ),
        )

        async with self._connection_pool.acquire() as connection:
            records = await connection.fetch(
                query=getting_tables_columns_sql,
            )

        coroutines = [
            self.tables[table_name].append_column(
                column_name=column_name,
                data_type=data_type,
                ordinal_position=ordinal_position,
                constraint_table=self.tables.get(constraint_table_name),
                constraint_type=constraint_type,
            )
            for (
                table_name,
                column_name,
                data_type,
                ordinal_position,
                constraint_table_name,
                constraint_type,
            ) in records
        ]

        if coroutines:
            await asyncio.gather(*coroutines)

    async def prepare_tables(self):
        """
        Prepare tables structure for transferring data process
        """
        logger.info('prepare tables structure for transferring process')

        self.tables = {
            f'{table_name}': DBTable(
                name=table_name,
            )
            for table_name in self.table_names
        }

        chunks_table_names = make_chunks(
            iterable=self.table_names,
            size=settings.TABLES_LIMIT_PER_TRANSACTION,
        )

        coroutines = [
            self._prepare_chunk_tables(
                chunk_table_names=chunk_table_names,
            )
            for chunk_table_names in chunks_table_names
        ]

        if coroutines:
            await asyncio.gather(*coroutines)

        logger.info(
            f'prepare tables progress - {len(self.tables.keys())}/'
            f'{len(self.table_names)}'
        )

    async def set_max_tables_sequences(self):
        """
        Setting max table sequence value as max(id) + 1
        """
        coroutines = [
            table.set_max_sequence(self._connection_pool)
            for table in self.tables.values()
        ]

        await asyncio.wait(coroutines)

    async def prepare_structure(self):
        """
        Prepare destination database structure
        """
        await self.prepare_table_names()

        await self.prepare_tables()

        logger.info(f'dst database tables count - {len(self.table_names)}')

    async def truncate_tables(self):
        """
        Truncating tables
        """
        if settings.IS_TRUNCATE_TABLES:
            logger.info('start truncating tables..')

            if settings.TABLES_TRUNCATE_INCLUDED:
                table_names = settings.TABLES_TRUNCATE_INCLUDED
            else:
                table_names = tuple(
                    filter(
                        lambda table_name: (
                            table_name not in settings.TABLES_WITH_GENERIC_FOREIGN_KEY
                        ),
                        self.table_names,
                    )
                )

            if settings.TABLES_TRUNCATE_EXCLUDED:
                table_names = tuple(
                    filter(
                        lambda table_name: (
                            table_name not in settings.TABLES_TRUNCATE_EXCLUDED
                        ),
                        table_names,
                    )
                )

            truncate_table_queries = SQLRepository.get_truncate_table_queries(
                table_names=table_names,
            )

            for query in truncate_table_queries:
                await self.execute_raw_sql(query)

            logger.info('truncating tables finished.')

    async def disable_triggers(self):
        """
        Disable database triggers
        """
        disable_triggers_sql = SQLRepository.get_disable_triggers_sql()

        await self.execute_raw_sql(disable_triggers_sql)

        logger.info('trigger disabled.')

    async def enable_triggers(self):
        """
        Enable database triggers
        """
        enable_triggers_sql = SQLRepository.get_enable_triggers_sql()

        await self.execute_raw_sql(enable_triggers_sql)

        logger.info('triggers enabled.')


class DBTable(object):
    """
    Класс описывающий таблицу БД
    Имеет название и поля с типами
    """

    __slots__ = (
        'name',
        'full_count',
        'max_pk',
        'columns',
        '_is_ready_for_transferring',
        '_is_checked',
        '_key_column',
        'revert_fk_tables',
        'need_transfer_pks',
        'transferred_pks',
    )

    schema = 'public'

    # понижающее количество объектов, т.к. во время доведения могут
    # производиться действия пользователями и кол-во объектов может меняться
    inaccuracy_count = 100

    def __init__(self, name):
        self.name = name
        self.full_count = 0
        self.max_pk = 0
        self.columns: Dict[str, 'DBColumn'] = {}

        # Table is ready for transferring
        self._is_ready_for_transferring = False

        # Table is checked in collecting values process
        self._is_checked: bool = False

        self._key_column = None

        # хранит названия таблиц которые ссылаются на текущую и признак того,
        # что записи зависимой таблицы были внесены в список для импорта
        self.revert_fk_tables = {}

        # Pks of table for transferring
        self.need_transfer_pks = set()

        self.transferred_pks = set()

    def __str__(self):
        return (
            f'table {self.name} with_fk {self.with_fk}, '
            f'with_key_column {self.with_key_column}, '
            f'with_self_fk {self.with_self_fk}'
        )

    @property
    @lru_cache()
    def rev_fk_tables_records_transferred(self):
        return all([t for t in self.revert_fk_tables.values()])

    @property
    @lru_cache()
    def primary_key(self):
        primary_keys = list(
            filter(
                lambda c: ConstraintTypesEnum.PRIMARY_KEY in c.constraint_type,
                self.columns.values(),
            )
        )

        if primary_keys:
            return primary_keys[0]

    @property
    def is_ready_for_transferring(self) -> bool:
        """
        Table is ready for transferring
        """
        return self._is_ready_for_transferring

    @is_ready_for_transferring.setter
    def is_ready_for_transferring(self, is_ready_for_transferring):
        self._is_ready_for_transferring = is_ready_for_transferring

    @property
    def is_full_prepared(self):
        logger.debug(
            f'table - {self.name} -- count table records {self.full_count} and '
            f'need transfer pks {len(self.need_transfer_pks)}'
        )

        if len(self.need_transfer_pks) >= self.full_count - self.inaccuracy_count:  # noqa
            logger.info(f'table {self.name} full transferred')

            return True

    @property
    @lru_cache()
    def with_fk(self):
        return bool(self.foreign_keys_columns)

    @property
    @lru_cache()
    def key_column(self):
        return self._key_column

    @property
    @lru_cache()
    def with_key_column(self):
        return bool(self._key_column)

    @property
    @lru_cache()
    def with_self_fk(self):
        return bool(self.self_fk_columns)

    @property
    @lru_cache()
    def with_not_self_fk(self):
        return bool(self.not_self_fk_columns)

    @property
    @lru_cache()
    def unique_foreign_keys_columns(self) -> List['DBColumn']:
        return list(filter(
            lambda c: c.is_foreign_key and c.is_unique,
            self.not_self_fk_columns
        ))

    @property
    @lru_cache()
    def foreign_keys_columns(self):
        return list(filter(lambda c: c.is_foreign_key, self.columns.values()))

    @property
    @lru_cache()
    def self_fk_columns(self):
        return list(filter(lambda c: c.is_self_fk, self.columns.values()))

    @property
    @lru_cache()
    def not_self_fk_columns(self) -> List['DBColumn']:
        return list(
            filter(
                lambda c: c.is_foreign_key and not c.is_self_fk,
                self.columns.values()
            )
        )

    @property
    @lru_cache()
    def fks_with_key_column(self):
        return list(
            filter(
                lambda c: c.constraint_table.with_key_column,
                self.not_self_fk_columns
            )
        )

    @property
    def is_checked(self) -> bool:
        return self._is_checked

    @is_checked.setter
    def is_checked(self, value):
        self._is_checked = value

    async def append_column(
        self,
        column_name: str,
        data_type: str,
        ordinal_position: int,
        constraint_table: Optional['DBTable'],
        constraint_type: str,
    ):
        if column_name in self.columns:
            column: DBColumn = await self.get_column_by_name(column_name)

            if constraint_type:
                await column.add_constraint_type(constraint_type)

                if constraint_type == ConstraintTypesEnum.FOREIGN_KEY:
                    column.constraint_table = constraint_table
        else:
            # postgresql возврщает тип array вместо integer array
            if data_type == 'ARRAY':
                data_type = 'integer array'

            column = DBColumn(
                column_name=column_name,
                table_name=self.name,
                data_type=data_type,
                ordinal_position=ordinal_position,
                constraint_table=constraint_table,
                constraint_type=constraint_type,
            )

            self.columns[column_name] = column

        if not self._key_column and column.is_key_column:
            self._key_column = column

        if column.is_foreign_key:
            column.constraint_table.revert_fk_tables[self.name] = False

        return column

    async def get_column_by_name(self, column_name):
        """
        Get table column by name
        """
        return self.columns.get(column_name)

    def get_columns_by_constraint_types_table_name(
        self,
        table_name: str,
        constraint_types: Optional[Iterable[str]] = None,
    ):
        """
        Get foreign columns by constraint types and table name
        """
        return list(
            filter(
                lambda c: (
                    deep_getattr(c.constraint_table, 'name') == table_name and (
                        set(c.constraint_type).intersection(
                            set(constraint_types)
                        ) if
                        constraint_types else
                        True
                    )
                ),
                self.columns.values(),
            )
        )

    def get_columns_list_str_commas(self):
        return ', '.join(
            map(
                lambda c: f'"{c.name}"',
                sorted(self.columns.values(), key=lambda c: c.ordinal_position),
            )
        )  # noqa

    def get_columns_list_with_types_str_commas(self):
        return ', '.join(
            map(
                lambda c: f'"{c.name}" {c.data_type}',
                sorted(self.columns.values(), key=lambda c: c.ordinal_position),
            )
        )

    async def set_max_sequence(self, dst_pool: Pool):
        async with dst_pool.acquire() as connection:
            try:
                get_serial_sequence_sql = SQLRepository.get_serial_sequence_sql(
                    table_name=self.name,
                    pk_column_name=self.primary_key.name,
                )
            except AttributeError:
                logger.warning(
                    f'AttributeError --- {self.name} --- set_max_sequence'
                )
                return

            serial_seq_name = await connection.fetchrow(
                get_serial_sequence_sql
            )

            if serial_seq_name and serial_seq_name[0]:
                serial_seq_name = serial_seq_name[0]

                max_val = self.max_pk + 100000

                set_sequence_val_sql = (
                    SQLRepository.get_set_sequence_value_sql(
                        seq_name=serial_seq_name,
                        seq_val=max_val,
                    )
                )

                await connection.execute(set_sequence_val_sql)


class DBColumn(object):
    __slots__ = (
        'name',
        'table_name',
        'data_type',
        'ordinal_position',
        'constraint_table',
        'constraint_type',
    )

    def __init__(
        self,
        column_name: str,
        table_name: str,
        data_type: str,
        ordinal_position: int,
        constraint_table: Optional[DBTable] = None,
        constraint_type: Optional[str] = None,
    ):

        assert column_name, None
        assert table_name, None

        self.name = column_name
        self.table_name = table_name
        self.data_type = data_type or ''
        self.ordinal_position = ordinal_position or 0
        self.constraint_table = constraint_table
        self.constraint_type = []

        if constraint_type:
            self.constraint_type.append(constraint_type)

    def __repr__(self):
        return (
            f'{self.name} - {self.data_type} - {self.ordinal_position} - '
            f'{deep_getattr(self.constraint_table, "name", " - ")}'
            f'{make_str_from_iterable(self.constraint_type)}'
        )
    
    def __str__(self):
        return self.__repr__()

    @property
    @lru_cache()
    def is_foreign_key(self):
        return ConstraintTypesEnum.FOREIGN_KEY in self.constraint_type

    @property
    @lru_cache()
    def is_primary_key(self):
        return ConstraintTypesEnum.PRIMARY_KEY in self.constraint_type

    @property
    @lru_cache()
    def is_unique(self):
        return (
            ConstraintTypesEnum.UNIQUE in self.constraint_type or (
                self.is_foreign_key and self.is_primary_key
            )
        )

    @property
    @lru_cache()
    def is_key_column(self):
        return (
            self.name in settings.KEY_COLUMN_NAMES or
            deep_getattr(self.constraint_table, 'name') == settings.KEY_TABLE_NAME  # noqa
        )

    @property
    def is_self_fk(self):
        return (
            self.is_foreign_key and 
            deep_getattr(self.constraint_table, 'name') == self.table_name
        )

    def get_column_name_with_type(self):
        return f'{self.name} {self.data_type}'

    async def add_constraint_type(self, constraint_type):
        self.constraint_type.append(constraint_type)
