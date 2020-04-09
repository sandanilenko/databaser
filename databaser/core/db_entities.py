import asyncio
from functools import (
    lru_cache,
)
from typing import (
    Dict,
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
    Базовый класс БД
    """

    def __init__(
        self,
        db_connection_parameters: DBConnectionParameters,
    ):
        self.db_connection_parameters: DBConnectionParameters = (
            db_connection_parameters
        )
        self.table_names: Optional[List[str]] = None

    @property
    def connection_str(self):
        return CONNECTION_STR_TEMPLATE.format(
            self.db_connection_parameters
        )

    async def prepare_table_names(self):
        select_tables_names_list_sq = SQLRepository.get_select_tables_names_list_sql(  # noqa
            excluded_tables=settings.EXCLUDED_TABLES,
        )

        tables_names = await self.fetch_raw_sql(select_tables_names_list_sq)

        self.table_names = [
            table_name_rec[0]
            for table_name_rec in tables_names
        ]

    async def truncate_tables(self):
        """
        Очистка всех данных из БД
        """
        logger.info('start truncating all tables')

        tables_names = filter(
            lambda item: (
                item not in settings.TRUNCATE_EXCLUDED_TABLES and
                item not in settings.TABLES_WITH_GENERIC_FOREIGN_KEY
            ),
            self.table_names,
        )

        truncate_table_queries = SQLRepository.get_truncate_table_queries(
            table_names=tables_names,
        )

        for query in truncate_table_queries:
            await self.execute_raw_sql(query)

        logger.info('finish truncating all tables')

    async def disable_triggers(self):
        """
        Выключение тригеров на проверку целостности данных
        """
        disable_triggers_sql = SQLRepository.get_disable_triggers_sql()

        await self.execute_raw_sql(disable_triggers_sql)

    async def enable_triggers(self):
        """
        Включение всех тригеров в БД
        """
        enable_triggers_sql = SQLRepository.get_enable_triggers_sql()

        await self.execute_raw_sql(enable_triggers_sql)

        logger.warning('triggers enabled!')

    async def execute_raw_sql(self, raw_sql):
        """
        Асинхронный метод выполнения чистого sql
        """
        connection = await asyncpg.connect(self.connection_str)

        try:
            await connection.execute(raw_sql)
        finally:
            del raw_sql
            await connection.close()

    async def fetch_raw_sql(self, raw_sql):
        """
        Асинхронный метод выполнения чистого sql с возвращением результата
        """
        connection = await asyncpg.connect(self.connection_str)

        try:
            result = await connection.fetch(raw_sql)
        finally:
            await connection.close()

        del raw_sql

        return result


class DstDatabase(BaseDatabase):
    def __init__(
        self,
        db_connection_parameters: DBConnectionParameters,
    ):
        """
        Целевая база данных
        """
        super().__init__(db_connection_parameters)

        logger.info('Init dst database')
        self.tables: Optional[Dict[str, DBTable]] = None

        self.drop_all_constraints_definitions = []
        self.create_all_constraints_definitions = []
        self.constrains_names_list = []

        self.indexes = set()
        self.indexes_definitions = []

    async def prepare_tables(self):
        logger.info('prepare tables structure for transferring process')

        self.tables = {
            f'{table_name}': DBTable(
                name=table_name,
            )
            for table_name in self.table_names
        }

        getting_tables_columns_sql = SQLRepository.get_table_columns_sql(
            table_names=make_str_from_iterable(
                iterable=self.table_names,
                with_quotes=True,
                quote='\'',
            ),
        )

        records = await self.fetch_raw_sql(
               raw_sql=getting_tables_columns_sql,
        )

        for record in records:
            (
                table_name,
                column_name,
                data_type,
                ordinal_position,
                constraint_table_name,
                constraint_type,
            ) = record

            self.tables[table_name].append_column(
                column_name=column_name,
                data_type=data_type,
                ordinal_position=ordinal_position,
                constraint_table=self.tables.get(constraint_table_name),
                constraint_type=constraint_type,
            )

        logger.info(
            f'prepare tables progress - {len(self.tables.keys())}/'
            f'{len(self.table_names)}'
        )

    def fill_revert_tables(self):
        """
        Метод для заполнения списка таблиц из которых есть ссылки на эту таблицу
        """
        for table in self.tables.values():
            for column in table.foreign_keys_columns:
                try:
                    column.constraint_table.revert_fk_tables[table.name] = False
                except KeyError:
                    logger.warning(
                        f'Key error - table name - "{table.name}", '
                        f'constraint table name - '
                        f'"{column.constraint_table.name}"'
                    )
                    raise KeyError

    async def set_max_tables_sequences(self, dst_pool: Pool):
        """
        Устанавливает на всех таблицах значения последовательностей
        Значение последовательности равно max(id) + 1
        Может быть метод стоит декомпозировать
        """
        coroutines = [
            table.set_max_sequence(dst_pool)
            for table in self.tables.values()
        ]

        await asyncio.wait(coroutines)

    def prepare_fks_with_ent_id(self):
        """
        Метод проставления внешних ключей, которые указывают на таблицы с ent_id
        Если в таблице имеются ссылки на таблицы с ent_id, то при добавлении
        записей стоит опираться только на эти поля
        :return:
        """
        logger.info('prepare fks with ent id columns')

        for table in self.tables.values():
            for fk_column in table.not_self_fk_columns:
                if fk_column.constraint_table.with_ent_id:
                    table.fks_with_ent_id.append(fk_column)

        logger.info('finish preparing fks with ent id columns')


class SrcDatabase(BaseDatabase):
    def __init__(self, db_connection_parameters):
        """
        :param DBConnectionParameters db_connection_parameters:
        """
        logger.info('init src database')
        super(SrcDatabase, self).__init__(db_connection_parameters)


class DBTable(object):
    """
    Класс описывающий таблицу БД
    Имеет название и поля с типами
    """

    __slots__ = (
        'name',
        '_is_transferred',
        'full_count',
        'max_id',
        'columns',
        '_with_ent_id',
        '_ent_id_column',
        'revert_fk_tables',
        'need_imported',
        'transferred_rel_tables_percent',
        'fks_with_ent_id',
        'transferred_ids',
    )

    schema = 'public'

    # понижающее количество объектов, т.к. во время доведения могут
    # производиться действия пользователями и кол-во объектов может меняться
    inaccuracy_count = 100

    def __init__(self, name):
        self.name = name
        self._is_transferred = False
        self.full_count = 0
        self.max_id = 0
        self.columns: Dict[str, 'DBColumn'] = {}

        self._with_ent_id = False
        self._ent_id_column = None

        # хранит названия таблиц которые ссылаются на текущую и признак того,
        # что записи зависимой таблицы были внесены в список для импорта
        self.revert_fk_tables = {}

        # множество идентификаторов записей предназначенных для импорта
        self.need_imported = set()

        # параметр указывает процент соседних таблиц, которые были импортированы
        self.transferred_rel_tables_percent = 1

        self.fks_with_ent_id: List['DBColumn'] = []

        self.transferred_ids = set()

    def __str__(self):
        return (
            f'table {self.name} with_fk {self.with_fk}, '
            f'with_ent_id {self.with_ent_id}, with_self_fk {self.with_self_fk}'
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
    def is_transferred(self):
        return self._is_transferred

    @is_transferred.setter
    def is_transferred(self, is_transferred):
        self._is_transferred = is_transferred

    @property
    def is_full_transferred(self):
        logger.debug(
            f'table - {self.name} -- count table records {self.full_count} and '
            f'need imported {len(self.need_imported)}'
        )

        if len(self.need_imported) >= self.full_count - self.inaccuracy_count:
            logger.info(f'table {self.name} full transferred')

            return True

    @property
    @lru_cache()
    def with_fk(self):
        return bool(self.foreign_keys_columns)

    @property
    @lru_cache()
    def ent_id_column(self):
        return self._ent_id_column

    @property
    @lru_cache()
    def with_ent_id(self):
        return self._with_ent_id

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

    def append_column(
        self,
        column_name: str,
        data_type: str,
        ordinal_position: int,
        constraint_table: Optional['DBTable'],
        constraint_type: str,
    ):
        if column_name in self.columns:
            column: DBColumn = self.get_column_by_name(column_name)

            if constraint_type:
                column.add_constraint_type(constraint_type)

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

        if not self._with_ent_id and column.is_ent_id:
            self._ent_id_column = column
            self._with_ent_id = True

        return column

    def get_column_by_name(self, column_name):
        """
        :param column_name:
        :return DbColumn or None:
        """
        return self.columns.get(column_name)

    def get_columns_by_constraint_table_name(
        self,
        table_name,
        constraint_type=None,
    ):
        return list(
            filter(
                lambda c: (
                    deep_getattr(c.constraint_table, 'name') == table_name and (
                        set(c.constraint_type).intersection(
                            set(constraint_type)
                        ) if
                        constraint_type else
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

                max_val = self.max_id + 100000

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
    def is_ent_id(self):
        return (
            self.name == 'ent_id' or 
            deep_getattr(self.constraint_table, 'name') == 'enterprise'
        )

    @property
    def is_self_fk(self):
        return (
            self.is_foreign_key and 
            deep_getattr(self.constraint_table, 'name') == self.table_name
        )

    def get_column_name_with_type(self):
        return f'{self.name} {self.data_type}'

    def add_constraint_type(self, constraint_type):
        self.constraint_type.append(constraint_type)
