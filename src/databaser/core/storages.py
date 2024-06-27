from abc import ABC
from typing import AsyncIterator, List, Iterable, Union, Optional, Set

from databaser.core.helpers import make_chunks
from databaser.settings import USE_DATABASE_FOR_STORE_INTERMEDIATE_VALUES, COLLECTOR_CHUNK_SIZE


class AbstractStorage(ABC):
    """
    Абстрактный Класс хранилище id записей
    """

    async def delete(self):
        """
        Удаление/очистка элементов хранилища
        """

        raise NotImplementedError

    async def __aiter__(self) -> AsyncIterator[List[str]]:
        """
        Итератор по бачам записей

        Returns:
            Итератор
        """

        raise NotImplementedError

    async def insert(self, data: Iterable[Union[int, str]]):
        """
        Добавление в хранилище данных

        Args:
            data: данные для добавления
        """

        raise NotImplementedError

    async def add_storage_data(self, storage_data: 'AbstractStorage'):
        """
        Копирование в хранилище данных из другого хранилища

        Args:
            storage_data: хранилище с данными для копирования
        """

        raise NotImplementedError

    async def is_not_empty(self) -> bool:
        """
        Проверка, что хранилище не пустое

        Returns:
            True, если хранилище пустое, инчае False
        """

        raise NotImplementedError

    async def len(self) -> int:
        """
        Получение количества элементов в хранилище

        Returns:
            Размер хранилища
        """

        raise NotImplementedError

    def iter_difference(self, other: 'AbstractStorage') -> AsyncIterator[List[str]]:
        """
        Итератор по элементам, которых нет в другом хранилище

        Args:
            other: Другое хранилище
        Returns:
            Итератор по бачам
        """

        raise NotImplementedError

    async def all(self) -> List[str]:
        """
        Возвращение всех элементов хранилища

        Returns:
            Список всех элементов хранилища
        """

        raise NotImplementedError


class StorageDataTable(AbstractStorage):
    """
    Класс для хранения временных данных в целевой бд
    """

    CREATE_TABLE_SQL = """
        CREATE TABLE storage_data (
            group_id INTEGER,
            data VARCHAR(255),
            CONSTRAINT UC_VALUE UNIQUE (group_id,data)
    );

    CREATE INDEX group_idx ON storage_data (group_id);
    """

    DROP_TABLE_SQL = """DROP TABLE IF EXISTS storage_data;"""

    INSERT_DATA_SQL_TEMPLATE = """
        INSERT INTO storage_data (group_id, data)
        VALUES {values}
        ON CONFLICT DO NOTHING;
    """

    DELETE_DATA_SQL_TEMPLATE = """
        DELETE FROM storage_data
        WHERE group_id = '{group}';
    """

    IS_EXIST_DATA_SQL_TEMPLATE = """SELECT EXISTS(SELECT * FROM storage_data WHERE group_id = '{group}') as exist;"""

    ALL_DATA_SQL_TEMPLATE = """SELECT data FROM storage_data WHERE group_id = '{group}' ORDER BY data;"""

    COUNT_DATA_SQL_TEMPLATE = """SELECT count(*) FROM storage_data WHERE group_id = '{group}';"""

    DIFFERENCE_DATA_SQL_TEMPLATE = """
        SELECT data FROM storage_data WHERE group_id = '{group1}' AND data not in (
            SELECT data FROM storage_data WHERE group_id = '{group2}'
        ) ORDER BY data;
    """

    _last_group_number = 0
    _dst_db: Optional["DstDatabase"] = None

    def __init__(self):
        StorageDataTable._last_group_number += 1
        self._have_value = False
        self._group = StorageDataTable._last_group_number

    @staticmethod
    async def init_table(dst_db: "DstDatabase"):
        """
        Создание таблицы для хранения временных данных в целевой бд

        Args:
            dst_db: Целевая бд
        """

        StorageDataTable._dst_db = dst_db
        await StorageDataTable.drop_table()
        await StorageDataTable._dst_db.execute_raw_sql(StorageDataTable.CREATE_TABLE_SQL)

    @staticmethod
    async def drop_table():
        """
        Удаление таблицы для хранения временных данных из целевой бд
        """

        await StorageDataTable._dst_db.execute_raw_sql(StorageDataTable.DROP_TABLE_SQL)

    async def delete(self):
        delete_data_sql = StorageDataTable.DELETE_DATA_SQL_TEMPLATE.format(group=self._group)
        await StorageDataTable._dst_db.execute_raw_sql(delete_data_sql)
        self._have_value = False

    def __aiter__(self) -> AsyncIterator[List[str]]:
        all_sql = StorageDataTable.ALL_DATA_SQL_TEMPLATE.format(group=self._group)
        return self._dst_db.get_iter(all_sql, COLLECTOR_CHUNK_SIZE)

    async def insert(self, data: Iterable[Union[int, str]]):
        if not data:
            return

        insert_data_sql = StorageDataTable.INSERT_DATA_SQL_TEMPLATE.format(
            values=', '.join([str((self._group, i)) for i in data])
        )
        await StorageDataTable._dst_db.execute_raw_sql(insert_data_sql)

    async def add_storage_data(self, storage_data: 'StorageDataTable'):
        async for chunk in storage_data:
            await self.insert(chunk)

    async def is_not_empty(self) -> bool:
        if not self._have_value:
            is_exist_sql = StorageDataTable.IS_EXIST_DATA_SQL_TEMPLATE.format(group=self._group)

            result = await StorageDataTable._dst_db.fetch_raw_sql(is_exist_sql)

            self._have_value = result[0][0]

        return self._have_value

    async def len(self) -> int:
        count_data_sql = StorageDataTable.COUNT_DATA_SQL_TEMPLATE.format(group=self._group)

        result = await StorageDataTable._dst_db.fetch_raw_sql(count_data_sql)

        return result[0][0]

    def iter_difference(self, other: 'StorageDataTable') -> AsyncIterator[List[str]]:
        data_sql = StorageDataTable.DIFFERENCE_DATA_SQL_TEMPLATE.format(group1=self._group, group2=other._group)

        return self._dst_db.get_iter(data_sql, COLLECTOR_CHUNK_SIZE)

    async def all(self) -> list[str]:
        all_data_sql = StorageDataTable.ALL_DATA_SQL_TEMPLATE.format(group=self._group)

        result = await StorageDataTable._dst_db.fetch_raw_sql(all_data_sql)

        return [i[0] for i in result]


class StorageList(AbstractStorage):
    """
    Класс для хранения данных в памяти
    """

    def __init__(self):
        self._storage: Set[str] = set()

    async def delete(self):
        self._storage.clear()

    def __aiter__(self) -> AsyncIterator[List[str]]:
        async def iterator():
            for i in make_chunks(self._storage, COLLECTOR_CHUNK_SIZE):
                yield list(i)

        return iterator()

    async def insert(self, data: Iterable[Union[int, str]]):
        if not data:
            return

        self._storage.update(map(str, data))

    async def add_storage_data(self, storage_data: 'StorageList'):
        self._storage.update(storage_data._storage)

    async def is_not_empty(self) -> bool:
        return bool(self._storage)

    async def len(self) -> int:
        return len(self._storage)

    def iter_difference(self, other: 'StorageList') -> AsyncIterator[List[str]]:
        data = self._storage - other._storage

        async def iterator():
            for i in make_chunks(data, COLLECTOR_CHUNK_SIZE):
                yield list(i)

        return iterator()

    async def all(self):
        return list(self._storage)


def create_storage() -> AbstractStorage:
    """
        Возвращает новое хранилище исходя из выбранных настроек

        Returns:
            Хранилище для данных
    """

    if USE_DATABASE_FOR_STORE_INTERMEDIATE_VALUES:
        return StorageDataTable()
    else:
        return StorageList()
