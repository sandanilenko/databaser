import copy
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

from databaser.core.enums import (
    ConstraintTypesEnum,
    DataTypesEnum,
    LogLevelEnum,
)
from databaser.core.helpers import (
    logger,
    make_chunks,
    make_str_from_iterable,
)
from databaser.settings import (
    KEY_COLUMN_NAMES,
    LOG_LEVEL,
    TABLES_LIMIT_PER_TRANSACTION,
)


class SQLRepository:
    """Репозиторий SQL-запросов для работы с базами данных.

    Класс содержит шаблоны SQL-запросов и методы для их формирования
    для различных операций с базами данных, включая:
    - Управление расширением postgres_fdw
    - Создание и удаление схем и сопоставлений пользователей
    - Импорт и экспорт данных между базами
    - Управление триггерами и последовательностями
    - Получение метаданных таблиц и столбцов
    """

    CHUNK_SIZE = 60000

    CREATE_FDW_EXTENSION_SQL_TEMPLATE = 'CREATE EXTENSION postgres_fdw;'

    DROP_FDW_EXTENSION_SQL_TEMPLATE = 'DROP EXTENSION IF EXISTS postgres_fdw CASCADE;'

    # ruff: noqa: E501
    CREATE_SERVER_SQL_TEMPLATE = """
        CREATE SERVER src_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '{src_host}', port '{src_port}', dbname '{src_dbname}', fetch_size '{fetch_size}' , updatable 'false');
        """

    CREATE_USER_MAPPING_SQL_TEMPLATE = """
        CREATE USER MAPPING FOR "{dst_user}"
        SERVER src_server
        OPTIONS (user '{src_user}', password '{src_password}');
        """

    DROP_USER_MAPPING_SQL_TEMPLATE = 'DROP USER MAPPING IF EXISTS FOR "{dst_user}" SERVER "src_server"'

    CREATE_TEMP_SRC_SCHEMA_SQL_TEMPLATE = 'CREATE SCHEMA "tmp_src_schema" AUTHORIZATION "{dst_user}";'

    DROP_TEMP_SRC_SCHEMA_SQL_TEMPLATE = 'DROP SCHEMA IF EXISTS "tmp_src_schema" CASCADE;'

    IMPORT_FOREIGN_SCHEMA_SQL_TEMPLATE = """
        IMPORT FOREIGN SCHEMA "{src_schema}" LIMIT TO ({tables})
        FROM SERVER src_server INTO "tmp_src_schema" OPTIONS (import_default 'true');
        """

    TRUNCATE_TABLE_SQL_TEMPLATE = """
        TRUNCATE {table_names} CASCADE;
    """

    SELECT_PARTITION_NAMES_LIST_SQL_TEMPLATE = """
        SELECT pt.relname
        FROM pg_class base_tb
            JOIN pg_inherits i 
                ON i.inhparent = base_tb.oid
            JOIN pg_class pt 
                ON pt.oid = i.inhrelid
        WHERE pt.relpartbound IS NOT NULL;
    """

    SELECT_TABLES_NAMES_LIST_SQL_TEMPLATE = """
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE 
            t.table_schema = 'public' 
            AND t.table_type = 'BASE TABLE' 
            AND t.table_name NOT IN ({excluded_tables}) 
            AND t.table_name NOT LIKE '\_%';
    """

    SELECT_TABLE_FIELDS_SQL = """
        SELECT 
            col.table_name,
            col.column_name,
            col.data_type,
            col.ordinal_position,
            COALESCE(constr_col_usage.table_name, constr_col_usage_fk.table_name) AS constraint_table_name,
            constr.constraint_type
        FROM information_schema.columns col
            LEFT JOIN information_schema.key_column_usage key_col_usage
                ON key_col_usage.table_catalog = col.table_catalog
                AND key_col_usage.table_schema = col.table_schema
                AND key_col_usage.table_name = col.table_name
                AND key_col_usage.column_name = col.column_name
            LEFT JOIN information_schema.table_constraints constr
                ON constr.table_catalog = key_col_usage.table_catalog
                AND constr.table_schema = key_col_usage.table_schema
                AND constr.table_name = key_col_usage.table_name
                AND constr.constraint_catalog = key_col_usage.constraint_catalog
                AND constr.constraint_schema = key_col_usage.constraint_schema
                AND constr.constraint_name = key_col_usage.constraint_name
                AND constr.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
            LEFT JOIN information_schema.constraint_column_usage constr_col_usage
                ON constr_col_usage.constraint_catalog = key_col_usage.constraint_catalog
                AND constr_col_usage.constraint_schema = key_col_usage.constraint_schema
                AND constr_col_usage.constraint_name = key_col_usage.constraint_name
                AND constr_col_usage.column_name = key_col_usage.column_name
                AND constr.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
            LEFT JOIN information_schema.constraint_column_usage constr_col_usage_fk
                ON constr_col_usage_fk.constraint_catalog = key_col_usage.constraint_catalog
                AND constr_col_usage_fk.constraint_schema = key_col_usage.constraint_schema
                AND constr_col_usage_fk.constraint_name = key_col_usage.constraint_name
                AND constr.constraint_type IN ('FOREIGN KEY')
        WHERE 
            col.table_schema = 'public'
            AND col.table_name IN ({table_names});        
    """

    DISABLE_TRIGGERS_SQL_TEMPLATE = "UPDATE pg_trigger SET tgenabled='D' ;"

    ENABLE_TRIGGERS_SQL_TEMPLATE = "UPDATE pg_trigger SET tgenabled='O' ;"

    SERIAL_SEQUENCE_SQL_TEMPLATE = """
        SELECT pg_get_serial_sequence('"{table_name}"', '{pk_column_name}');
    """

    SET_SEQUENCE_VALUE_SQL_TEMPLATE = """
        SELECT setval('{seq_name}', {seq_val});
    """

    SELECT_TABLE_COLUMN_VALUES_TEMPLATE = """
        SELECT "{constraint_column_name}" FROM "{table_name}" {where_conditions};
    """

    COUNT_ALL_SQL_TEMPLATE = """
        SELECT count(*), {max_pk_value_sql} FROM "{table_name}";
    """

    TRANSFER_SQL_TEMPLATE = """
        INSERT INTO "public"."{table_name}" ({selection_params_commas})
        SELECT {selection_params_commas}
        FROM "tmp_src_schema"."{table_name}" 
        WHERE {pk_condition_sql}
        RETURNING "{primary_key}";"""

    CONTENT_TYPE_TABLE_SQL_TEMPLATE = """
        SELECT "table_name", "app_label", "model"
        FROM django_content_type_table;
    """

    CONTENT_TYPE_SQL_TEMPLATE = """
        SELECT "id", "app_label", "model"
        FROM django_content_type;
    """

    @classmethod
    def get_create_fdw_extension_sql(cls):
        """Получение SQL-запроса для создания расширения postgres_fdw.

        Returns:
            str: SQL-запрос для создания расширения
        """
        return cls.CREATE_FDW_EXTENSION_SQL_TEMPLATE

    @classmethod
    def get_drop_fdw_extension_sql(cls):
        """Получение SQL-запроса для удаления расширения postgres_fdw.

        Returns:
            str: SQL-запрос для удаления расширения
        """
        return cls.DROP_FDW_EXTENSION_SQL_TEMPLATE

    @classmethod
    def get_create_server_sql(
        cls,
        src_host: str,
        src_port: str,
        src_dbname: str,
    ):
        """Получение SQL-запроса для создания внешнего сервера.

        Args:
            src_host: Хост исходной базы данных
            src_port: Порт исходной базы данных
            src_dbname: Имя исходной базы данных

        Returns:
            str: SQL-запрос для создания сервера
        """
        return cls.CREATE_SERVER_SQL_TEMPLATE.format(
            src_host=src_host,
            src_port=src_port,
            src_dbname=src_dbname,
            fetch_size=cls.CHUNK_SIZE,
        )

    @classmethod
    def get_create_user_mapping_sql(
        cls,
        dst_user: str,
        src_user: str,
        src_password: str,
    ):
        """Получение SQL-запроса для создания сопоставления пользователей.

        Args:
            dst_user: Пользователь целевой базы данных
            src_user: Пользователь исходной базы данных
            src_password: Пароль пользователя исходной базы данных

        Returns:
            str: SQL-запрос для создания сопоставления пользователей
        """
        return cls.CREATE_USER_MAPPING_SQL_TEMPLATE.format(
            dst_user=dst_user,
            src_user=src_user,
            src_password=src_password,
        )

    @classmethod
    def get_drop_user_mapping_sql(
        cls,
        dst_user: str,
    ):
        """Получение SQL-запроса для удаления сопоставления пользователей.

        Args:
            dst_user: Пользователь целевой базы данных

        Returns:
            str: SQL-запрос для удаления сопоставления пользователей
        """
        return cls.DROP_USER_MAPPING_SQL_TEMPLATE.format(
            dst_user=dst_user,
        )

    @classmethod
    def get_create_temp_src_schema_sql(
        cls,
        dst_user: str,
    ):
        """Получение SQL-запроса для создания временной схемы.

        Args:
            dst_user: Пользователь целевой базы данных

        Returns:
            str: SQL-запрос для создания временной схемы
        """
        return cls.CREATE_TEMP_SRC_SCHEMA_SQL_TEMPLATE.format(
            dst_user=dst_user,
        )

    @classmethod
    def get_drop_temp_src_schema_sql(cls):
        """Получение SQL-запроса для удаления временной схемы.

        Returns:
            str: SQL-запрос для удаления временной схемы
        """
        return cls.DROP_TEMP_SRC_SCHEMA_SQL_TEMPLATE

    @classmethod
    def get_import_foreign_schema_sql(
        cls,
        src_schema: str,
        tables: Iterable[str],
    ):
        """Получение SQL-запроса для импорта внешней схемы.

        Args:
            src_schema: Имя схемы в исходной базе данных
            tables: Список таблиц для импорта

        Returns:
            str: SQL-запрос для импорта схемы
        """
        tables_str = make_str_from_iterable(
            iterable=tables,
            with_quotes=True,
        )

        return cls.IMPORT_FOREIGN_SCHEMA_SQL_TEMPLATE.format(
            src_schema=src_schema,
            tables=tables_str,
        )

    @classmethod
    def get_truncate_table_queries(
        cls,
        table_names: Iterable[str],
    ):
        """Получение SQL-запросов для очистки таблиц.

        Args:
            table_names: Список имен таблиц для очистки

        Returns:
            List[str]: Список SQL-запросов для очистки таблиц
        """
        table_names_chunks = make_chunks(
            iterable=table_names,
            size=TABLES_LIMIT_PER_TRANSACTION,
        )

        truncate_queries = []
        for table_names_chunk in table_names_chunks:
            table_names_str = make_str_from_iterable(
                iterable=table_names_chunk,
                with_quotes=True,
            )

            truncate_queries.append(
                cls.TRUNCATE_TABLE_SQL_TEMPLATE.format(
                    table_names=table_names_str,
                )
            )

        return truncate_queries

    @classmethod
    def get_select_partition_names_list_sql(cls):
        """Получение SQL-запроса для получения списка имен секционированных таблиц.

        Returns:
            str: SQL-запрос для получения списка секций
        """
        return cls.SELECT_PARTITION_NAMES_LIST_SQL_TEMPLATE

    @classmethod
    def get_select_tables_names_list_sql(
        cls,
        excluded_tables=None,
    ):
        """Получение SQL-запроса для получения списка имен таблиц.

        Args:
            excluded_tables: Список исключаемых таблиц

        Returns:
            str: SQL-запрос для получения списка таблиц
        """
        if excluded_tables:
            excluded_tables_str = make_str_from_iterable(
                iterable=excluded_tables,
                with_quotes=True,
                quote="'",
            )
        else:
            excluded_tables_str = ''

        return cls.SELECT_TABLES_NAMES_LIST_SQL_TEMPLATE.format(
            excluded_tables=excluded_tables_str,
        )

    @classmethod
    def get_table_columns_sql(
        cls,
        table_names: str,
    ):
        """Получение SQL-запроса для получения информации о столбцах таблиц.

        Args:
            table_names: Строка с именами таблиц через запятую

        Returns:
            str: SQL-запрос для получения информации о столбцах
        """
        return cls.SELECT_TABLE_FIELDS_SQL.format(
            table_names=table_names,
            constraint_types=ConstraintTypesEnum.get_types_comma(),
        )

    @classmethod
    def get_disable_triggers_sql(cls):
        """Получение SQL-запроса для отключения триггеров.

        Returns:
            str: SQL-запрос для отключения триггеров
        """
        return cls.DISABLE_TRIGGERS_SQL_TEMPLATE

    @classmethod
    def get_enable_triggers_sql(cls):
        """Получение SQL-запроса для включения триггеров.

        Returns:
            str: SQL-запрос для включения триггеров
        """
        return cls.ENABLE_TRIGGERS_SQL_TEMPLATE

    @classmethod
    def get_serial_sequence_sql(
        cls,
        table_name: str,
        pk_column_name: str,
    ):
        """Получение SQL-запроса для получения имени последовательности для столбца.

        Args:
            table_name: Имя таблицы
            pk_column_name: Имя столбца первичного ключа

        Returns:
            str: SQL-запрос для получения имени последовательности
        """
        return cls.SERIAL_SEQUENCE_SQL_TEMPLATE.format(
            table_name=table_name,
            pk_column_name=pk_column_name,
        )

    @classmethod
    def get_set_sequence_value_sql(
        cls,
        seq_name: str,
        seq_val: int,
    ):
        """Получение SQL-запроса для установки значения последовательности.

        Args:
            seq_name: Имя последовательности
            seq_val: Новое значение последовательности

        Returns:
            str: SQL-запрос для установки значения последовательности
        """
        return cls.SET_SEQUENCE_VALUE_SQL_TEMPLATE.format(
            seq_name=seq_name,
            seq_val=seq_val,
        )

    @classmethod
    async def get_table_column_values_sql(
        cls,
        table,
        column,
        key_column_values: Set[int],
        primary_key_values: Iterable[Union[int, str]] = (),
        where_conditions_columns: Optional[Dict[str, Set[Union[int, str]]]] = None,
        is_revert=False,
    ) -> list:
        """Получение SQL-запросов для получения значений столбца таблицы.

        Args:
            table: Объект таблицы
            column: Объект столбца
            key_column_values: Множество значений ключевого столбца
            primary_key_values: Список значений первичного ключа
            where_conditions_columns: Словарь с дополнительными условиями фильтрации
            is_revert: Флаг инвертирования условий фильтрации

        Returns:
            List[str]: Список SQL-запросов для получения значений
        """
        if LOG_LEVEL == LogLevelEnum.DEBUG:
            logger.debug(
                f'SQL constraint ids. table name - {table.name}, '
                f'column_name - {column.name}, '
                f'key_column_value - {str(key_column_values)}, '
                f'with_key_column - {table.with_key_column}, '
                f'primary_key_ids - {make_str_from_iterable(list(primary_key_values)[:10])}'
                f' ({len(primary_key_values)})\n'
            )

            if where_conditions_columns:
                for c, v in where_conditions_columns.items():
                    v_list = list(v)

                    condition_str = f'{c}={make_str_from_iterable(v_list[:10])}'

                    logger.debug(f'where condition --- {condition_str} ({len(v_list)})')

                    del v_list
                    del condition_str

        where_conditions_combinations = []

        if where_conditions_columns:
            where_conditions = []

            for c_name, c_ids in where_conditions_columns.items():
                if c_name in KEY_COLUMN_NAMES:
                    continue

                condition_column = await table.get_column_by_name(c_name)

                if c_ids:
                    if is_revert:
                        w_cond_tmpl = '{c_name} in ({c_ids})'
                    else:
                        w_cond_tmpl = '({c_name} in ({c_ids}) or {c_name} isnull)'

                    ids_chunks = make_chunks(
                        iterable=c_ids,
                        size=cls.CHUNK_SIZE,
                        is_list=True,
                    )

                    tmp_where_conditions = []
                    for ids_chunk in ids_chunks:
                        ids_str = cls._get_ids_str_by_column_type(
                            column=condition_column,
                            ids=ids_chunk,
                        )

                        if ids_str:
                            tmp_where_conditions.append(
                                w_cond_tmpl.format(
                                    c_name=c_name,
                                    c_ids=ids_str,
                                )
                            )

                    where_conditions.append(tmp_where_conditions)
                else:
                    where_conditions.append([f'{c_name} isnull'])

            if where_conditions:
                # необходимо скомбинировать все условия следующему алгоритму:
                # выделяются все списки условий, длина которых == 1, из тех
                # списков, у которых длина более 1, нужно составлять комбинации
                # после того, как комбинации будут составлены к ним нужно
                # добросить одиночные условия

                single_conditions = list(filter(lambda cond: len(cond) == 1, where_conditions))

                single_conditions_values = [cond[0] for cond in single_conditions]

                if len(single_conditions) == len(where_conditions):
                    where_conditions_combinations.append(single_conditions_values)
                else:
                    multiple_conditions = list(filter(lambda cond: len(cond) > 1, where_conditions))

                    for index, cond_list in enumerate(multiple_conditions):
                        if index == 0:
                            where_conditions_combinations = [[cond] for cond in cond_list]
                            continue

                        where_cond_copy = copy.deepcopy(where_conditions_combinations)
                        for idx, c in enumerate(cond_list):
                            if idx == 0:
                                for w_c in where_conditions_combinations:
                                    w_c.append(c)
                                continue

                            wcc2 = copy.deepcopy(where_cond_copy)
                            for w_c in wcc2:
                                w_c.append(c)

                            where_conditions_combinations.extend(wcc2)

                    del multiple_conditions[:]

                    for comb in where_conditions_combinations:
                        comb.extend(single_conditions_values)

            del where_conditions[:]

        sql_result_list = []
        if where_conditions_combinations:
            for where_conditions in where_conditions_combinations:
                sql_result = cls._select_table_column_values_part_sql(
                    table=table,
                    column=column,
                    key_column_values=key_column_values,
                    primary_key_values=primary_key_values,
                    where_conditions=where_conditions,
                )
                if sql_result:
                    sql_result_list.append(sql_result)
        else:
            sql_result = cls._select_table_column_values_part_sql(
                table=table,
                column=column,
                key_column_values=key_column_values,
                primary_key_values=primary_key_values,
            )

            if sql_result:
                sql_result_list.append(sql_result)

        del where_conditions_combinations[:]

        return sql_result_list

    @classmethod
    def _select_table_column_values_part_sql(
        cls,
        table,
        column,
        key_column_values: Set[int],
        primary_key_values: Optional[Iterable[Union[int, str]]] = None,
        where_conditions: Optional[Iterable[str]] = (),
    ):
        """Формирование части SQL-запроса для получения значений столбца.

        Args:
            table: Объект таблицы
            column: Объект столбца
            key_column_values: Множество значений ключевого столбца
            primary_key_values: Список значений первичного ключа
            where_conditions: Список дополнительных условий

        Returns:
            str: Часть SQL-запроса
        """
        # отфильтруем все 1
        where_conditions_filtering = list(filter(lambda cond: cond != '1', where_conditions))

        # если в условии летят все 1, то такой запрос выполнять не надо
        if not where_conditions_filtering and where_conditions:
            return
        else:
            where_conditions = where_conditions_filtering

        where_conditions_str = ''
        if where_conditions:
            where_conditions_str = f'{" and ".join(where_conditions)}'

        if primary_key_values:
            primary_key_ids_str = cls._get_ids_str_by_column_type(column=table.primary_key, ids=primary_key_values)

            pk_condition_sql = f'{table.primary_key.name} in ({primary_key_ids_str})'

            if where_conditions_str:
                where_conditions_str = ' '.join([where_conditions_str, 'and', pk_condition_sql])
            else:
                where_conditions_str = pk_condition_sql

        if table.with_key_column:
            key_column = table.key_column

            logger.debug(f'find key_column - {key_column.name}')

            if key_column_values:
                key_column_ids_sql = (
                    f'{key_column.name} in ({make_str_from_iterable(key_column_values)}) or {key_column.name} isnull'
                )

                if where_conditions_str:
                    where_conditions_str = ' '.join([where_conditions_str, 'and', key_column_ids_sql])
                else:
                    where_conditions_str = key_column_ids_sql

        if where_conditions_str:
            where_conditions_str = f'where {where_conditions_str}'

        result_sql = cls.SELECT_TABLE_COLUMN_VALUES_TEMPLATE.format(
            constraint_column_name=column.name,
            table_name=table.name,
            where_conditions=where_conditions_str,
            constraint_column_with_type=column.get_column_name_with_type(),
        )

        logger.debug(result_sql)

        del where_conditions_filtering[:]
        del where_conditions_str

        return result_sql

    @staticmethod
    def _get_ids_str_by_column_type(
        column,
        ids: List[str],
    ):
        """Форматирование списка идентификаторов в зависимости от типа столбца.

        Args:
            column: Объект столбца
            ids: Список идентификаторов

        Returns:
            str: Отформатированная строка с идентификаторами
        """
        if column.data_type in DataTypesEnum.NUMERAL:
            ids_str = ', '.join(map(str, ids))
        else:
            ids_str = ', '.join(map(lambda pk: f"''{pk}''", ids))

        return ids_str

    @classmethod
    def get_count_table_records(
        cls,
        primary_key,
    ):
        """Получение SQL-запроса для подсчета записей в таблице.

        Args:
            primary_key: Объект первичного ключа

        Returns:
            str: SQL-запрос для подсчета записей
        """
        if primary_key.data_type in DataTypesEnum.NUMERAL:
            max_pk_value_sql = f'max("{primary_key.table_name}"."{primary_key.name}")'
        else:
            max_pk_value_sql = 'count(*)'

        return cls.COUNT_ALL_SQL_TEMPLATE.format(
            table_name=primary_key.table_name,
            max_pk_value_sql=max_pk_value_sql,
        )

    @classmethod
    def get_transfer_records_sql(
        cls,
        table,
        connection_params_str,
        primary_key_ids,
    ):
        """Получение SQL-запроса для переноса записей между таблицами.

        Args:
            table: Объект таблицы
            connection_params_str: Строка с параметрами подключения
            primary_key_ids: Список значений первичного ключа

        Returns:
            str: SQL-запрос для переноса записей
        """
        logger.debug(f'get transfer records sql \n table name - {table.name}')

        if table.primary_key.data_type in ['integer']:
            primary_key_ids_str = ', '.join(map(str, primary_key_ids))
        else:
            primary_key_ids_str = ', '.join(map(lambda pk: f"'{pk}'", primary_key_ids))

        pk_condition_sql = f'"tmp_src_schema"."{table.name}"."{table.primary_key.name}" in ({primary_key_ids_str})'

        transfer_sql = cls.TRANSFER_SQL_TEMPLATE.format(
            connection_params_str=connection_params_str,
            selection_params_commas=table.get_columns_list_str_commas(),
            table_name=table.name,
            selection_params_with_types=table.get_columns_list_with_types_str_commas(),
            primary_key=table.primary_key.name,
            pk_condition_sql=pk_condition_sql,
        )

        return transfer_sql

    @classmethod
    def get_content_type_table_sql(cls):
        """Получение SQL-запроса для получения данных из таблицы типов контента Django.

        Returns:
            str: SQL-запрос для получения типов контента
        """
        return cls.CONTENT_TYPE_TABLE_SQL_TEMPLATE

    @classmethod
    def get_content_type_sql(cls):
        """Получение SQL-запроса для получения типов контента Django.

        Returns:
            str: SQL-запрос для получения типов контента
        """
        return cls.CONTENT_TYPE_SQL_TEMPLATE
