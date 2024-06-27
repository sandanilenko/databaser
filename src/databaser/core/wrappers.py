import asyncio

from asyncpg.pool import (
    Pool,
)

from databaser.core.db_entities import (
    DstDatabase,
    SrcDatabase,
)
from databaser.core.repositories import (
    SQLRepository,
)


class PostgresFDWExtensionWrapper:
    """
    Обертка для работы с расширением FDW Postgres
    """

    def __init__(
        self,
        src_database: SrcDatabase,
        dst_database: DstDatabase,
        dst_pool: Pool,
    ):
        self._src_database = src_database
        self._dst_database = dst_database
        self._dst_pool = dst_pool

    async def enable(self):
        """
        Активация FDW и подготовка СУБД для работы с ним
        """
        
        create_fdw_extension_sql = SQLRepository.get_create_fdw_extension_sql()

        create_server_sql = SQLRepository.get_create_server_sql(
            src_host=self._src_database.db_connection_parameters.host,
            src_port=self._src_database.db_connection_parameters.port,
            src_dbname=self._src_database.db_connection_parameters.dbname,
        )

        create_user_mapping_sql = SQLRepository.get_create_user_mapping_sql(
            dst_user=self._dst_database.db_connection_parameters.user,
            src_user=self._src_database.db_connection_parameters.user,
            src_password=self._src_database.db_connection_parameters.password,
        )

        create_temp_src_schema_sql = (
            SQLRepository.get_create_temp_src_schema_sql(
                dst_user=self._dst_database.db_connection_parameters.user,
            )
        )

        import_foreign_schema_sql = SQLRepository.get_import_foreign_schema_sql(
            src_schema=self._src_database.db_connection_parameters.schema,
            tables=self._dst_database.table_names,
        )

        async with self._dst_pool.acquire() as connection:
            await asyncio.wait(
                [
                    asyncio.create_task(
                        connection.execute(create_fdw_extension_sql)
                    ),
                ]
            )
            await asyncio.wait(
                [
                    asyncio.create_task(
                        connection.execute(create_server_sql)
                    ),
                ]
            )
            await asyncio.wait(
                [
                    asyncio.create_task(
                        connection.execute(create_user_mapping_sql)
                    ),
                ]
            )
            await asyncio.wait(
                [
                    asyncio.create_task(
                        connection.execute(create_temp_src_schema_sql)
                    ),
                ]
            )
            await asyncio.wait(
                [
                    asyncio.create_task(
                        connection.execute(import_foreign_schema_sql)
                    ),
                ]
            )

    async def disable(self):
        """
        Деактивация плагина FDW
        """
        drop_temp_src_schema_sql = SQLRepository.get_drop_temp_src_schema_sql()
        drop_user_mapping_sql = SQLRepository.get_drop_user_mapping_sql(
            dst_user=self._dst_database.db_connection_parameters.user,
        )
        drop_fdw_extension_sql = SQLRepository.get_drop_fdw_extension_sql()

        async with self._dst_pool.acquire() as connection:
            await asyncio.wait(
                [
                    asyncio.create_task(
                        connection.execute(drop_temp_src_schema_sql)
                    ),
                ]
            )
            await asyncio.wait(
                [
                    asyncio.create_task(
                        connection.execute(drop_user_mapping_sql)
                    ),
                ]
            )
            await asyncio.wait(
                [
                    asyncio.create_task(
                        connection.execute(drop_fdw_extension_sql)
                    ),
                ]
            )
