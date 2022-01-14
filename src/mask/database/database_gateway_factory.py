from mask.database.database_context import (
    DatabaseContext,
    SupportedDatabases
)
from mask.database.database_gateway import DatabaseGateway
from mask.database.mssql_database_gateway import SqlServerDatabaseGateway
from mask.database.postgres_database_gateway import PostgresDatabaseGateway


class DatabaseGatewayFactory:
    @staticmethod
    def create_database_gateway(database_context: DatabaseContext) -> DatabaseGateway:
        if database_context.get_database_type() == SupportedDatabases.MICROSOFT_SQL_SERVER.value:
            return SqlServerDatabaseGateway(database_context)
        elif database_context.get_database_type() == SupportedDatabases.POSTGRESQL.value:
            return PostgresDatabaseGateway(database_context)
        else:
            raise ValueError(f"{database_context.get_database_type()} does not have a database gateway.")
