import pymssql
import psycopg2
from abc import ABC, abstractmethod
from enum import Enum


class SupportedDatabases(Enum):
    MICROSOFT_SQL_SERVER = "mssql"
    POSTGRESQL = "postgres"


class DatabaseContext(ABC):
    """ Interface for database context implementations """

    @abstractmethod
    def get_database_type(self):
        """Returns the RDBMS type for the database context"""
        pass

    @abstractmethod
    def query(self, query: str, values: tuple = None) -> dict:
        """Sends a select statement to the database and returns results"""
        pass

    @abstractmethod
    def execute(self, query: str, values: tuple = None) -> None:
        """Sends a DML query or stored procedure call to the database"""
        pass


class SqlServerDatabaseContext(DatabaseContext):
    """ Database context for Microsoft SQL Server data sources """

    def __init__(self, server: str = None, user: str = None, password: str = None, database: str = None):
        self._server: str = server
        self._user: str = user
        self._password: str = password
        self._database: str = database

    def get_database_type(self):
        """
        Identifies which relational database management system is being used as the
        database context.

        :return: Common shorthand string representation of RDBMS
        """
        return SupportedDatabases.MICROSOFT_SQL_SERVER.value

    def query(self, query: str, values: tuple = None) -> dict:
        """
        Queries a database with the provided query and values and expects in return
        a dictionary with column names as keys and column values as values.

        This method is used only for SELECT statements. Use the "execute()" method for
        operations not expecting a result set.

        :param query: SQL statement to execute on the database server
        :param values: Tuple of values to insert into query string
        :return: Dictionary of query result values with key as column name
        """
        connection = pymssql.connect(
            server=self._server,
            user=self._user,
            password=self._password,
            database=self._database,
            as_dict=True
        )

        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query, values)
                    results = cursor.fetchall()
        finally:
            connection.close()

        return results

    def execute(self, query: str, values: tuple = None) -> None:
        """
        Executes the provided query and values against the database and does not
        expect a result in return. Use this method for INSERT, UPDATE, DELETE, and
        other DML statements, or for calling stored procedures.

        :param query: SQL query to execute on the database server
        :param values: Tuple of values to insert into query string
        """
        connection = pymssql.connect(
            server=self._server,
            user=self._user,
            password=self._password,
            database=self._database,
            as_dict=True
        )

        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query, values)
                    connection.commit()
        except Exception as ex:
            print(f"query={query}")
            print(f"values={values}")
            print(f"exception={ex}")
        finally:
            connection.close()


class PostgresDatabaseContext(DatabaseContext):
    """ Database context for PostgreSQL data sources """

    def __init__(self, server: str = None, user: str = None, password: str = None, database: str = None):
        self._server: str = server
        self._user: str = user
        self._password: str = password
        self._database: str = database

    def get_database_type(self):
        """
        Identifies which relational database management system is being used as the
        database context.

        :return: Common shorthand string representation of RDBMS
        """
        return SupportedDatabases.POSTGRESQL.value

    def query(self, query: str, values: tuple = None) -> dict:
        """
        Queries a database with the provided query and values and expects in return
        a dictionary with column names as keys and column values as values.

        This method is used only for SELECT statements. Use the "execute()" method for
        operations not expecting a result set.

        :param query: SQL statement to execute on the database server
        :param values: Tuple of values to insert into query string
        :return: Dictionary of query result values with key as column name
        """
        connection = psycopg2.connect(
            host=self._server,
            user=self._user,
            password=self._password,
            dbname=self._database
        )

        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query, values)
                    results = cursor.fetchall()
        finally:
            connection.close()

        return results

    def execute(self, query: str, values: tuple = None) -> None:
        """
        Executes the provided query and values against the database and does not
        expect a result in return. Use this method for INSERT, UPDATE, DELETE, and
        other DML statements, or for calling stored procedures.

        :param query: SQL query to execute on the database server
        :param values: Tuple of values to insert into query string
        """
        connection = psycopg2.connect(
            host=self._server,
            user=self._user,
            password=self._password,
            dbname=self._database
        )

        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query, values)
                    connection.commit()
        finally:
            connection.close()


class DatabaseContextFactory:
    @staticmethod
    def create_database_context(
            database_type: str,
            server: str,
            user: str,
            password: str,
            database: str
    ) -> DatabaseContext:
        if database_type == SupportedDatabases.MICROSOFT_SQL_SERVER.value:
            return SqlServerDatabaseContext(server=server, user=user, password=password, database=database)
        elif database_type == SupportedDatabases.POSTGRESQL.value:
            return PostgresDatabaseContext(server=server, user=user, password=password, database=database)
        else:
            ValueError(f"'{database_type}' is not a supported database type.")
