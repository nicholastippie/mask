from abc import ABC, abstractmethod
from typing import Optional
from mask.config.constants import Constants
from mask.database_access.database_context import (
    DatabaseContext,
    SupportedDatabases
)


class DatabaseGateway(ABC):
    """ Interface for database access """

    @abstractmethod
    def generate_where_clause_from_record(self, record: dict, primary_key: list) -> str:
        """
        Create a where clause from a record such that the where clause can distinctly
        identify the record in the database.

        :param record: Dictionary containing column names as keys and column values as values
        :param primary_key: List of primary key column names
        :return: String where clause which can identify the record in the database
        """
        pass

    @abstractmethod
    def generate_update_set_clause_for_column(self, column: str, replacement_value: vars) -> str:
        pass

    @abstractmethod
    def append_where_column_is_not_null(self, column: str, where_clause: str) -> str:
        pass

    @abstractmethod
    def get_list_of_primary_key_columns_for_table(self, database: str, schema: str, table: str) -> list:
        pass

    @abstractmethod
    def get_records_from_table(self, database: str, schema: str, table: str, where_clause: str) -> dict:
        pass

    @abstractmethod
    def update_rows(
            self, database: str, schema: str, table: str, set_clause: str, where_clause: str
    ) -> None:
        pass

    @abstractmethod
    def update_date_column_with_random_variance(
            self, database: str, schema: str, table: str, column: str, where_clause: str,
            range_min: int, range_max: int
    ) -> None:
        pass

    @abstractmethod
    def truncate_table(self, database: str, schema: str, table: str) -> None:
        pass

    @abstractmethod
    def delete_rows(self, database: str, schema: str, table: str, where_clause: str) -> None:
        pass

    @abstractmethod
    def disable_all_triggers_for_database(self, database: str) -> None:
        pass

    @abstractmethod
    def disable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    @abstractmethod
    def disable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None:
        pass

    @abstractmethod
    def enable_all_triggers_for_database(self, database: str) -> None:
        pass

    @abstractmethod
    def enable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    @abstractmethod
    def enable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None:
        pass

    @abstractmethod
    def disable_all_check_constraints_for_database(self, database: str) -> None:
        pass

    @abstractmethod
    def disable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    @abstractmethod
    def disable_single_check_constraint_for_table(
            self, database: str, schema: str, table: str, check_constraint: str) -> None:
        pass

    @abstractmethod
    def enable_all_check_constraints_for_database(self, database: str) -> None:
        pass

    @abstractmethod
    def enable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    @abstractmethod
    def enable_single_check_constraint_for_table(
            self, database: str, schema: str, table: str, check_constraint: str) -> None:
        pass

    @abstractmethod
    def disable_all_foreign_keys_for_database(self, database: str) -> None:
        pass

    @abstractmethod
    def disable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    @abstractmethod
    def disable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None:
        pass

    @abstractmethod
    def enable_all_foreign_keys_for_database(self, database: str) -> None:
        pass

    @abstractmethod
    def enable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    @abstractmethod
    def enable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None:
        pass


class SqlServerDatabaseGateway(DatabaseGateway):
    def __init__(self, database_context: DatabaseContext):
        self._database_context: DatabaseContext = database_context

    def generate_where_clause_from_record(self, record: dict, primary_key: list = None) -> str:
        if record is None:
            raise ValueError("Cannot generate where clause from empty record")

        where_clause: str = Constants.DEFAULT_WHERE_CLAUSE

        if primary_key is not None and primary_key != []:
            # If primary key columns were passed in, then reduce the record
            # to only the primary key columns and then make a where clause
            # based only on that.
            record: dict = {x: record[x] for x in primary_key}

        for column in record:
            if record[column] is None:
                where_clause = where_clause + f" and [{column}] is NULL "
            elif isinstance(record[column], int):
                where_clause = where_clause + f" and [{column}] = {record[column]} "
            else:
                # default to a string-like value
                where_clause = where_clause + f" and [{column}] = '{record[column]}' "

        return where_clause

    def generate_update_set_clause_for_column(self, column: str, replacement_value: vars) -> str:
        if replacement_value is None:
            return f" set [{column}] = NULL"
        elif isinstance(replacement_value, int):
            return f" set [{column}] = {replacement_value} "
        else:
            # default to a string-like value
            return f" set [{column}] = '{replacement_value}'"

    def append_where_column_is_not_null(self, column: str, where_clause: Optional[str] = None) -> str:
        if where_clause == "" or where_clause is None:
            where_clause = Constants.DEFAULT_WHERE_CLAUSE

        return where_clause + f" and [{column}] is not null "

    def get_list_of_primary_key_columns_for_table(self, database: str, schema: str, table: str) -> list[str]:
        data_set: dict = self._database_context.query(
            query=f"use [{database}]; "
                  f"select col.[name] as column_name "
                  f"from sys.tables tab "
                  f"inner join sys.schemas as sch "
                  f"on tab.schema_id = sch.schema_id "
                  f"inner join sys.indexes pk "
                  f"on tab.object_id = pk.object_id and pk.is_primary_key = 1 "
                  f"inner join sys.index_columns ic "
                  f"on ic.object_id = pk.object_id and ic.index_id = pk.index_id "
                  f"inner join sys.columns col "
                  f"on pk.object_id = col.object_id and col.column_id = ic.column_id "
                  f"where sch.[name] = '{schema}' "
                  f"and tab.name = '{table}' "
                  f"order by ic.index_column_id asc;",
            values=None
        )

        primary_keys: list[str] = list()
        for datum in data_set:
            primary_keys.append(datum["column_name"])

        return primary_keys

    def get_records_from_table(self, database: str, schema: str, table: str, where_clause: str) -> dict:
        return self._database_context.query(
            query=f"select t1.* from [{database}].[{schema}].[{table}] as t1 {where_clause};",
            values=None
        )

    def update_rows(
            self,
            database: str,
            schema: str,
            table: str,
            set_clause: str,
            where_clause: str
    ) -> None:
        self._database_context.execute(
            query=f"update [{database}].[{schema}].[{table}] "
                  f"{set_clause} {where_clause};",
            values=None
        )

    def update_date_column_with_random_variance(
            self,
            database: str,
            schema: str,
            table: str,
            column: str,
            where_clause: str,
            range_min: int,
            range_max: int
    ) -> None:
        self._database_context.execute(
            query=f"update [{database}].[{schema}].[{table}] "
                  f"set [{column}] = "
                  f"dateadd(day, ({range_min} + floor(rand() * ({range_max} + 1 - {range_min}))), [{column}]) "
                  f"{where_clause} ; ",
            values=None
        )

    def truncate_table(self, database: str, schema: str, table: str) -> None:
        self._database_context.execute(
            query=f"truncate table [{database}].[{schema}].[{table}];",
            values=None
        )

    def delete_rows(self, database: str, schema: str, table: str, where_clause: str) -> None:
        self._database_context.execute(
            query=f"delete from [{database}].[{schema}].[{table}] {where_clause};",
            values=None
        )

    def disable_all_triggers_for_database(self, database: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(128); "
                  f"declare @TableName nvarchar(128); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(128) not null, "
                  f"[table_name] nvarchar(128) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name]) "
                  f"select s.[name], o.[name] "
                  f"from sys.objects as o inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 @Id = [id], @SchemaName = [schema_name], @TableName = [table_name] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = 'disable trigger all on [' + @SchemaName + '].[' + @TableName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def disable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None:
        self._database_context.execute(
            query=f"use {database}; disable trigger all on [{schema}].[{table}];",
            values=None
        )

    def disable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None:
        self._database_context.execute(
            query=f"use {database}; disable trigger [{schema}].[{trigger}] on [{schema}].[{table}];",
            values=None
        )

    def enable_all_triggers_for_database(self, database: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(128); "
                  f"declare @TableName nvarchar(128); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(128) not null, "
                  f"[table_name] nvarchar(128) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name]) "
                  f"select s.[name], o.[name] "
                  f"from sys.objects as o inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 @Id = [id], @SchemaName = [schema_name], @TableName = [table_name] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = 'enable trigger all on [' + @SchemaName + '].[' + @TableName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def enable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None:
        self._database_context.execute(
            query=f"use {database}; enable trigger all on [{schema}].[{table}];",
            values=None
        )

    def enable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None:
        self._database_context.execute(
            query=f"use {database}; enable trigger [{schema}].[{trigger}] on [{schema}].[{table}];",
            values=None
        )

    def disable_all_check_constraints_for_database(self, database: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(256); "
                  f"declare @TableName nvarchar(256); "
                  f"declare @CheckConstraintName nvarchar(256); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(256) not null, "
                  f"[table_name] nvarchar(256) not null, "
                  f"[check_constraint_name] nvarchar(256) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name], [check_constraint_name]) "
                  f"select s.[name], o.[name], cc.[name] "
                  f"from sys.check_constraints as cc "
                  f"inner join sys.objects as o on cc.parent_object_id = o.object_id "
                  f"inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 "
                  f"@Id = [id], @SchemaName = [schema_name], "
                  f"@TableName = [table_name], @CheckConstraintName = [check_constraint_name] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = "
                  f"'alter table [' + @SchemaName + '].[' + @TableName + '] "
                  f"nocheck constraint [' + @CheckConstraintName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def disable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None:
        """
        Disables all check constraints on the specified table.

        Note that we cannot use the fancier T-SQL statement which disables all
        constraints on a table because SQL Server sees foreign keys as
        constraints as well, and so those will also be disabled. It is necessary
        to get the names of the check constraints on a table and then iterate through
        each individually to disable.

        :param database: database context to use
        :param schema: the schema in which the table exists
        :param table: the target table
        :return: None
        """
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(256); "
                  f"declare @TableName nvarchar(256); "
                  f"declare @CheckConstraintName nvarchar(256); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(256) not null, "
                  f"[table_name] nvarchar(256) not null, "
                  f"[check_constraint_name] nvarchar(256) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name], [check_constraint_name]) "
                  f"select s.[name], o.[name], cc.[name] "
                  f"from sys.check_constraints as cc "
                  f"inner join sys.objects as o on cc.parent_object_id = o.object_id "
                  f"inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U' "
                  f"and o.[name] = '{table}' "
                  f"and s.[name] = '{schema}'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 "
                  f"@Id = [id], @SchemaName = [schema_name], "
                  f"@TableName = [table_name], @CheckConstraintName = [check_constraint_name] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = "
                  f"'alter table [' + @SchemaName + '].[' + @TableName + '] "
                  f"nocheck constraint [' + @CheckConstraintName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def disable_single_check_constraint_for_table(
            self,
            database: str,
            schema: str,
            table: str,
            check_constraint: str
    ) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"alter table [{schema}].[{table}] nocheck constraint [{check_constraint}];",
            values=None
        )

    def enable_all_check_constraints_for_database(self, database: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(256); "
                  f"declare @TableName nvarchar(256); "
                  f"declare @CheckConstraintName nvarchar(256); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(256) not null, "
                  f"[table_name] nvarchar(256) not null, "
                  f"[check_constraint_name] nvarchar(256) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name], [check_constraint_name]) "
                  f"select s.[name], o.[name], cc.[name] "
                  f"from sys.check_constraints as cc "
                  f"inner join sys.objects as o on cc.parent_object_id = o.object_id "
                  f"inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 "
                  f"@Id = [id], @SchemaName = [schema_name], "
                  f"@TableName = [table_name], @CheckConstraintName = [check_constraint_name] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = "
                  f"'alter table [' + @SchemaName + '].[' + @TableName + '] "
                  f"check constraint [' + @CheckConstraintName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def enable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(256); "
                  f"declare @TableName nvarchar(256); "
                  f"declare @CheckConstraintName nvarchar(256); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(256) not null, "
                  f"[table_name] nvarchar(256) not null, "
                  f"[check_constraint_name] nvarchar(256) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name], [check_constraint_name]) "
                  f"select s.[name], o.[name], cc.[name] "
                  f"from sys.check_constraints as cc "
                  f"inner join sys.objects as o on cc.parent_object_id = o.object_id "
                  f"inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U' "
                  f"and o.[name] = '{table}' "
                  f"and s.[name] = '{schema}'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 "
                  f"@Id = [id], @SchemaName = [schema_name], "
                  f"@TableName = [table_name], @CheckConstraintName = [check_constraint_name] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = "
                  f"'alter table [' + @SchemaName + '].[' + @TableName + '] "
                  f"check constraint [' + @CheckConstraintName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def enable_single_check_constraint_for_table(
            self,
            database: str,
            schema: str,
            table: str,
            check_constraint: str
    ) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"alter table [{schema}].[{table}] check constraint [{check_constraint}];",
            values=None
        )

    def disable_all_foreign_keys_for_database(self, database: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(256); "
                  f"declare @TableName nvarchar(256); "
                  f"declare @ForeignKeyName nvarchar(256); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(256) not null, "
                  f"[table_name] nvarchar(256) not null, "
                  f"[foreign_key] nvarchar(256) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name], [foreign_key]) "
                  f"select s.[name], o.[name], fk.[name] "
                  f"from sys.foreign_keys as fk "
                  f"inner join sys.objects as o on fk.parent_object_id = o.object_id "
                  f"inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 "
                  f"@Id = [id], @SchemaName = [schema_name], "
                  f"@TableName = [table_name], @ForeignKeyName = [foreign_key] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = "
                  f"'alter table [' + @SchemaName + '].[' + @TableName + '] "
                  f"nocheck constraint [' + @ForeignKeyName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def disable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(256); "
                  f"declare @TableName nvarchar(256); "
                  f"declare @ForeignKeyName nvarchar(256); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(256) not null, "
                  f"[table_name] nvarchar(256) not null, "
                  f"[foreign_key] nvarchar(256) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name], [foreign_key]) "
                  f"select s.[name], o.[name], fk.[name] "
                  f"from sys.foreign_keys as fk "
                  f"inner join sys.objects as o on fk.parent_object_id = o.object_id "
                  f"inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U' "
                  f"and o.[name] = '{table}' "
                  f"and s.[name] = '{schema}'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 "
                  f"@Id = [id], @SchemaName = [schema_name], "
                  f"@TableName = [table_name], @ForeignKeyName = [foreign_key] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = "
                  f"'alter table [' + @SchemaName + '].[' + @TableName + '] "
                  f"nocheck constraint [' + @ForeignKeyName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def disable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"alter table [{schema}].[{table}] nocheck constraint [{foreign_key}];",
            values=None
        )

    def enable_all_foreign_keys_for_database(self, database: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(256); "
                  f"declare @TableName nvarchar(256); "
                  f"declare @ForeignKeyName nvarchar(256); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(256) not null, "
                  f"[table_name] nvarchar(256) not null, "
                  f"[foreign_key] nvarchar(256) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name], [foreign_key]) "
                  f"select s.[name], o.[name], fk.[name] "
                  f"from sys.foreign_keys as fk "
                  f"inner join sys.objects as o on fk.parent_object_id = o.object_id "
                  f"inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 "
                  f"@Id = [id], @SchemaName = [schema_name], "
                  f"@TableName = [table_name], @ForeignKeyName = [foreign_key] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = "
                  f"'alter table [' + @SchemaName + '].[' + @TableName + '] "
                  f"check constraint [' + @ForeignKeyName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def enable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"declare @Id int; "
                  f"declare @SchemaName nvarchar(256); "
                  f"declare @TableName nvarchar(256); "
                  f"declare @ForeignKeyName nvarchar(256); "
                  f"declare @SqlStatement nvarchar(max); "
                  f"drop table if exists #tables; "
                  f"create table #tables ( "
                  f"[id] int identity(1,1) not null, "
                  f"[schema_name] nvarchar(256) not null, "
                  f"[table_name] nvarchar(256) not null, "
                  f"[foreign_key] nvarchar(256) not null, "
                  f"[processed] bit default 0); "
                  f"insert into #tables ([schema_name], [table_name], [foreign_key]) "
                  f"select s.[name], o.[name], fk.[name] "
                  f"from sys.foreign_keys as fk "
                  f"inner join sys.objects as o on fk.parent_object_id = o.object_id "
                  f"inner join sys.schemas as s on o.schema_id = s.schema_id "
                  f"where o.[type] = 'U' "
                  f"and o.[name] = '{table}' "
                  f"and s.[name] = '{schema}'; "
                  f"while exists (select 1 from #tables where [processed] = 0) "
                  f"begin "
                  f"select top 1 "
                  f"@Id = [id], @SchemaName = [schema_name], "
                  f"@TableName = [table_name], @ForeignKeyName = [foreign_key] "
                  f"from #tables "
                  f"where [processed] = 0; "
                  f"select @SqlStatement = "
                  f"'alter table [' + @SchemaName + '].[' + @TableName + '] "
                  f"check constraint [' + @ForeignKeyName + '];' "
                  f"execute sp_executesql @SqlStatement; "
                  f"update #tables set [processed] = 1 where [id] = @Id; "
                  f"end",
            values=None
        )

    def enable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None:
        self._database_context.execute(
            query=f"use [{database}]; "
                  f"alter table [{schema}].[{table}] check constraint [{foreign_key}];",
            values=None
        )


class PostgresDatabaseGateway(DatabaseGateway):
    def __init__(self, database_context: DatabaseContext):
        self._database_context: DatabaseContext = database_context

    def generate_where_clause_from_record(self, record: dict, primary_key: list) -> str:
        pass

    def generate_update_set_clause_for_column(self, column: str, replacement_value: vars) -> str:
        pass

    def append_where_column_is_not_null(self, column: str, where_clause: str) -> str:
        pass

    def get_list_of_primary_key_columns_for_table(self, database: str, schema: str, table: str) -> list:
        pass

    def get_records_from_table(self, database: str, schema: str, table: str, where_clause: str) -> dict:
        pass

    def update_rows(
            self,
            database: str,
            schema: str,
            table: str,
            set_clause: str,
            where_clause: str
    ) -> None:
        pass

    def update_date_column_with_random_variance(
            self, database: str, schema: str, table: str, column: str, where_clause: str,
            range_min: int, range_max: int
    ) -> None:
        pass

    def truncate_table(self, database: str, schema: str, table: str) -> None:
        self._database_context.execute(
            query=f'truncate table "{database}"."{schema}"."{table}";',
            values=None
        )

    def delete_rows(self, database: str, schema: str, table: str, where_clause: str) -> None:
        self._database_context.execute(
            query=f'delete from "{database}"."{schema}"."{table}" {where_clause};'
        )

    def disable_all_triggers_for_database(self, database: str) -> None:
        pass

    def disable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def disable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None:
        pass

    def enable_all_triggers_for_database(self, database: str) -> None:
        pass

    def enable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def enable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None:
        pass

    def disable_all_check_constraints_for_database(self, database: str) -> None:
        pass

    def disable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def disable_single_check_constraint_for_table(
            self, database: str, schema: str, table: str, check_constraint: str) -> None:
        pass

    def enable_all_check_constraints_for_database(self, database: str) -> None:
        pass

    def enable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def enable_single_check_constraint_for_table(
            self, database: str, schema: str, table: str, check_constraint: str) -> None:
        pass

    def disable_all_foreign_keys_for_database(self, database: str) -> None:
        pass

    def disable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def disable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None:
        pass

    def enable_all_foreign_keys_for_database(self, database: str) -> None:
        pass

    def enable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def enable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None:
        pass


class DatabaseGatewayFactory:
    @staticmethod
    def create_database_gateway(database_context: DatabaseContext) -> DatabaseGateway:
        if database_context.get_database_type() == SupportedDatabases.MICROSOFT_SQL_SERVER.value:
            return SqlServerDatabaseGateway(database_context)
        elif database_context.get_database_type() == SupportedDatabases.POSTGRESQL.value:
            return PostgresDatabaseGateway(database_context)
        else:
            raise ValueError(f"{database_context.get_database_type()} does not have a database gateway.")
