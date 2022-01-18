from mask.rules.rule import Rule

from dataclasses import dataclass


@dataclass
class DatabaseObjectRule(Rule):
    database: str = ""
    schema: str = ""
    table: str = ""

    def validate_instructions(self) -> None:
        super().validate_instructions()
        if self.database == "":
            raise ValueError(f"'database' property not set for {self}")
        if self.schema == "":
            raise ValueError(f"'schema' property not set for {self}")
        if self.database == "*":
            raise ValueError(f"Wildcard character is not allowed for 'database' property "
                             f"for {self}")
        if self.table == "*":
            raise ValueError(f"Wildcard character is not allowed for 'table' property "
                             f"for {self}")

    def execute(self) -> None:
        pass


@dataclass
class DisableTriggerRule(DatabaseObjectRule):
    """ Disables one or more table triggers in a database """

    trigger: str = ""

    def execute(self) -> None:
        """
        This rule is the reverse of EnableTriggerRule.

        The instruction set has four properties for this rule: database,
        schema, table, and trigger. A wildcard character (*) can be used
        to tell the masking engine to disable all triggers for the database
        or for a specific table. This character does not handle partial namesS
        for any properties (e.g., "*_audit" for all audit tables). Think
        "select * from", not "where first_name like '%ohn'".

        A wildcard character for "database" is illegal, since this rule will
        not disable all triggers across all database, but only within one
        database at a time. The "database" value tells the masking engine
        which database context to use.

        A wildcard character for "schema" means disable all triggers within
        all schema (and tables) in the database: That is, disable all triggers
        in the database. The "table" and "trigger" properties are ignored if
        this wildcard is used.

        A wildcard character for "table" is illegal, since this situation is
        not supported: The masking engine will not disable all triggers for
        all tables within a schema. This could be a future feature.

        A wildcard character for "trigger" will disable all triggers for
        the provided table, assuming schema and table are valid database
        objects.

        Note that this rule disables triggers associated with tables and not
        database-level or higher-level triggers. Use DisableDatabaseTriggerRule
        and DisableServerTriggerRule, respectively.

        :return: None
        """
        if self.schema == "*":
            self.database_gateway.disable_all_triggers_for_database(self.database)
            return

        if self.trigger == "*":
            self.database_gateway.disable_all_triggers_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.disable_single_trigger_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            trigger=self.trigger
        )


@dataclass
class EnableTriggerRule(DatabaseObjectRule):
    """ Enables one or more table triggers in a database """

    trigger: str = ""

    def execute(self) -> None:
        """
        This rule is the reverse of DisableTriggerRule.

        The instruction set has four properties for this rule: database,
        schema, table, and trigger. A wildcard character (*) can be used
        to tell the masking engine to enable all triggers for the database
        or for a specific table. This character does not handle partial names
        for any properties (e.g., "*_audit" for all audit tables). Think
        "select * from", not "where first_name like '%ohn'".

        A wildcard character for "database" is illegal, since this rule will
        not enable all triggers across all database, but only within one
        database at a time. The "database" value tells the masking engine
        which database context to use.

        A wildcard character for "schema" means enable all triggers within
        all schema (and tables) in the database: That is, enable all triggers
        in the database. The "table" and "trigger" properties are ignored if
        this wildcard is used.

        A wildcard character for "table" is illegal, since this situation is
        not supported: The masking engine will not enable all triggers for
        all tables within a schema. This could be a future feature.

        A wildcard character for "trigger" will enable all triggers for
        the provided table, assuming schema and table are valid database
        objects.

        Note that this rule enables triggers associated with tables and not
        database-level or higher-level triggers. Use EnableDatabaseTriggerRule
        and EnableServerTriggerRule, respectively.

        :return: None
        """
        if self.schema == "*":
            self.database_gateway.enable_all_triggers_for_database(self.database)
            return

        if self.trigger == "*":
            self.database_gateway.enable_all_triggers_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.enable_single_trigger_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            trigger=self.trigger
        )


@dataclass
class DisableCheckConstraintRule(DatabaseObjectRule):
    """ Disable one or more table check constraints in a database """

    check_constraint: str = ""

    def execute(self) -> None:
        if self.schema == "*":
            self.database_gateway.disable_all_check_constraints_for_database(self.database)
            return

        if self.check_constraint == "*":
            self.database_gateway.disable_all_check_constraints_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.disable_single_check_constraint_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            check_constraint=self.check_constraint
        )


@dataclass
class EnableCheckConstraintRule(DatabaseObjectRule):
    """ Enable one or more table check constraints in a database """

    check_constraint: str = ""

    def execute(self) -> None:
        if self.schema == "*":
            self.database_gateway.enable_all_check_constraints_for_database(self.database)
            return

        if self.check_constraint == "*":
            self.database_gateway.enable_all_check_constraints_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.enable_single_check_constraint_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            check_constraint=self.check_constraint
        )


@dataclass
class DisableForeignKeyRule(DatabaseObjectRule):
    """ Disable one or more foreign keys in a database """

    foreign_key: str = ""

    def execute(self) -> None:
        if self.schema == "*":
            self.database_gateway.disable_all_foreign_keys_for_database(self.database)
            return

        if self.foreign_key == "*":
            self.database_gateway.disable_all_foreign_keys_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.disable_single_foreign_key_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            foreign_key=self.foreign_key
        )


@dataclass
class EnableForeignKeyRule(DatabaseObjectRule):
    """ Enable one or more foreign keys in a database """

    foreign_key: str = ""

    def execute(self) -> None:
        if self.schema == "*":
            self.database_gateway.enable_all_foreign_keys_for_database(self.database)
            return

        if self.foreign_key == "*":
            self.database_gateway.enable_all_foreign_keys_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.enable_single_foreign_key_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            foreign_key=self.foreign_key
        )
