from mask.database_access.database_gateway import DatabaseGateway
from mask.rules.rule import Rule
from mask.rules.data_rules import (
    FakeStringSubstitutionRule,
    FakeSsnSubstitutionRule,
    StaticStringSubstitutionRule,
    DateVarianceRule,
    TruncateTableRule,
    DeleteRowsRule
)
from mask.rules.database_object_rules import (
    DisableTriggerRule,
    EnableTriggerRule,
    DisableCheckConstraintRule,
    EnableCheckConstraintRule,
    DisableForeignKeyRule,
    EnableForeignKeyRule
)


class RulesFactory:
    @staticmethod
    def create_rule(instructions: dict, database_gateway: DatabaseGateway) -> Rule:
        """
        Creates and returns an instance of a data masking rule for the
        rule type provided in the instructions.

        :param instructions: Instructions which contain the rule to use
        :param database_gateway: Concrete implementation of the database gateway
        :return: Instance of the data masking rule
        """

        try:
            if instructions["rule"] == "fake_string_substitution":
                return FakeStringSubstitutionRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    column=instructions["column"],
                    where_clause=instructions["where_clause"],
                    data_set_path=instructions["data_set_path"],
                    data_set_key=instructions["data_set_key"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "static_string_substitution":
                return StaticStringSubstitutionRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    column=instructions["column"],
                    static_value=instructions["static_value"],
                    where_clause=instructions["where_clause"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "fake_ssn_substitution":
                return FakeSsnSubstitutionRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    column=instructions["column"],
                    seperator=instructions["seperator"],
                    ignore_null=instructions["ignore_null"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "date_variance":
                return DateVarianceRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    column=instructions["column"],
                    range=instructions["range"],
                    where_clause=instructions["where_clause"],
                    method=instructions["method"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "truncate_table":
                return TruncateTableRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "delete_rows":
                return DeleteRowsRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    where_clause=instructions["where_clause"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "disable_trigger":
                return DisableTriggerRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    trigger=instructions["trigger"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "enable_trigger":
                return EnableTriggerRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    trigger=instructions["trigger"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "disable_check_constraint":
                return DisableCheckConstraintRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    check_constraint=instructions["check_constraint"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "enable_check_constraint":
                return EnableCheckConstraintRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    check_constraint=instructions["check_constraint"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "disable_foreign_key":
                return DisableForeignKeyRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    foreign_key=instructions["foreign_key"],
                    database_gateway=database_gateway
                )
            elif instructions["rule"] == "enable_foreign_key":
                return EnableForeignKeyRule(
                    database=instructions["database"],
                    schema=instructions["schema"],
                    table=instructions["table"],
                    foreign_key=instructions["foreign_key"],
                    database_gateway=database_gateway
                )
            else:
                raise ValueError(f"'{instructions['rule']}' is not a recognized rule type")
        except KeyError as ke:
            print(f"{ke} is missing from the instructions for the {instructions['rule']} rule.")
            raise
