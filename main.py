from mask.config.operations import load_configuration
from mask.rules.rule import Rule
from mask.rules.rules_factory import RulesFactory
from mask.database_access.database_gateway import DatabaseGateway

import getopt
import sys


def main(instruction_set: dict, database_gateway: DatabaseGateway) -> None:
    rule_controller: list[Rule] = []

    for instructions in instruction_set:
        rule: Rule = RulesFactory.create_rule(instructions, database_gateway)
        rule.validate_instructions()
        rule_controller.append(rule)

    for rule in rule_controller:
        rule.execute()
        print(f"Executed {rule}")


def usage():
    print("main.py [-c|--config=<ConfigFilePath>] [-h|--help]")


if __name__ == "__main__":
    """ If executing as program, bootstrap then call main() """
    try:
        options, arguments = getopt.getopt(sys.argv[1:], "hc", ["help", "config="])
    except getopt.GetoptError as exception:
        print(exception)
        usage()
        sys.exit(2)

    configuration_file = None

    for option, value in options:
        if option in ("-h", "--help"):
            usage()
            sys.exit()
        elif option in ("-c", "--config"):
            configuration_file = value
        else:
            assert False, f"Option '{option}' not recognized"

    ins, db = load_configuration(configuration_file)

    main(instruction_set=ins, database_gateway=db)
