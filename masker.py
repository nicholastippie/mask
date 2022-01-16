from mask.config.operations import *
from mask.database.database_gateway import DatabaseGateway
from mask.rules.rule import Rule
from mask.rules.rules_factory import RulesFactory

import argparse
import sys
import threading


def main(arguments) -> None:
    configuration_settings: dict = get_configuration_settings_from_file(
        configuration_file=arguments.config
    )
    instruction_set: dict = get_instruction_set_from_file(
        instruction_set_file=configuration_settings["instruction_set_file"]
    )
    database_gateway: DatabaseGateway = create_database_gateway_from_configuration_settings(
        configuration_settings=configuration_settings
    )

    rule_controller: list[Rule] = []

    # This has a list of distinct group numbers from the instruction set. It
    # does not matter whether the integers are sequential (e.g. 1, 2, 3, ..., n),
    # but rather we'll sort them and run them from smallest to largest regardless
    # of the actual integers chosen (e.g. 5, 23, 97, ..., n).
    groups: list[int] = []

    for instructions in instruction_set:
        rule: Rule = RulesFactory.create_rule(instructions, database_gateway)
        if rule.group not in groups:
            groups.append(rule.group)

        rule_controller.append(rule)

    if rule_controller == [] or rule_controller is None:
        print(f"No rules found in instruction set.")
        sys.exit()

    # Sort group numbers in reverse order so that we can pop them. Pop will
    # give the last element in the list (and remove it), which we want to be
    # the smallest next integer.
    groups.sort(reverse=True)

    while groups:
        group: int = groups.pop()
        rules_in_group: list[Rule] = [r for r in rule_controller if r.group == group]
        started_threads: list[any] = []

        for rule_to_execute in rules_in_group:
            thread = threading.Thread(target=rule_to_execute.execute)
            thread.start()
            print(f"Thread started = {rule_to_execute}")
            started_threads.append(thread)

        for started_thread in started_threads:
            started_thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='A data masking utility for relational database management systems'
    )
    parser.add_argument("--config", required=True, help="path to the configuration file")
    args = parser.parse_args()

    main(args)
