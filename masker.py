from mask.config.operations import load_configuration
from mask.database_access.database_gateway import DatabaseGateway
from mask.rules.rule import Rule
from mask.rules.rules_factory import RulesFactory

import argparse
import sys
import threading


def main(instruction_set: dict, database_gateway: DatabaseGateway) -> None:
    rule_controller: list[Rule] = []

    # This has a list of distinct group numbers from the instruction set. It
    # does not matter whether the integers are sequential (e.g. 1, 2, 3, ..., n),
    # but rather we'll sort them and run them from lowest to highest regardless
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
    # the lowest next integer.
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
    """ If executing as program, bootstrap then call main() """

    parser = argparse.ArgumentParser(
        description='A data masking utility for relational database management systems'
    )
    parser.add_argument("--config", required=True, help="path to the configuration file")
    args = parser.parse_args()

    ins, db = load_configuration(args.config)

    main(instruction_set=ins, database_gateway=db)
