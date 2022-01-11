from mask.file import generate_dict_from_json
from mask.database_access.database_context import DatabaseContextFactory
from mask.database_access.database_gateway import DatabaseGateway, DatabaseGatewayFactory


def get_configuration_settings_from_file(configuration_file: str) -> dict:
    try:
        configuration_settings: dict = generate_dict_from_json(configuration_file)
    except IOError:
        print(f"There was an error reading the configuration file")
        raise

    return configuration_settings


def get_instruction_set_from_file(instruction_set_file: str) -> dict:
    try:
        instruction_set: dict = generate_dict_from_json(instruction_set_file)
    except IOError:
        print(f"There was an error reading the instruction set file.")
        raise

    return instruction_set


def create_database_gateway_from_configuration_settings(configuration_settings: dict) -> DatabaseGateway:
    try:
        database_gateway: DatabaseGateway = DatabaseGatewayFactory.create_database_gateway(
            database_context=DatabaseContextFactory.create_database_context(
                database_type=configuration_settings["database_type"],
                server=configuration_settings["database_server"],
                user=configuration_settings["database_user"],
                password=configuration_settings["database_password"],
                database=configuration_settings["database_name"]
            )
        )
    except KeyError as ke:
        print(f"{ke} is missing from the configuration file or an invalid configuration setting")
        raise

    return database_gateway
