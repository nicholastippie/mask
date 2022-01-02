from mask.file import generate_dict_from_json
from mask.database_access.database_context import DatabaseContextFactory
from mask.database_access.database_gateway import DatabaseGateway, DatabaseGatewayFactory


def load_configuration(configuration_file: str) -> (dict, DatabaseGateway):
    try:
        configuration: dict = generate_dict_from_json(configuration_file)
    except IOError:
        print(f"There was an error reading the configuration file.")
        raise

    try:
        instruction_set: dict = generate_dict_from_json(configuration["instruction_file"])
        database_gateway: DatabaseGateway = DatabaseGatewayFactory.create_database_gateway(
            database_context=DatabaseContextFactory.create_database_context(
                database_type=configuration["database_type"],
                server=configuration["database_server"],
                user=configuration["database_user"],
                password=configuration["database_password"],
                database=configuration["database_name"]
            )
        )
    except KeyError as ke:
        print(f"{ke} is either missing from the configuration file or invalid configuration setting.")
        raise

    return instruction_set, database_gateway
