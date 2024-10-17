import sqlite3
import argparse
import yaml
import logging
from pathlib import Path


def add_tables(db_cursor: sqlite3.Cursor):
    """
    Creates the telemetry and commands tables needed for the database.
    :param db_cursor: The cursor database handle.
    :return:
    """
    db_cursor.execute('create table if not exists union_selections('
                      'id INTEGER primary key,'
                      'union INTEGER NOT NULL,'
                      'union_field INTEGER NOT NULL,'
                      'telemetry_item INTEGER ,'
                      'command_item INTEGER,'
                      'FOREIGN KEY (union) REFERENCES fields(id),'
                      'FOREIGN KEY (union_field) REFERENCES symbols(field),'
                      'UNIQUE (union, union_field));')

def read_yaml(yaml_file: str) -> dict:
    yaml_data = yaml.load(open(yaml_file, 'r'),
                          Loader=yaml.FullLoader)
    return yaml_data


# FIXME:It looks like we don't use this function. We should remove it.
def get_module_id(module_name: str, db_cursor: sqlite3.Cursor) -> tuple:
    """
    Fetches the id of the module whose name module_name
    :param module_name: The name of the module as it appears in the database.
    :param db_cursor: The cursor that points to the database.
    :return: The module id.
    """
    module_id = db_cursor.execute('SELECT * FROM modules where name =?',
                                  (module_name,))

    # FIXME: This is a possible approach to take when the module does not exist. Will re-address in the future.
    # if module_id is None:
    #     db_cursor.execute('INSERT INTO modules(name) '
    #                       'values(?)',
    #                       (module_name,))
    #     logging.warning(f'{module_name} module was added to the database.')

    # module_id = db_cursor.execute('SELECT * FROM modules where name =?',
    #                               (module_name,)).fetchone()

    return module_id.fetchone()


def get_symbol_id(symbol_name: str, db_cursor: sqlite3.Cursor) -> tuple:
    """
    Fetches the id of the symbol whose name symbol_name
    :param symbol_name: The name of the module as it appears in the database.
    :param db_cursor: The cursor that points to the database.
    :return: The module id.
    """
    symbol_id = db_cursor.execute('SELECT * FROM symbols where name =?',
                                  (symbol_name,))
    return symbol_id.fetchone()





def write_telemetry_records(telemetry_data: dict, modules_dict: dict, db_cursor: sqlite3.Cursor):
    """
    Scans telemetry_data and writes it to the database. Please note that the database changes are not committed. Thus,
    it is the responsibility of the caller to commit these changes to the database.
    :param telemetry_data:
    :param db_cursor:
    :param modules_dict: A dictionary of the form {module_id: module_name}
    :return:
    """
    if telemetry_data['modules'] is None:
        # This has a 'modules' key, but its empty.  Skip it.
        pass
    else:
        for module_name in telemetry_data['modules']:
            if 'telemetry' in telemetry_data['modules'][module_name]:
                if telemetry_data['modules'][module_name]['telemetry'] is None:
                    # This has a 'telemetry' key, but its empty.  Skip it.
                    pass
                else:
                    for message in telemetry_data['modules'][module_name]['telemetry']:
                        message_id = None
                        symbol = None
                        message_dict = telemetry_data['modules'][module_name]['telemetry'][message]
                        name = message
                        min_rate = None

                        # Check for empty values
                        # FIXME: This logic is starting to look convoluted. The schema might help with this.
                        if 'msgID' in message_dict:
                            if message_dict['msgID'] is None:
                                message_id = 0
                                logging.warning(
                                    f"modules.{module_name}.telemetry.{name}.msgID must not be empty. Setting it to 0.")
                                # continue
                            else:
                                message_id = message_dict['msgID']
                        else:
                            logging.error(f"modules.{module_name}.telemetry.{name}.msgID key must exist.  Skipping.")

                        if 'min_rate' in message_dict:
                            if message_dict['min_rate'] is None:
                                continue
                            else:
                                min_rate = message_dict['min_rate']

                        if 'struct' in message_dict:
                            if message_dict['struct'] is None:
                                logging.error(
                                    f"modules.{module_name}.telemetry.{name}.struct must not be empty. Skipping.")
                                continue
                            else:
                                symbol = get_symbol_id(message_dict['struct'], db_cursor)
                        else:
                            logging.error(f"modules.{module_name}.telemetry.{name}.struct key must exist. Skipping.")

                        # If the symbol does not exist, we skip it
                        if symbol is None:
                            logging.error(
                                f"modules.{module_name}.telemetry.{name}.struct could not be found.  Skipping.")
                        else:
                            symbol_id = symbol[0]

                            # FIXME:Is there a point to this statement?
                            macro = name

                            # Write our telemetry record to the database.
                            db_cursor.execute(
                                'INSERT INTO telemetry(name, message_id, macro, symbol, module, min_rate) '
                                'VALUES (?, ?, ?, ?, ?, ?)',
                                (name, message_id, macro, symbol_id, modules_dict[module_name], min_rate))

            if 'modules' in telemetry_data['modules'][module_name]:
                write_telemetry_records(telemetry_data['modules'][module_name], modules_dict, db_cursor)










def write_command_records(command_data: dict, modules_dict: dict, db_cursor: sqlite3.Cursor):
    """
    Scans command_data and writes it to the database. Please note that the database changes are not committed. Thus,
    it is the responsibility of the caller to commit these changes to the database.
    :param command_data:
    :param db_cursor:
    :return:
    """
    # This has a modules key, but its empty.  Skip it.
    if command_data['modules'] is None:
        return

    for module_name in command_data['modules']:
        # FIXME: We need that schema. If we had the schema, we wouldn't need all these checks and the code would look cleaner.
        if 'commands' in command_data['modules'][module_name]:
            if command_data['modules'][module_name]['commands'] is None:
                # This has a command key, but no commands are defined.  Skip it.
                continue

            for command in command_data['modules'][module_name]['commands']:
                command_dict = command_data['modules'][module_name]['commands'][command]

                if command_dict['msgID'] is None:
                    command_dict['msgID'] = 0
                    logging.warning(f"modules.{module_name}.commands.{command}.msgID must not be empty.  Setting it to 0.")
                    # continue

                message_id = command_dict['msgID']

                if message_id is None:
                    logging.error(
                        f"modules.{module_name}.commands.{command} message does not have any msgID defined. Skipping.")
                    continue

                if command_data['modules'][module_name]['commands'] is None:
                    logging.error(
                        f"modules.{module_name}.commands.{command} message does not have any actual commands defined.  Skipping.")
                    continue

                sub_commands = command_data['modules'][module_name]['commands']

                if 'commands' in sub_commands[command]:
                    for sub_command in sub_commands[command]['commands']:
                        if sub_commands[command]['commands'] is None:
                            logging.error(
                                f"modules.{module_name}.commands.{command}.{sub_command} command is empty.  Skipping.")
                            continue

                        sub_command_dict = sub_commands[command]['commands']
                        name = sub_command

                        symbol = get_symbol_id(sub_command_dict[name]['struct'], db_cursor)

                        # If the symbol does not exist, we skip it
                        if not symbol:
                            logging.error(
                                f"modules.{module_name}.commands.{command}.{sub_command}.{sub_command_dict[name]['struct']} was not found.  Skipping.")
                        else:
                            symbol_id = symbol[0]

                            if sub_command_dict[name]['cc'] is None:
                                logging.error(
                                    f"modules.{module_name}.commands.{command}.cc must not be empty.  Skipping.")
                                continue

                            command_code = sub_command_dict[name]['cc']

                            macro = command

                            # Write our command record to the database.
                            db_cursor.execute(
                                'INSERT INTO commands(name, command_code, message_id, macro, symbol ,module) '
                                'VALUES (?, ?, ?, ?, ?, ?)',
                                (name, command_code, message_id, macro, symbol_id, modules_dict[module_name],))

        if 'modules' in command_data['modules'][module_name]:
            write_command_records(command_data['modules'][module_name], modules_dict, db_cursor)



def write_tlm_cmd_data(yaml_data: dict, db_cursor: sqlite3.Cursor):

    # Get all modules needed now that they are on the database.
    modules_dict = {}
    for module_id, module_name in db_cursor.execute('select id, name from modules').fetchall():
        modules_dict[module_name] = module_id

    write_telemetry_records(yaml_data, modules_dict, db_cursor)

    telemetry_dict = {}
    for tlm_id, tlm_name in db_cursor.execute('select id, name from telemetry').fetchall():
        telemetry_dict[tlm_name] = tlm_id

    write_command_records(yaml_data, modules_dict, db_cursor)





def parse_cli() -> argparse.Namespace:
    """
    Parses cli arguments.
    :return: The namespace that has all the arguments that have been parsed.
    """
    parser = argparse.ArgumentParser(description='Takes in paths to yaml file and sqlite database.')
    parser.add_argument('--yaml_path', type=str,
                        help='The file path to the YAML file which contains telemetry and command metadata.',
                        required=True)
    parser.add_argument('--sqlite_path', type=str,
                        help='The file path to the sqlite database', required=True)

    return parser.parse_args()
    

def get_module_by_path(module_path: str, yaml_data: dict):
    module_yaml_dict = yaml_data

    for module_name in module_path.split("/"):
        if module_name != "":
            if "modules" in module_yaml_dict:
                if module_name not in module_yaml_dict["modules"]:
                    logging.error('"{0}" is not found. Aborting'.format(module_name))
                    exit(-1)
                else:
                    module_yaml_dict = module_yaml_dict["modules"][module_name]
            else:
                logging.error('"{0}" is not found. Aborting'.format(module_name))
                exit(-1)

    return module_yaml_dict


def merge_all(database_path: str, module_path: str, yaml_file: str):
    db_handle = sqlite3.connect(database_path)
    db_cursor = db_handle.cursor()

    add_tables(db_cursor)

    full_yaml_data = read_yaml(yaml_file)
    module_data = get_module_by_path(module_path, full_yaml_data)
    
    # Write all the data to the database.
    write_tlm_cmd_data(module_data, db_cursor)

    # Save our changes to the database.
    db_handle.commit()


def main():
    args = parse_cli()
    merge_all(args.sqlite_path, args.module_path, args.yaml_path)


if __name__ == '__main__':
    main()
