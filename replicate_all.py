import os
import subprocess
import argparse
import configparser

class DBReplicator:

    def __init__(self):
        pass

    def main(self):
        schemas = self.get_schemas()
        schema_list = ','.join(schemas)
        self.write_config_file(schema_list)
        status_code = self.execute_replicate_task()

    def get_schemas(self):
        try:
            output = subprocess.check_output(
                "/opt/vertica/bin/vsql -CAtX -c 'SELECT schema_name FROM schemata WHERE NOT is_system_schema;'",
                shell=True
            )
            schemas = output.decode('ascii').splitlines()
            return schemas
        except Exception as e:
            print(str(e))

    def write_config_file(self, object):
        config = self.generate_ini(object)
        with open('db_replicate.ini', 'w') as configfile:
            config.write(configfile)

    def generate_ini(self, schema_list):
        config = configparser.ConfigParser()
        config.optionxform = str
        config.read('db_replicate.ini')
        config['Misc'].pop('objects', None)
        config['Misc']['includeObjects'] = schema_list
        return config

    def execute_replicate_task(self):
        return os.system('/opt/vertica/bin/vbr -t replicate -c db_replicate.ini')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Replicate full database")

    dbr = DBReplicator()
    dbr.main()
