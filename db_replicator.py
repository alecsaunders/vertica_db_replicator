import os
import sys
import subprocess
import argparse
import configparser


class DBReplicator:
    def __init__(self, config_file, start_with_all_schemas, include, exclude):
        self.config_file = config_file
        self.start_with_all_schemas = start_with_all_schemas
        self.include = include
        self.exclude = exclude

    def main(self):
        if self.start_with_all_schemas:
            schemas = self.get_schemas()
            schema_list = ','.join(schemas).strip()
            schema_list = schema_list + ',' if self.include else schema_list
        else:
            schema_list = ''
        include_list = schema_list + self.include if self.include else schema_list
        self.write_config_file(include_list, self.exclude)
        status_code = self.execute_replicate_task()

    def get_schemas(self):
        try:
            output = subprocess.check_output(
                "/opt/vertica/bin/vsql -CAtX -c 'SELECT schema_name FROM schemata WHERE NOT is_system_schema ORDER BY schema_name;'",
                shell=True
            )
            schemas = output.decode('ascii').splitlines()
            return schemas
        except Exception as e:
            print(str(e))

    def write_config_file(self, includeObjects, excludeObjects):
        config = self.generate_ini(includeObjects, excludeObjects)
        with open(self.config_file, 'w') as configfile:
            config.write(configfile)

    def generate_ini(self, includeObjects, excludeObjects):
        config = configparser.ConfigParser()
        config.optionxform = str
        config.read(self.config_file)
        config['Misc'].pop('objects', None)
        config['Misc']['includeObjects'] = includeObjects
        config['Misc']['excludeObjects'] = excludeObjects
        return config

    def execute_replicate_task(self):
        return os.system('/opt/vertica/bin/vbr -t replicate -c ' + self.config_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Replicate full database",
        epilog=(
            'For example config files search in /opt/vertica/share/vbr/example_configs/ '
            'or view the documentation at https://www.vertica.com/docs/latest/HTML/index.htm#Authoring/AdministratorsGuide/BackupRestore/SampleConfigFiles/SampleIniFiles.htm\n'
        )
    )
    parser.add_argument('-c', '--config-file', metavar='', required=True, help='Specify config file (e.g. replicate_db.ini)')
    parser.add_argument('-a', '--all-schemas', action='store_true', help='Starts with all schemas for the includeObjects parameter, then applies the explicit include and exclude objects. When this option is specified there is no reason to specify a schema with the --include option; however, an individual table can be listed if that table is in a schema specified with the --exclude option (e.g. if the store schema is excluded, but the store.store_sales_fact table is included)')
    parser.add_argument('-i', '--include', metavar='', help='Optional (if --all-schemas is used): Objects to include (existing values in the config file will be overridden)')
    parser.add_argument('-x', '--exclude', metavar='', help='Optional: Objects to exclude (existing values in the config file will be overridden)')
    args = parser.parse_args()

    start_with_all_schemas = args.all_schemas
    config_file = args.config_file
    include = args.include
    exclude = args.exclude if args.exclude else ''

    if not start_with_all_schemas and not include:
        print("Must specify all schemas with --all-schemas or specify individual objects with --include")
        parser.print_help()
        sys.exit(0)

    dbr = DBReplicator(config_file, start_with_all_schemas, include, exclude)
    dbr.main()
