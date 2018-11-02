import os
import argparse
import configparser

class DBReplicator:

    def __init__(self):
        pass

    def main(self):
        objects = ['public', 'store', 'online_sales']
        for object in objects:
            self.write_config_file(object)
            # status_code = self.execute_replicate_task()

    def write_config_file(self, object):
        config = self.generate_ini(object)
        with open('db_replicate.ini', 'w') as configfile:
            config.write(configfile)

    def generate_ini(self, object):
        config = configparser.ConfigParser()
        config.optionxform = str
        config.read('db_replicate.ini')
        misc = config['Misc']['objects']
        config['Misc']['objects'] = object
        misc = config['Misc']['objects']
        return config

    def execute_replicate_task(self):
        return os.system('/opt/vertica/bin/vbr -t replicate -c db_replicate.ini')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Replicate full database")

    dbr = DBReplicator()
    dbr.main()
