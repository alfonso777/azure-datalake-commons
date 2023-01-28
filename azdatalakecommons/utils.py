import configparser

def load_config(path_file = 'conf/app_test.conf', use_row_parser = False):
    path_file = path_file if path_file else 'conf/app_test.conf'
    if use_row_parser:
        config = configparser.RawConfigParser()
    else:
        config = configparser.ConfigParser()
    config.read(path_file)
    return config
