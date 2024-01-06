import json


class Configuration:
    def __init__(self):
        file_path = "config.json"
        self._file_path = file_path
        self._config = self._load_config()

    def _load_config(self):
        with open(self._file_path, 'r') as file:
            return json.load(file)

    def get_user_credentials(self):
        return self._config['UserCredentials']['login'], self._config['UserCredentials']['password']

    def get_database_properties(self):
        return (self._config['DatabaseProperties']['host'], self._config['DatabaseProperties']['user'],
                self._config['DatabaseProperties']['password'], self._config['DatabaseProperties']['database'])

    def get_headers(self):
        return self._config["Headers"]

    def get_parse_tag(self, part):
        return self._config["Tags"][part]
