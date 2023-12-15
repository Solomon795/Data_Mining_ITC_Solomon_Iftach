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

    def get_topic_settings(self):
        return self._config['Topic']['name'], self._config['Topic']['num-pages-to-process']

    def get_database_properties(self):
        return (self._config['DatabaseProperties']['host'], self._config['DatabaseProperties']['user'],
                self._config['DatabaseProperties']['password'], self._config['DatabaseProperties']['database'])


def main():
    c = Configuration();

    user, password = c.get_user_credentials()
    topic_name, num_pages_to_process = c.get_topic_settings()
    print(f"username: {user}\npassword: {password}\ntopic:{topic_name}\nnum pages:{num_pages_to_process}")


if __name__ == "__main__":
    main()
