import pymysql
import pymysql.cursors


class DatabaseManager:
    def __init__(self, conf, topic_subject):
        # Connect to the database
        host, user, password, database = conf.get_database_properties()
        self._connection = pymysql.connect(host=host,
                                           user=user,
                                           password=password,
                                           database=database,
                                           cursorclass=pymysql.cursors.DictCursor)

        self._topic_id = self._insert_topic_if_needed(topic_subject)
        # publications_types is a dictionary {type_name: type_code}
        self._publications_types = self._get_publications_types()

    def _sql_run_fetch_command(self, sql_command, vals=None, fetch_all=False):
        """
        This function runs an SQL command by the chosen mode: modification, fetching, or closing the database.
        Modification includes for example the insertion of values into the SQL table.
        """
        with self._connection.cursor() as cursor:
            if fetch_all:
                cursor.execute(sql_command, vals)
                result = cursor.fetchall()
            else:
                cursor.execute(sql_command, vals)
                result = cursor.fetchone()

            return result

    def _sql_run_execute(self, sql_command, vals=None):
        with self._connection.cursor() as cursor:
            cursor.execute(sql_command, vals)
            self._connection.commit()

    def insert_publications_info(self, publications_info_list):
        # start_time = time.time()
        # # insert
        # sql_command = 'INSERT INTO trips (trip_id, taxi_id, start_year, start_month, start_day, start_hour, nb_points) VALUES (%s, %s, %s, %s, %s, %s, %s)'
        # sql_run_command(sql_command, vals=trips_info, modify_db=True, fetch_all=False, close_db=False)
        # trips_info = []
        #
        # end_time = time.time()
        # print(f"Time it took: {round(end_time - start_time, 3)}")
        self._insert_publication_type_if_needed(publication_type)
        return None

    def _insert_topic_if_needed(self, topic_subject):
        """
        This method verifies if topic already exists. If not, updates the table.
        :param topic_subject:
        :return topic_id: int - topic_id of the subject search
        """
        sql_command = 'SELECT id, subject from topics where subject=%s'
        result = self._sql_run_fetch_command(sql_command, vals=topic_subject)
        topic_id = -1
        if result is None:
            # The topic does not exist in the topic table, needs to insert it
            sql_command = 'SELECT max(id) from topics'
            result = self._sql_run_fetch_command(sql_command)
            if result is None:
                topic_id = 0
            else:
                topic_id = int(result['max(id)']) + 1
            sql_command = 'insert into topics (id, subject) values (%s, %s)'
            self._sql_run_execute(sql_command, vals=(topic_id, topic_subject))
        return topic_id

    def _get_publications_types(self):
        """
        This function returns a dictionary of publications types and codes:
        {publication_type: publication_code}
        :return result:
        """
        sql_command = 'SELECT type_name, type_code from publications_types'
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        local_dict = {}
        if result is not None:
            for elem in result:
                local_dict.update({elem['type_name']: elem['type_code']})
        return local_dict

    def _insert_publication_type_if_needed(self, publication_type):
        """
        This method returns the code for the publication_type:
        if the publication type already exists it returns its code from the local dictionary,
        else it updates both the table publications_types and the local dictionary.
        :param publication_type:
        :return type code: int type code for the publication_type
        """
        try:  # If publication_type already in table
            type_code = self._publications_types[publication_type]
            return type_code
        except KeyError:  # publication_type is not in the table
            type_code = len(self._publications_types) + 1
            # Updating publications_types table
            sql_command = 'insert into publications_types (type_code, type_name) values (%s, %s)'
            self._sql_run_execute(sql_command, vals=(type_code, publication_type))
            # Updating the local dictionary
            self._publications_types.update({publication_type: type_code})
            return type_code


def main():
    import Configuration
    c = Configuration.Configuration()

    m = DatabaseManager(c, 'energy market')
    type_code = m._insert_publication_type_if_needed('Article')
    print(f"type_code of Article:{type_code}")
    type_code = m._insert_publication_type_if_needed('Article')
    print(f"type_code of Article:{type_code}")
    type_code = m._insert_publication_type_if_needed('Poster')
    print(f"type_code of Poster:{type_code}")


if __name__ == "__main__":
    main()
