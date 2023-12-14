import pymysql
import pymysql.cursors


class DatabaseManager:
    def __init__(self, conf):
        # Connect to the database
        host, user, password, database = conf.get_database_properties()
        self._connection = pymysql.connect(host=host,
                                           user=user,
                                           password=password,
                                           database=database,
                                           cursorclass=pymysql.cursors.DictCursor)

    def sql_run_fetch_command(self, sql_command, vals=None, fetch_all=False, close_db=False):
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

            if close_db:
                self._connection.close()

            return result

    def insert_publications_info(self, publications_info_list):
        start_time = time.time()
        # insert
        sql_command = 'INSERT INTO trips (trip_id, taxi_id, start_year, start_month, start_day, start_hour, nb_points) VALUES (%s, %s, %s, %s, %s, %s, %s)'
        sql_run_command(sql_command, vals=trips_info, modify_db=True, fetch_all=False, close_db=False)
        trips_info = []

        end_time = time.time()
        print(f"Time it took: {round(end_time - start_time, 3)}")

    def insert_topic_if_needed(self, topic_subject):
        """
        This method verifies if topic already exists. If not, updates the table.
        :param topic_subject:
        :return None:
        """
        sql_command = 'SELECT id, subject from topics where subject=%s'
        result = self.sql_run_fetch_command(sql_command, vals=topic_subject)
        if result is None:
            # The topic does not exist in the topic table, needs to insert it
            sql_command = 'SELECT max(id) from topics'
            result = self.sql_run_fetch_command(sql_command)
            if result is None:
                topic_id = 0
            else:
                topic_id = int(result['max(id)'])
                topic_id += 1
            sql_command = 'insert into topics (id, subject) values (%s, %s)'
            with self._connection.cursor() as cursor:
                cursor.execute(sql_command, (topic_id, topic_subject))
                self._connection.commit()
        print(result)


def main():
    import Configuration
    c = Configuration.Configuration()

    m = DatabaseManager(c)

    sql_command = 'SELECT * from topics'
    result = m.sql_run_fetch_command(sql_command, fetch_all=True)
    print(f"before inserting: {result}")

    m.insert_topic_if_needed("try topic3")
    sql_command = 'SELECT * from topics'
    result = m.sql_run_fetch_command(sql_command, fetch_all=True)
    print(f"after inserting: {result}")

    m.insert_topic_if_needed("try topic3")
    result = m.sql_run_fetch_command(sql_command, fetch_all=True)
    print(f"after inserting again: {result}")


if __name__ == "__main__":
    main()
