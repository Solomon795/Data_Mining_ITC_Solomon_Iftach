import pymysql
import pymysql.cursors


class DatabaseManager:
    def __init__(self, conf):
        # Connect to the database
        host, user, password, database = conf.get_database_properties()
        self._connection = pymysql.connect(host, user, password, database, cursorclass=pymysql.cursors.DictCursor)

    def sql_run_command(sql_command, connection, vals=None, modify_db=False, fetch_all=False, close_db=False):
        """
        This function runs an SQL command by the chosen mode: modification, fetching, or closing the database.
        Modification includes for example the insertion of values into the SQL table.
        """
        with connection.cursor() as cursor:
            if modify_db:
                if vals is not None:
                    cursor.executemany(sql_command, vals)
                else:
                    cursor.execute(sql_command)
                connection.commit()
                result = None
            elif fetch_all:
                cursor.execute(sql_command)
                result = cursor.fetchall()
            else:
                cursor.execute(sql_command)
                result = cursor.fetchone()

            if close_db:
                connection.close()

            return result

    def insert_publications_info(self, publications_info_list):
        start_time = time.time()
        # insert
        sql_command = 'INSERT INTO trips (trip_id, taxi_id, start_year, start_month, start_day, start_hour, nb_points) VALUES (%s, %s, %s, %s, %s, %s, %s)'
        sql_run_command(sql_command, connection, vals=trips_info, modify_db=True, fetch_all=False, close_db=False)
        trips_info = []

        end_time = time.time()
        print(f"Time it took: {round(end_time-start_time, 3)}")

    def insert_topic_if_needed(self, topic):
        return