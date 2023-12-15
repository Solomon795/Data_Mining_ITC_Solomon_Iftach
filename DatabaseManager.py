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

    def _sql_run_execute_many(self, sql_command, vals=None):  #,special_exception_handling=True):
        try:
            with self._connection.cursor() as cursor:
                cursor.executemany(sql_command, vals)
                self._connection.commit()
        except pymysql.err.IntegrityError as e:
            # In case of key error due to an already existing row we switch to execution of row by row
            print(f"_sql_run_execute_many with command:{sql_command}\nfailed with {e} on vals:\n{vals}")
            print(f"Due to already existed row switching to single row handling")
            # if special_exception_handling:
            #     for single_val in vals:
            #         print(f'single_val: {single_val}')
            #         self._sql_run_execute(sql_command, single_val)
        except Exception as e:
            print(f"_sql_run_execute_many with command:{sql_command}\nfailed with {e} on vals:\n{vals}")
            print(f"_sql_run_execute_many with command:continue running")
            return


    def insert_publications_info(self, publications_info_list):
        """
        This function inserts the publications_info_list into the following tables in the
        following order:
        1. publication_types
        2. publications
        3. publications_by_topics
        4. authors
        5. publications_by_authors
        Commit is done only after all insertions.
        :param publications_info_list:
        :return None:
        """
        # {"publication_type": publication_type, "title": title, "site": site, "journal": journal, "id": pub_id,
        #  "authors": authors, "month - year": monthyear, "reads": reads,
        #  "citations": citations})
        vals = []
        for dict in publications_info_list:
            pub_type = dict['publication_type']
            title = dict['title']
            site = dict["site"]
            journal = dict['journal']
            pub_id = dict['id']
            authors = dict['authors']
            year = dict['year']
            reads = dict['reads']
            citations = dict['citations']

            #  1. insertion to publication_types table
            type_code = self._insert_publication_type_if_needed(pub_type)
            vals.append((pub_id, type_code, title, year, citations, reads, site, journal))

            #  2. insertion to publications
            sql_command = ('insert into publications (id, pub_type_code, title, year, num_citations, num_reads, url, '
                           'journal) values (%s, %s, %s, %s, %s, %s, %s, %s)')

        self._sql_run_execute_many(sql_command, vals=vals, special_exception_handling = True)
        print (f"vals:{vals}")
        #self._sql_run_execute(sql_command,
        #                      vals=(pub_id, type_code, title, year, citations, reads, site, journal))

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
            max_val = result['max(id)'] # The table is still empty
            if max_val is None:
                topic_id = 0
            else:
                topic_id = int(max_val) + 1
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
