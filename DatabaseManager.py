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

        self._max_pub_topic_id = self._extract_max_id('publications_by_topics')

    def _extract_max_id(self, table_name):
        sql_command = f"select max(id) from {table_name}"
        result = self._sql_run_fetch_command(sql_command)
        max_val = result['max(id)']
        if max_val is None:
            return 0
        else:
            return int(max_val)

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

    def _sql_run_execute_many(self, sql_command, vals=None):
        try:
            with self._connection.cursor() as cursor:
                cursor.executemany(sql_command, vals)
                self._connection.commit()
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
        pubs_vals_dict = {}  # dictionary of pub_id to check for already existing articles
        pubs_ids = []
        pubs_for_topic = set()
        authors_per_pub = {}
        authors_per_batch = set()
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
            authors_per_batch.update(set(authors))
            authors_per_pub.update({pub_id: authors})
            pubs_for_topic.update({pub_id})
            #  Check if to insert publication_type
            type_code = self._insert_publication_type_if_needed(pub_type)
            pubs_vals_dict.update({pub_id: (pub_id, type_code, title, year, citations, reads, site, journal)})
            # pubs_topics_dict.update({pub_id: (topic_id, pub_id)})
            pubs_ids += [str(pub_id)]

        ##### 2 + 3. Removing existing rows from publications ####
        # Making a string of pub_ids
        separator = ','
        pubs_ids_str = separator.join(pubs_ids)

        # Selecting existing publications and removing them from pubs_vals_dict
        sql_command = f"select id from publications where id in ({pubs_ids_str})"
        # Example for result structure: [{'id': 1}, {'id': 2}]
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        vals_to_update = []
        for dict_id in result:
            id_to_remove = dict_id['id']
            # removal of all publications for the insertion to publications table
            element_to_remove = pubs_vals_dict.pop(id_to_remove)
            # removal of all publications for the insertion to publications_by_authors table
            authors_per_pub.pop(id_to_remove)
            # Building an update list of num_reads and num_citations for existing publications
            num_citation_pos = 4  # position in the tuple
            num_reads_pos = 5  # position in the tuple
            # The actual list building
            vals_to_update += [(element_to_remove[num_citation_pos], element_to_remove[num_reads_pos], id_to_remove)]

        sql_command = 'update publications set num_citations = %s, num_reads = %s where id = %s'
        self._sql_run_execute_many(sql_command, vals=vals_to_update)

        #  2. insertion to publications the verified new rows
        sql_command = ('insert into publications (id, pub_type_code, title, year, num_citations, num_reads, url, '
                       'journal) values (%s, %s, %s, %s, %s, %s, %s, %s)')
        pubs_vals = pubs_vals_dict.values()
        self._sql_run_execute_many(sql_command, vals=pubs_vals)

        #  3. Insertion of rows {id, topic_id, pub_id} to publications_by_topics ########
        # Existing rows in publications_by_topics removed from pubs_for_topic
        sql_command = (f"select pub_id from publications_by_topics "
                       f"where topic_id=({self._topic_id}) and pub_id in ({pubs_ids_str})")
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        for pub_dict in result:  # [{'pub_id': 32441}, {'pub_id': 25646}]
            pub_id_to_remove = pub_dict['pub_id']  # pub_id from the table
            pubs_for_topic.remove(pub_id_to_remove)
        # Insertion of remaining rows to publications_by_topics table
        pubs_for_topics_vals = []
        for pub_id in pubs_for_topic:
            self._max_pub_topic_id += 1
            pubs_for_topics_vals += [(self._max_pub_topic_id, self._topic_id, pub_id)]

        sql_command = 'insert into publications_by_topics (id, topic_id, pub_id) values (%s, %s, %s)'
        self._sql_run_execute_many(sql_command, vals=pubs_for_topics_vals)

        # 4. Insert set authors_per_batch to authors table
        # cast the authors to a string for the removal selection
        list(authors_per_batch)
        authors_str = '\",\"'.join(authors_per_batch)
        authors_str = f'\"{authors_str}\"'

        # extract the authors to remove from authors table
        authors_to_insert = set(authors_per_batch)
        sql_command = f'select full_name from authors where full_name in ({authors_str})'
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        for dict_aut in result:
            aut_to_remove = dict_aut['full_name']
            authors_to_insert.remove(aut_to_remove)

        # Insertion of new authors to authors table
        sql_command = 'insert into authors (full_name) values (%s)'
        self._sql_run_execute_many(sql_command, vals=list(authors_to_insert))

        # extract the id and authors from the new batch that was added for publications_by_authors
        sql_command = f"select id, full_name from authors where full_name in ({authors_str})"
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        dict_aut_id = {}
        for dict_aut in result:
            dict_aut_id.update({dict_aut['full_name']: dict_aut['id']})

        # 5. Insertion to publications_by_authors
        # Generating pairs candidates to insert to the table
        pub_aut_pairs = []
        for pub_aut in authors_per_pub:
            for aut in authors_per_pub.get(pub_aut):
                pub_aut_pairs += [(pub_aut, dict_aut_id[aut])]

        sql_command = 'insert into publications_by_authors (pub_id, author_id) values (%s, %s)'
        self._sql_run_execute_many(sql_command, vals=pub_aut_pairs)

    def _insert_topic_if_needed(self, topic_subject):
        """
        This method verifies if topic already exists. If not, updates the table.
        :param topic_subject:
        :return topic_id: int - topic_id of the subject search
        """
        sql_command = 'SELECT id from topics where subject=%s'
        result = self._sql_run_fetch_command(sql_command, vals=topic_subject)
        topic_id = -1
        if result is None:
            # The topic does not exist in the topic table, needs to insert it
            topic_id = self._extract_max_id(table_name='topics') + 1
            sql_command = 'insert into topics (id, subject) values (%s, %s)'
            self._sql_run_execute(sql_command, vals=(topic_id, topic_subject))
        else:
            # Get the id of the topic from topics table
            topic_id = result['id']
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

    m = DatabaseManager(c, 'Energy Market2')

    print("######### BEFORE INSERTION")
    sql_command = "select * from topics"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    print(f"top:{result}")
    sql_command = "select * from publications"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub in result:
        print(f"pub:{pub}")
    sql_command = "select * from authors"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for aut in result:
        print(f"aut:{aut}")
    sql_command = "select * from publications_by_authors"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub_aut in result:
        print(f"pub_aut:{pub_aut}")
    sql_command = "select * from publications_by_topics"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub_top in result:
        print(f"pub_top:{pub_top}")

    print(f"topic id:{m._topic_id}")

    pubs = [{"publication_type": "Article", "title": "t5", "site": "s5", "journal": "j3", "id": 5,
             "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
            {"publication_type": "Article", "title": "t6", "site": "s6", "journal": "j4", "id": 6,
             "authors": ["imma", "abba"], "year": 2023, "reads": 3, "citations": 2}]
    m.insert_publications_info(pubs)

    print("######### AFTER INSERTION")
    sql_command = "select * from topics"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    print(f"top:{result}")
    sql_command = "select * from publications"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub in result:
        print(f"pub:{pub}")
    sql_command = "select * from authors"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for aut in result:
        print(f"aut:{aut}")
    sql_command = "select * from publications_by_authors"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub_aut in result:
        print(f"pub_aut:{pub_aut}")
    sql_command = "select * from publications_by_topics"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub_top in result:
        print(f"pub_top:{pub_top}")

    # sql_command = "select type_code from publications_types where type_code in (1,2,3, 4, 5)"
    # result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    # print(f"res1:{result}")


if __name__ == "__main__":
    main()
