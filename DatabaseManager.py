import pymysql
import pymysql.cursors
import json


class DatabaseManager:
    def __init__(self, conf, topic_subject, db_source):
        # Connect to the database
        host, user, password, database = conf.get_database_properties()
        self._connection = pymysql.connect(host=host,
                                           user=user,
                                           password=password,
                                           database=database,
                                           cursorclass=pymysql.cursors.DictCursor)
        # maximal id  which increases per publication insertion
        self._max_pub_id = self._extract_max_id('publications')
        self._topic_id = self._insert_topic_if_needed(topic_subject)
        # publications_types is a dictionary {type_name: type_code}
        self._publications_types = self._get_publications_types()

        self._max_pub_topic_id = self._extract_max_id('publications_by_topics')
        self.db_source = db_source  # 0 - ResearchGate; 1 - Pubmed

        self._countries_codes = self._get_countries_codes()
    def _extract_max_id(self, table_name):
        """
        This function returns the maximal id. Currently used only for the topics table
        :param table_name:
        :return max_val: int
        """
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
        """"
        This method actually runs the sql_command.
        """
        with self._connection.cursor() as cursor:
            cursor.execute(sql_command, vals)
            self._connection.commit()

    def _sql_run_execute_many(self, sql_command, vals=None):
        """
        This methods executes sql_command on many rows.
        :param sql_command:
        :param vals:
        :return:
        """
        try:
            with self._connection.cursor() as cursor:
                cursor.executemany(sql_command, vals)
                self._connection.commit()
        except Exception as e:
            print(f"_sql_run_execute_many with command:{sql_command}\nfailed with {e} on vals:\n{vals}")
            print(f"_sql_run_execute_many with command:continue running")
            return

    def insert_publications_info(self, db_source, publications_info_list, countries_list=None):
        """
        This function inserts the publications_info_list into the following tables in the
        following order:
        1. publication_types
        2. publications
        3. publications_by_topics
        4. authors
        5. publications_by_authors
        Commit is done only after all insertions.
        :param db_source: 0 - ResearchGate; 1 - Pubmed
        :param publications_info_list:
        :return None:
        """
        # {"publication_type": publication_type, "title": title, "site": site, "journal": journal, "id": rgate_id,
        #  "authors": authors, "month - year": monthyear, "reads": reads,
        #  "citations": citations})
        pubs_vals_dict = {}  # dictionary of rgate_id to check for already existing articles
        pubs_ids = []
        titles = set()
        pubs_for_topic = set()  # for updating publications_by_topics table
        authors_per_pub = {}  # for updating publications_by_authors table
        authors_per_batch = set()  # for updating authors table
        for dict in publications_info_list:
            # Step A: making a batch of dictionaries to compare with datamining DB.
            #         where the primary 'pub_id' is set consecutively to _max_pub_id + 1
            # pub_id in the batch formation will set to 'id' in the publications table.
            self._max_pub_id += 1 #Setting the primary key - which is a technical index
            pub_id = self._max_pub_id
            # Initializing and setting fields that exist only in one of the DBs (ResearcgGate or Pubmed).
            doi = None
            pubmed_id = None
            rgate_id = None
            site = None
            if db_source == 1:  # Pubmed
                doi = dict['doi']
                pubmed_id = dict['pubmed_id']
            else:  # db_source == 0 corresponding to ResearchGate
                rgate_id = dict['id']
                site = dict["site"]

            # Fields common to both DBs
            pub_type = dict['publication_type']
            title = dict['title']
            journal = dict['journal']
            authors = dict['authors']
            year = dict['year']
            reads = dict['reads']
            citations = dict['citations']

            # Preparing batches to insert into tables
            authors_per_batch.update(set(authors))  # for authors table
            authors_per_pub.update({pub_id: authors})  # for updating publications_by_authors table
            pubs_for_topic.update({pub_id})  # for updating publications_by_topics table

            #  Check if to insert publication_type
            type_code = self._insert_publication_type_if_needed(pub_type)

            # preparing a batch for updating publications table
            if db_source == 0: # Researchgate
                pubs_vals_dict.update(
                    {rgate_id: (pub_id, rgate_id, type_code, title, year, citations, reads, site, journal)})
            else:  # db_source == 1 ==> Pubmed
                pubs_vals_dict.update(
                    {pubmed_id: (
                    pub_id, pubmed_id, doi, type_code, title, year, citations, reads, site, journal)})

            # pubs_ids += [str(pub_id)]  # was used when pub_id was PK
            titles.update({title})

        ##### 2 + 3. Removing existing rows from publications ####
        # Making a string of pub_ids
        # separator = ','
        # pubs_ids_str = separator.join(pubs_ids)  # was used when pub_id was PK
        titles_str = '"' + '","'.join(titles) + '"'

        # Selecting existing publications and removing them from pubs_vals_dict
        if db_source == 0:
            sql_command = \
                f'select id, rgate_id as comp_key, citations, reads from publications where title in ({titles_str})'
        else:
            sql_command = \
                f'select id, pubmed_id as comp_key, citations, reads from publications where title in ({titles_str})'
        # Example for result structure: [{'id': 1, 'comp_key'=567 ...,'reads': 666}, {'id': 2, ..., 'reads': 517}]
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        vals_to_update = []
        rgate_id_pos = 1
        pubmed_id_pos = 2
        doi_pos = 3
        num_citation_pos = 7  # position in the tuple
        num_reads_pos = 8  # position in the tupl
        for dict_id in result:
            id_to_remove = str(dict_id['comp_key'])
            # removal of all publications for the insertion to publications table
            element_to_remove = pubs_vals_dict.pop(id_to_remove)
            # removal of all publications for the insertion to publications_by_authors table
            authors_per_pub.pop(id_to_remove)
            # Building an update list of num_reads and num_citations for existing publications
            # The actual list building
            if db_source == 0:  # ResearchGate
                vals_to_update += [(element_to_remove[rgate_id_pos], element_to_remove[num_citation_pos],
                                    element_to_remove[num_reads_pos], id_to_remove)]
            else:
                vals_to_update += [(element_to_remove[pubmed_id_pos], element_to_remove[doi_pos],
                                    element_to_remove[num_citation_pos], element_to_remove[num_reads_pos],
                                    id_to_remove)]


        if db_source == 0: # ResearchGate
            sql_command = 'update publications set rgate_id_pos = %s, num_citations = %s, num_reads = %s where id = %s'
        else:              # Pubmed
            sql_command = \
                'update publications set pubmed_id_pos = %s, doi_pos = %s, num_citations = %s, num_reads = %s where id = %s'
        self._sql_run_execute_many(sql_command, vals=vals_to_update)

        #{id: (id, rgate_id, pubmed_id, doi, type_code, title, year, citations, reads, site, journal)})
        #  2. insertion to publications the verified new rows
        sql_command = \
            ('insert into publications '
            '(id, rgate_id, pubmed_id, doi, pub_type_code, title, year, num_citations, num_reads, url, journal) '
            'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
        pubs_vals = pubs_vals_dict.values()
        self._sql_run_execute_many(sql_command, vals=pubs_vals)

        #  3. Insertion of rows {id, topic_id, pub_id} to publications_by_topics ########
        # Existing rows in publications_by_topics removed from pubs_for_topic
        sql_command = (f"select pub_id from publications_by_topics "
                       f"where topic_id=({self._topic_id}) and pub_id in ({pubs_ids_str})")
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        for pub_dict in result:  # [{'pub_id': 32441}, {'pub_id': 25646}]
            pub_id_to_remove = str(pub_dict['pub_id'])  # pub_id from the table
            pubs_for_topic.remove(pub_id_to_remove)
        # Insertion of remaining rows to publications_by_topics table
        pubs_for_topics_vals = []
        for pub_id in pubs_for_topic:
            self._max_pub_topic_id += 1
            pubs_for_topics_vals += [(s nelf._max_pub_topic_id, self._topic_id, pub_id)]

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
        :param topic_subject: str
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

    def _get_countries_codes(self):
        """
        This function returns a dictionary of countries and codes:
        {country_name: country_code}
        :return local_dict: dictionary of countries names and codes
        """
        sql_command = 'SELECT country_name, country_code from countries_codes'
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        local_dict = {}
        if result is not None:
            for elem in result:
                local_dict.update({elem['country_name']: elem['country_code']})
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

    def _insert_country_code_if_needed(self, country_name):
        """
        This method returns the country_code for the country_name:
        if the country_name already exists it returns its code from the local dictionary,
        else it updates both the table countries_codes and the local dictionary.
        :param country_name: str
        :return country_code: int
        """
        try:  # If country_name already in table
            country_code = self._publications_types[country_name]
            return country_code
        except KeyError:  # country_name is not in the table
            country_code = len(self._countries_codes) + 1
            # Updating countries_codes table
            sql_command = 'insert into countries_codes (country_code, country_name) values (%s, %s)'
            self._sql_run_execute(sql_command, vals=(country_code, country_name))
            # Updating the local dictionary
            self._countries_codes.update({country_name: country_code})
            return country_code

    def _insert_publications_countries(self, ids_countries_dict):
        """
        This method insert values into publications_countries table.
        :param ids_countries_dict: [{pubmed_id: [country_name1, country_name2]}]
        :return None:
        """
        pub_countries_list = []
        sql_command = 'SELECT id, pubmed_id from publications where pubmed_id=%s'
        # example of result would be: [{'id':400, 'pubmed_id': 32441}, {'id':401, 'pubmed_id': 55442}]
        result = self._sql_run_fetch_command(sql_command, vals=ids_countries_dict.keys())
        for elem in result:
            pubmed_id = elem['pubmed_id']
            for countries_per_pubmed_id in ids_countries_dict[pubmed_id]:
                for country_name in countries_per_pubmed_id:
                    country_code = self._insert_country_code_if_needed(country_name)
                    pub_countries_list += [(elem["id"] , country_code)]

        sql_command = 'insert into publications_countries (pub_id, country_code) values (%s, %s)'
        self._sql_run_execute_many(sql_command, vals=pub_countries_list)










#
# INSERT INTO your_table (unique_column, other_column)
# VALUES (value, other_value)
# ON DUPLICATE KEY UPDATE other_column = VALUES(other_column);
