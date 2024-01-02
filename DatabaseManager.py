import pymysql
import pymysql.cursors
import json
import logging
import time

# Initialize the logger according to the module name
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, conf, topic_subject):
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
            logger.error(f"_sql_run_execute_many failed on command:{sql_command}\n exception:{e}\n vals:{vals}\n")
            logger.error(f"_sql_run_execute_many continues running")
            return

    def insert_publications_info(self, db_source, publications_info_list):
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
        s_time = time.time()
        pubs_vals_dict = {}  # dictionary of rgate_id to check for already existing articles
        dois = set()
        pubs_for_topic = {}  # for updating publications_by_topics table
        authors_per_pub = {}  # for updating publications_by_authors table
        authors_per_batch = set()  # for updating authors table
        countries_per_batch = set()  # for countries_codes table
        countries_per_pubs = {}  # for countries_publications table
        for dict in publications_info_list:
            # Making a batch of dictionaries to compare with datamining DB.
            # where the primary 'pub_id' is set consecutively to _max_pub_id + 1
            # Initializing and setting fields that exist only in one of the DBs (ResearcgGate or Pubmed).
            serial_id = self._max_pub_id + 1
            self._max_pub_id += 1
            site = None
            rgate_id = None
            pubmed_id = None
            doi = dict['doi'][0:45].upper()

            # Handle of fields specific to one DB only
            if db_source == 1:  # Pubmed
                pubmed_id = dict['pubmed_id']
            else:  # db_source == 0 corresponding to ResearchGate
                rgate_id = dict['id']
                site = dict["site"][0:250]
            # Fields common to both DBs
            pub_type = dict['publication_type']

            title = dict['title'].title()[0:250]
            if db_source == 1 and title[-1] == '.':
                title = title[:-1]  # removing the terminating dot in pubmed title
                
            journal = dict['journal'][0:150]
            authors = map(lambda x: x[0:45].title(), dict['authors'])
            countries = set()
            if db_source == 1:  # Pubmed
                countries.update(dict['countries'])
            year = dict['year']
            if dict['reads'] is None:
                reads = 0
            else:
                reads = dict['reads']
            if dict['citations'] is None:
                citations = 0
            else:
                citations = dict['citations']

            # Preparing batches to insert into tables
            authors_per_batch.update(set(authors))  # for authors table
            authors_per_pub.update({doi: [serial_id, authors]})  # for updating publications_by_authors table
            pubs_for_topic.update({doi: serial_id})  # for updating publications_by_topics table
            countries_per_batch.update(countries)  # for countries_codes table
            countries_per_pubs.update({doi: [serial_id, countries]})  # for publications_countries table

            #  Check if to insert publication_type
            type_code = self._insert_publication_type_if_needed(pub_type)

            # Preparing a batch for inserting into publications table
            pubs_vals_dict.update(
                {doi: (serial_id, rgate_id, pubmed_id, type_code, title, year, citations, reads, site, journal, doi)})
            # adding another doi to the dois batch
            dois.update({doi})

        ##### 2 + 3. Removing existing rows that already appear in publications table ####
        dois_str = '"' + '","'.join(dois) + '"'

        # Selecting existing publications and removing them from pubs_vals_dict
        if db_source == 0:  # Researchgate
            sql_command = \
                f'select id, rgate_id, doi, num_citations, num_reads from publications where doi in ({dois_str})'
        else:  # Pubmed
            sql_command = \
                f'select id, pubmed_id, doi, num_citations, num_reads from publications where doi in ({dois_str})'
        # Example for result structure (list of dictionaries): [{'id': 1, 'pubmed_id'=567 ...,}, ..., {...}]
        result = self._sql_run_fetch_command(sql_command, fetch_all=True)
        vals_to_update = []  # Update of fields for already existing publications
        #  id's from publications table that will be used for the select from publications_by_topics
        existing_pubs_ids = []
        rgate_id_pos = 1  # position in the tuple
        pubmed_id_pos = 2
        num_citation_pos = 6  # position in the tuple
        num_reads_pos = 7  # position in the tupl
        for dict_id in result:
            existing_pubs_ids += [str(dict_id['id'])]  # list of publications id's in str format
            doi_to_remove = dict_id['doi']
            # removal of all publications for the insertion to publications table
            element_to_remove = pubs_vals_dict.pop(doi_to_remove)
            # removal of all publications for the insertion to publications_by_authors table
            authors_per_pub.pop(doi_to_remove)
            # Removing existing publications from publication_by_topics batch.
            pubs_for_topic.pop(dict_id['doi'])  # pubs_for_topic will contain only new publications.
            # Removing existing publications from countries_per_pub
            countries_per_pubs.pop(dict_id['doi'])  # countries_per_pub will have countries only for new publications.

            # Building an update list of relevant fields for existing publications, e.g. num_citations.
            # The actual list building
            if db_source == 0:  # ResearchGate
                vals_to_update += [(element_to_remove[rgate_id_pos],
                                    element_to_remove[num_citation_pos], element_to_remove[num_reads_pos],
                                    doi_to_remove)]
            else:
                vals_to_update += [(element_to_remove[pubmed_id_pos],
                                    element_to_remove[num_citation_pos], element_to_remove[num_reads_pos],
                                    doi_to_remove)]

        if db_source == 0:  # ResearchGate
            sql_command = \
                ('update publications set rgate_id = %s, num_citations = %s, num_reads = %s where doi = %s')
        else:  # Pubmed
            sql_command = \
                ('update publications set pubmed_id = %s, num_citations = %s, num_reads = %s where doi = %s')
        self._sql_run_execute_many(sql_command, vals=vals_to_update)

        #  2. Insertion to publications table the verified new rows
        sql_command = \
            ('insert into publications '
             '(id, rgate_id, pubmed_id, pub_type_code, title, year, num_citations, num_reads, url, journal, doi) '
             'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
        pubs_vals = pubs_vals_dict.values()
        self._sql_run_execute_many(sql_command, vals=pubs_vals)

        #  3. Insertion of rows {id, topic_id, pub_id} to publications_by_topics ########
        # Existing rows in publications_by_topics removed from pubs_for_topic
        # Preparing all the existing id's in one string separated by commas for the SQL command
        existing_pubs_ids_str = ",".join(existing_pubs_ids)  # one string of id's
        if existing_pubs_ids_str != "":
            sql_command = (f"select pub_id from publications_by_topics "
                           f"where topic_id=({self._topic_id}) and pub_id in ({existing_pubs_ids_str})")
            result = self._sql_run_fetch_command(sql_command, fetch_all=True)
            set_existing_ids = set(existing_pubs_ids)  # cast to set format for removal of serials_id's'
            for pub_dict in result:  # [{'pub_id': 32441}, {'pub_id': 25646}]
                serial_id_to_remove = str(pub_dict['pub_id'])  # pub_id from the table to be removed from pubs_for_topic
                set_existing_ids.remove(serial_id_to_remove)
            # Insertion of remaining rows to publications_by_topics table
            ids_to_insert = list(pubs_for_topic.values()) + list(
                set_existing_ids)  # now pubs_for_topic contain all publications to be inserted
            pubs_for_topics_vals = []
            for pub_id in ids_to_insert:
                self._max_pub_topic_id += 1
                pubs_for_topics_vals += [(self._max_pub_topic_id, self._topic_id, pub_id)]

            sql_command = 'insert into publications_by_topics (id, topic_id, pub_id) values (%s, %s, %s)'
            self._sql_run_execute_many(sql_command, vals=pubs_for_topics_vals)

        # 4. Insert set authors_per_batch to authors table
        # cast the authors to a string for the removal selection
        authors_str = '\",\"'.join(authors_per_batch)
        if authors_str != "":
            authors_str = f'\"{authors_str}\"'

        # Extract the authors to remove from authors table
        authors_to_insert = authors_per_batch
        sql_command = f'select full_name from authors where full_name in ({authors_str})'
        #print(f"aut:len{len(sql_command)}, sel:{sql_command}")
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

        # 5. Insertion to publications_by_authors  from dict: {doi: [serial_id, [auther_fullname1, ...]}
        # Generating pairs candidates to insert to the table
        pub_aut_id_pairs = []
        for pub_id_and_authors in authors_per_pub.values():
            pub_id = pub_id_and_authors[0]
            authors_full_names = pub_id_and_authors[1]
            for full_name in authors_full_names:
                pub_aut_id_pairs += [(dict_aut_id[full_name], pub_id)]

        sql_command = 'insert into publications_by_authors (author_id, pub_id) values (%s, %s)'
        self._sql_run_execute_many(sql_command, vals=pub_aut_id_pairs)

        # 6. Insertion to countries_codes
        countries_codes_dict = {}
        for country_name in countries_per_batch:
            country_code = self._insert_country_code_if_needed(country_name)
            countries_codes_dict[country_name] = country_code

        # 7. publications_countries
        country_codes_pub_ids_pairs = []
        for pub_id_and_countries in countries_per_pubs.values():
            pub_id = pub_id_and_countries[0]
            countries_names = pub_id_and_countries[1]
            for country_name in countries_names:
                country_codes_pub_ids_pairs += [(countries_codes_dict[country_name], pub_id)]
        sql_command = 'insert into publications_countries(country_code, pub_id) values (%s, %s)'
        self._sql_run_execute_many(sql_command, vals=country_codes_pub_ids_pairs)
        e_time = time.time()
        logger.info(
            f"Time it took to insert {len(pubs_vals_dict)} and update {len(vals_to_update)} publications into DB: {round(e_time - s_time, 1)}")

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
            country_code = self._countries_codes[country_name]
            return country_code
        except KeyError:  # country_name is not in the table
            country_code = len(self._countries_codes) + 1
            # Updating countries_codes table
            sql_command = 'insert into countries_codes (country_code, country_name) values (%s, %s)'
            self._sql_run_execute(sql_command, vals=(country_code, country_name))
            # Updating the local dictionary
            self._countries_codes.update({country_name: country_code})
            return country_code
