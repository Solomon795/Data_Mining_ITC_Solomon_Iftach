import requests
import xml.etree.ElementTree as ET
import time
import logging
import pprint

# Initialize the logger according to the module name
logger = logging.getLogger(__name__)

class PubmedWrapper:
    """
    The role of this class is to fetch publications' information for given topic's subject.
    The topic's subject is passed as parameter for the __init__ method.
    The information is being fetched using 2 different methods:
    - fetch_pubs_info - returns all the information, aside from the authors' countries
    - fetch_countries - returns all the countries of all the publications according to the pubmed ids.

    Note: the pubmed ids, fetched during fetch_pubs_info, are stored as a class attribute, to be used in the
    fetch_countries (for ease of implementation).
    """

    def __init__(self, subject):
        """
        Initialization method.
        :param subject: query's topic
        """
        self._subject = subject
        self._pubmed_ids = "" # will be set in the fetch_pubs_info.

    def fetch_pubs_info(self, num_pubs_requested):
        """
        This function fetches info about publications, by using these services:
        - fetching the pubmed-id's
        - fetching the info (most attributes) by the pubmed-ids
        - fetching the countries by the pubmed-ids
        :param num_pubs_requested: int - number of publications to retrieve
        :return pubs_info: List of dictionaries
        """
        s_time = time.time()
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        summary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        params = {
            "db": "pubmed",  # database name
            "term": self._subject,  # subject of query
            "retmode": "json",  # format of response
            "retmax": num_pubs_requested,  # Number of results to retrieve
            'sort': 'pub+date'
        }

        # Step 1: Perform a search to get article IDs
        response = requests.get(base_url, params=params)
        pubs_info = []
        if response.status_code == 200:
            data = response.json()
            pubs_ids = data["esearchresult"]["idlist"]

            # The summary request gets a string of id's separated by commas
            # In addition, the pubmed_ids are saved as a class attribute to be an input
            # to the fetch_countries function (below).
            self._pubmed_ids = ",".join(pubs_ids)

            # Step 2: Fetch pubmed:summary information for multiple articles in a single request
            article_summary_params = {
                "db": "pubmed",
                "id": self._pubmed_ids,
                "retmode": "json"
            }

            summary_response = requests.get(summary_url, params=article_summary_params)

            if summary_response.status_code == 200:
                summary_data = summary_response.json()["result"]

                pubs_info = {}
                for pub_id in pubs_ids:
                    doi = ""
                    if len(summary_data[pub_id]["articleids"]) >= 2:
                        doi = summary_data[pub_id]["articleids"][1]["value"]
                    else:
                        doi = ''
                    article_info = {
                        "pubmed_id": pub_id,
                        "title": summary_data[pub_id]["title"],
                        #  author name format: 'Obama B H'
                        "authors": [author["name"] for author in summary_data[pub_id]["authors"]],
                        "year": summary_data[pub_id]["pubdate"].split(' ')[0],
                        "publication_type": ' '.join(summary_data[pub_id]["pubtype"]),
                        "journal": summary_data[pub_id]["source"],
                        "reads": summary_data[pub_id]["views"] if "views" in summary_data[pub_id] else None,
                        "citations": summary_data[pub_id]["citedby"] if "citedby" in summary_data[pub_id] else None,
                        "doi": doi
                    }
                    pubs_info[pub_id] = article_info
            else:
                logger.error("Failed to retrieve summary data.")
        else:
            logger.error("Failed to retrieve data from PubMed API.")

        e_time = time.time()
        logger.info (f"Fetching pubmed ids and summary took: {e_time-s_time}")

        #### Fetching countries ###
        s_time_2 = time.time()
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        params = {
            "db": "pubmed",
            "id": self._pubmed_ids,
            "retmode": "xml"  # the result is only in xml format
        }

        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            xml_data = response.text
            # Parse the XML response to extract the country information
            root = ET.fromstring(xml_data)

            # Using findall method to retrieve all values for the specified tag
            for pub in root.findall(".//PubmedArticle"):
                pmid_element = pub.find(".//PMID")
                pmid_str = pmid_element.text

                countries = set()
                for country in pub.findall(".//Country"):
                    if country.text is not None:
                        countries.add(country.text)
                # Adding a new attribute "countries" to the pubs_info[pmid_str], which is a dictionary for
                # a publication info
                pubs_info[pmid_str]["countries"] = countries

        e_time = time.time()
        logger.info(f"Fetching pubmed countries time: {round(e_time - s_time, 1)}")

        ret_val = list(pubs_info.values())
        if logger.getEffectiveLevel() == logging.DEBUG:
            pprint.pprint(ret_val)
        return ret_val


