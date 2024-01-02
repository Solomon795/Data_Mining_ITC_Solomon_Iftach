""" This script gets information on a particular scientific topic. it does that by:
1. Scraping and parsing Research-Gate publications
2. Using Pubmed Api
For now we scraped info from search pages only (not going to the page of every individual
publication. As so, didn't use grequests yet, planning to use it)"""

# we use combo of bs4, selenium and requests at this point
from dm_setup_manage_browser import *
from dm_parse_info import *
import argparse
import re
import logging
import DatabaseManager
import pubmed_wrapper
import json
import traceback

MAX_PAGES_PER_BATCH = 5

def setup_logger():
    """
    Set up the logger and log file (data_mining.log) with loggin.basicConfig - this will set these properties to
    alll loggers in the project.
    :return: logger
    """
    logging.basicConfig(format='%(asctime)s;%(name)s;%(funcName)s;%(levelname)s;%(message)s',
                        level=logging.INFO,
                        encoding="utf-8",
                        handlers=[
                            logging.FileHandler("data_mining.log", encoding="utf-8"),
                            logging.StreamHandler()])

    return logging.getLogger(__name__)


# Initialize the logger by calling setup_logger()
logger = setup_logger()


def get_publications_info(publication, driver):
    """
    This function gathers all the data for a single publication
    """
    publication_type = parse_single_pub_material(publication)
    logger.debug(f'Publication type ({publication_type}) parsed successfully')
    title = parse_single_pub_title(publication)
    logger.debug(f'Publication title ({title}) parsed successfully')
    site = parse_single_pub_site(publication)
    logger.debug(f'Publication site ({site}) parsed successfully')
    pub_id = re.search(r"publication/(\d+)_", site)  # use r to indicate a raw string
    pub_id = pub_id.group(1)
    logger.debug(f'Publication research gate id ({pub_id}) parsed successfully')
    journal = parse_single_pub_journal(publication)
    logger.debug(f'Publication journal ({journal}) parsed successfully')
    authors = parse_single_pub_authors(publication)
    logger.debug(f'Publication authors ({authors}) parsed successfully')
    monthyear = parse_single_pub_monthyear(publication)
    logger.debug(f'Publication year ({monthyear}) parsed successfully')
    original_tab = driver.current_window_handle
    driver.switch_to.new_window('tab')
    driver.get(site)
    try:
        doi = parse_single_pub_doi(driver)
    except IndexError:
        doi = ""
    driver.close()

    # Switch back to the old tab or window
    driver.switch_to.window(original_tab)
    logger.debug(f'Publication doi number ({doi}) parsed successfully')
    try:
        reads = int(parse_single_pub_reads(publication).split()[0])
    except IndexError:
        reads = 0
    except ValueError:
        reads = parse_single_pub_reads(publication).split()[0]
        reads = ''.join(char for char in reads if char.isdigit())
    logger.debug(f'Publication reads number ({reads}) parsed successfully')
    try:
        citations = int(parse_single_pub_citations(publication).split()[0])
    except IndexError:
        citations = 0
    logger.debug(f'Publication citations number ({citations}) parsed successfully')
    data = {"publication_type": publication_type, "title": title, "site": site, "journal": journal, "id": pub_id,
            "authors": authors, "year": monthyear.year, "reads": reads,
            "citations": citations, "doi": doi}
    return data


def fetching_from_pubmed_and_insert_db(db_manager, num_pages, topic):
    """
    The function gets the number of pages and the topic and getches information from Pubmed using Pubmed's API
    :param num_pages:
    :param topic:
    :return: a list of information on fetched publication.
    """
    logger.info("Fetching from pubmed ...")
    db_pubmed = pubmed_wrapper.PubmedWrapper(topic)
    db_source = 1  # Pubmed

    # Handling pages left out after performing mode MAX_PAGES_PER_BATCH)
    if num_pages % MAX_PAGES_PER_BATCH != 0:
        pubs_info = db_pubmed.fetch_pubs_info(num_pages*10)
        db_manager.insert_publications_info(db_source, pubs_info)

    # Handling all other pages
    num_pages_left = num_pages - num_pages % MAX_PAGES_PER_BATCH
    # Fetching publications by parts (batch = MAX_PAGES_PER_BATCH pages, that is MAX_PAGES_PER_BATCH * 10 pubs)
    for i in range(0, num_pages_left, MAX_PAGES_PER_BATCH):
        pubs_info = db_pubmed.fetch_pubs_info(MAX_PAGES_PER_BATCH * 10)
        db_manager.insert_publications_info(db_source, pubs_info)


def scraping_researchgate_and_insert_db(db_manager, num_pages, topic):
    logger.info("Scraping from Researchgate ...")
    start_time = time.time()

    # Initializing our container for parsed info of publications
    publications_info_list = []
    # Launching chrome and signing in
    browser1, url = get_url(topic)
    logger.info(f"URL {url} opened successfully in automated browser (selenium)")
    logger.info(f"Starting signing in ...")
    while True:
        try:
            sign_in(url, browser1)
            logger.info(f"Signed in successfully in automated browser (selenium)")
            break
        except selenium.common.exceptions.ElementClickInterceptedException and selenium.common.exceptions.ElementNotInteractableException and selenium.common.exceptions.WebDriverException:
            logger.debug(f"Waiting for sign in tags to appear (1 sec)")
            sleep(1)
    # Headers needed for the parallel downloading by grequests
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}
    # Looping through pages (with finding all pubs on each page)
    for p in range(1, num_pages + 1):
        logger.debug(f"Page proccessing: {p}")
        sleep(1)
        pubs1 = find_all_pubs_on_page(browser1)
        logger.info(f"Publications on page {p} recognized successfully")
        num_no_doi = 0
        for index, pub in enumerate(pubs1):
            dictionary = get_publications_info(pub, browser1)
            logger.debug(f"All info on publication {p * 10 - 10 + index} recognized successfully")
            if dictionary['doi'] == '':
                num_no_doi += 1
                logger.debug(f"dictionary:{dictionary}")

            if dictionary['doi'] != '':
                publications_info_list.append(dictionary)
                logger.debug(f"Publication {p * 10 - 10 + index} added to collection")
        logger.info(f"Total publications parsed: {len(publications_info_list)}, publications with no doi:{num_no_doi}")

        if p % MAX_PAGES_PER_BATCH == 0 or p == num_pages:
            db_source = 0  # ResearchGate
            if len(publications_info_list) > 0:
                db_manager.insert_publications_info(db_source, publications_info_list)
            publications_info_list = []  # initiation of the list prior to accepting new batch of publications info.

        # navigating to the next page, by pressing the next page button on the bottom of the page
        if p < num_pages:
            next_page(browser1)
            logger.info(f"Turned to page {p + 1}")

    # Accumulating data
    #print(*publications_info_list, sep="\n")
    end_time = time.time()
    logger.info(f"It took {round(end_time - start_time, 1)} sec")
    browser1.close()
    return publications_info_list


def parse_commandline_arguments():
    parser = argparse.ArgumentParser(description='Choose parameters for parsing - ')
    parser.add_argument('num_pages', type=int, help='Give a positive integer value for number of pages')  # 2
    parser.add_argument('topic', type=str, help='Give a topic for publications search')  # 3
    parser.add_argument('-w', action="store_true")
    args = parser.parse_args()
    if args.w:
        print("Good day! Today we are parsing ResearchGate and Pubmed! :)")
    return args


"""
Defining logger as global, so that's it would be accessible in all functions
"""

def main():
    try:
        # Paring the arguments
        args = parse_commandline_arguments()
        num_pages = args.num_pages
        topic = args.topic
        logger.info(f"Number of page = {num_pages}, topic - {topic} parsed from Command Line Interface")

        # Common initializations to both Researchgate and Pubmed
        db_manager = DatabaseManager.DatabaseManager(conf, topic)

        # Scrape information from Researchgate
        scraping_researchgate_and_insert_db(db_manager, num_pages, topic)

        # Retrieving information from Pubmed site
        fetching_from_pubmed_and_insert_db(db_manager, num_pages, topic)
    except Exception as e:
        logger.fatal(f"Script failed with:{e}")
        # Get the full stack trace of the exception
        full_traceback = traceback.format_exc()
        logger.fatal("Full stack trace:")
        logger.fatal(full_traceback)
        raise e


if __name__ == '__main__':
    main()
