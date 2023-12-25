"""Parsing Research-Gate publications on Energy Market subject
For now we scraped info from search pages only not going to page of every individual
publication (didn't use grequests yet, planning to use it)"""

# we use combo of bs4, selenium and requests at this point
from dm_setup_manage_browser import *
from dm_parse_info import *
import argparse
import re
import logging
import DatabaseManager
import pubmed_wrapper
import json

def setup_logger():
    """
    Set up the logger and log file (parse_process.log)
    :return: parse_process.log (or logger)
    """
    logger = logging.getLogger('logger1')
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler('parse_process.log')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - FUNC:%(funcName)s - LINE:%(lineno)d')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def get_publications_info(publication, driver, log=None):
    """
    This function gathers all the data for a single publication
    """
    publication_type = parse_single_pub_material(publication)
    log.info(f'Publication type ({publication_type}) parsed successfully')
    title = parse_single_pub_title(publication)
    log.info(f'Publication title ({title}) parsed successfully')
    site = parse_single_pub_site(publication)
    log.info(f'Publication site ({site}) parsed successfully')
    pub_id = re.search(r"publication/(\d+)_", site)  # use r to indicate a raw string
    pub_id = pub_id.group(1)
    log.info(f'Publication research gate id ({pub_id}) parsed successfully')
    journal = parse_single_pub_journal(publication)
    log.info(f'Publication journal ({journal}) parsed successfully')
    authors = parse_single_pub_authors(publication)
    log.info(f'Publication authors ({authors}) parsed successfully')
    monthyear = parse_single_pub_monthyear(publication)
    log.info(f'Publication year ({monthyear}) parsed successfully')
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
    log.info(f'Publication doi number ({doi}) parsed successfully')
    try:
        reads = int(parse_single_pub_reads(publication).split()[0])
    except IndexError:
        reads = 0
    except ValueError:
        reads = parse_single_pub_reads(publication).split()[0]
        reads = ''.join(char for char in reads if char.isdigit())
    log.info(f'Publication reads number ({reads}) parsed successfully')
    try:
        citations = int(parse_single_pub_citations(publication).split()[0])
    except IndexError:
        citations = 0
    log.info(f'Publication citations number ({citations}) parsed successfully')
    data = {"publication_type": publication_type, "title": title, "site": site, "journal": journal, "id": pub_id,
            "authors": authors, "year": monthyear.year, "reads": reads,
            "citations": citations, "doi": doi}
    return data


def main():
    logger = setup_logger()
    start_time = time.time()
    db_source = 0  # ResearchGate
    parser = argparse.ArgumentParser(description='Choose parameters for parsing - ')
    parser.add_argument('num_pages', type=int, help='Give a positive integer value for number of pages')  # 2
    parser.add_argument('topic', type=str, help='Give a topic for publications search')  # 3
    parser.add_argument('-w', action="store_true")
    args = parser.parse_args()
    if args.w:
        print("Good day! Today we are parsing ResearchGate! :)")
    num_pages = args.num_pages

    topic = args.topic
    logger.info(f"Number of page = {num_pages}, topic - {topic} parsed from Command Line Interface")
    # Check if topic exists already, if not insert it to DB.
    db_source = 0 # Researchgate
    db_manager = DatabaseManager.DatabaseManager(conf, topic)
    logger.info(f"Insertion of topic {topic} into database successful")
    """Constructor function"""
    # Initializing our container for parsed info of publications
    publications_info_list = []
    # Launching chrome and signing in
    browser1, url = get_url(topic)
    logger.info(f"URL {url} opened successfully in automated browser (selenium)")
    while True:
        try:
            sign_in(url, browser1)
            logger.info(f"Signed in successfully in automated browser (selenium)")
            break
        except selenium.common.exceptions.ElementClickInterceptedException and selenium.common.exceptions.ElementNotInteractableException and selenium.common.exceptions.WebDriverException:
            logger.info(f"Waiting for sign in tags to appear (1 sec)")
            sleep(1)

    # Headers needed for the parallel downloading by grequests
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}

    # Looping through pages (with finding all pubs on each page)
    for p in range(1, num_pages + 1):
        logger.info(f"Page proccessing: {p}")
        print("Page proccessing: ", p)
        sleep(1)
        pubs1 = find_all_pubs_on_page(browser1)
        logger.info(f"Publications on page {p} recognized successfully")
        for index, pub in enumerate(pubs1):
            dictionary = get_publications_info(pub, browser1, logger)
            logger.info(f"All info on publication {p * 10 - 10 + index} recognized successfully")
            # Preprints are excluded as they break the uniqueness of titles
            # when article with same title exists.
            if dictionary["publication_type"] != 'Preprint':
                publications_info_list.append(dictionary)
                logger.info(f"Publication {p * 10 - 10 + index} added to collection")
        logger.info(f"Total publications parsed: {len(publications_info_list)}")
        print("Total publications parsed: ", len(publications_info_list))

        if p % 100 == 0 or p == num_pages:
            db_source = 0 # ResearchGate
            db_manager.insert_publications_info(db_source,publications_info_list)
            publications_info_list = []  # initiation of the list prior to accepting new batch of publications info.

        # navigating to the next page, by pressing the next page button on the bottom of the page
        if p < num_pages + 1:
            next_page(browser1)
            logger.info(f"Turned to page {p+1}")

        # Accumulating data

    print(*publications_info_list, sep="\n")
    end_time = time.time()
    logger.info(f"It took {end_time - start_time} sec")
    print(f"It took {end_time - start_time} sec")

    browser1.close()
    browser2.close()

    #####       PUBMED                #########
    # Retrieving information from Pubmed site
    db_pubmed = pubmed_wrapper.PubmedWrapper(topic)
    db_source = 1  # Pubmed
    num_pubs_requested = p * 10
    # Fetching publications info including doi, and pubmed_id
    pubmed_info = db_pubmed.fetch_pubs_info(num_pubs_requested)
    db_manager.insert_publications_info(db_source, pubmed_info)

    return publications_info_list


if __name__ == '__main__':
    main()
