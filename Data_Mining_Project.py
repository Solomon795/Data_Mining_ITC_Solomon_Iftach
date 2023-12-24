"""Parsing Research-Gate publications on Energy Market subject
For now we scraped info from search pages only not going to page of every individual
publication (didn't use grequests yet, planning to use it)"""

# we use combo of bs4, selenium and requests at this point
from dm_setup_manage_browser import *
from dm_parse_info import *
import argparse
import re

import DatabaseManager
import json



def get_publications_info(publication, driver):
    """
    This function gathers all the data for a single publication
    """
    publication_type = parse_single_pub_material(publication)
    title = parse_single_pub_title(publication)
    site = parse_single_pub_site(publication)
    pub_id = re.search(r"publication/(\d+)_", site)  # use r to indicate a raw string
    pub_id = pub_id.group(1)
    journal = parse_single_pub_journal(publication)
    authors = parse_single_pub_authors(publication)
    monthyear = parse_single_pub_monthyear(publication)
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
    try:
        reads = int(parse_single_pub_reads(publication).split()[0])
    except IndexError:
        reads = 0
    except ValueError:
        reads = parse_single_pub_reads(publication).split()[0]
        reads = ''.join(char for char in reads if char.isdigit())
    try:
        citations = int(parse_single_pub_citations(publication).split()[0])
    except IndexError:
        citations = 0
    data = {"publication_type": publication_type, "title": title, "site": site, "journal": journal, "id": pub_id,
            "authors": authors, "year": monthyear.year, "reads": reads,
            "citations": citations, "doi": doi}
    return data


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Choose parameters for parsing - ')
    parser.add_argument('num_pages', type=int, help='Give a positive integer value for number of pages')  # 2
    parser.add_argument('topic', type=str, help='Give a topic for publications search')  # 3
    parser.add_argument('-w', action="store_true")
    args = parser.parse_args()
    if args.w:
        print("Good day! Today we are parsing ResearchGate! :)")
    num_pages = args.num_pages

    topic = args.topic
    # Check if topic exists already, if not insert it to DB.
    db_manager = DatabaseManager.DatabaseManager(conf, topic)

    """Constructor function"""
    # Initializing our container for parsed info of publications
    publications_info_list = []
    # Launching chrome and signing in
    browser1, url = get_url(topic)
    while True:
        try:
            sign_in(url, browser1)
            break
        except selenium.common.exceptions.ElementClickInterceptedException and selenium.common.exceptions.ElementNotInteractableException and selenium.common.exceptions.WebDriverException:
            sleep(1)

    # Headers needed for the parallel downloading by grequests
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}

    # Looping through pages (with finding all pubs on each page)
    for p in range(1, num_pages + 1):
        print("Page proccessing: ", p)
        sleep(1)
        pubs1 = find_all_pubs_on_page(browser1)

        for pub in pubs1:
            dictionary = get_publications_info(pub, browser1)
            # Preprints are excluded as they break the uniqueness of titles
            # when article with same title exists.
            if dictionary["publication_type"] != 'Preprint':
                publications_info_list.append(dictionary)
        print("Total publications parsed: ", len(publications_info_list))

        # if p % 100 == 0 or p == num_pages:
        #     db_manager.insert_publications_info(publications_info_list)
        #     publications_info_list = []  # initiation of the list prior to accepting new batch of publications info.

        # navigating to the next page, by pressing the next page button on the bottom of the page
        if p < num_pages + 1:
            next_page(browser1)

        # Accumulating data

    print(*publications_info_list, sep="\n")
    end_time = time.time()
    print(f"It took {end_time - start_time} sec")

    browser1.close()

    return publications_info_list


if __name__ == '__main__':
    main()
