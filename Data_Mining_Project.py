"""Parsing Research-Gate publications on Energy Market subject
For now we scraped info from search pages only not going to page of every individual
publication (didn't use grequests yet, planning to use it)"""

# we use combo of bs4, selenium and requests at this point
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from time import sleep
import argparse
import time
from datetime import datetime
import re

import Configuration
import DatabaseManager
import pubmed_wrapper
import json

# CONSTANTS used in the program
conf = Configuration.Configuration()


def get_url(topic_name):
    """
    Going to our homepage
    :return browser, url:
    """
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        # block image loading
        "profile.managed_default_content_settings.images": 2,
    })

    browser = webdriver.Chrome(options=options)
    url = f"https://www.researchgate.net/search/publication?q={topic_name}&page=1"
    browser.get(url)
    return browser, url


def sign_in(my_url, my_chrome):
    """
    Signing in ResearchGate, so we could scrape more info just from main page
    :param my_url:
    :param my_chrome:
    :return:
    """
    email, password = conf.get_user_credentials()

    # Finding button "Log In" by its text and pressing it
    my_chrome.find_element(By.LINK_TEXT, "Log in").click()

    # Finding login input text field "Log In" by its XPATH
    input_login = my_chrome.find_element(By.XPATH, '//*[@id="input-header-login"]')
    # Typing my registered email into login input text field
    input_login.send_keys(email)
    # Finding password input text field "Log In" by its XPATH
    input_pass = my_chrome.find_element(By.XPATH, '//*[@id="input-header-password"]')
    # Typing my registered password into password input text field
    input_pass.send_keys(password)
    # Findind and clicking final login
    my_chrome.find_element(By.CSS_SELECTOR,
                           '#headerLoginForm > div.nova-legacy-l-flex__item.nova-legacy-l-flex.nova-legacy-l-flex'
                           '--gutter-m'
                           '.nova-legacy-l-flex--direction-column\@s-up.nova-legacy-l-flex--align-items-stretch\@s-up'
                           '.nova'
                           '-legacy-l-flex--justify-content-flex-start\@s-up.nova-legacy-l-flex--wrap-nowrap\@s-up > '
                           'div:nth-child(1) > button').click()


def find_all_pubs_on_page(my_chrome):
    """
    Finding all publications on each individual page by class via BS4
    so that the pubs contain all the URLs of each publication in the page
    :param my_chrome:
    :return pubs: List of URLs
    """
    soup = BeautifulSoup(my_chrome.page_source, 'lxml')

    pubs = soup.findAll('div', class_='nova-legacy-v-entity-item__stack nova-legacy-v-entity-item__stack--gutter-m')
    if len(pubs) == 0:
        pubs = soup.findAll('div', class_='nova-legacy-c-card nova-legacy-c-card--spacing-xl nova-legacy-c-card--elevation-1-above')
    return pubs


# href="publication/364328816_Modeling_Electricity_Markets_and_Energy_Systems_Challenges_and_Opportunities_Ahead"

def parse_single_pub_material(publication):
    """
    Scraping type of individual publication from given search page
    :param publication:
    :return:
    """
    material = publication.find('a',
                                class_='nova-legacy-e-badge nova-legacy-e-badge--color-green '
                                       'nova-legacy-e-badge--display-block nova-legacy-e-badge--luminosity-high '
                                       'nova-legacy-e-badge--size-l nova-legacy-e-badge--theme-solid '
                                       'nova-legacy-e-badge--radius-m nova-legacy-v-entity-item__badge').text
    return material


def parse_single_pub_title(publication):
    """
    Scraping title of individual publication from given search page
    :param publication:
    :return title:
    """
    title = publication.find('a',
                             class_='nova-legacy-e-link nova-legacy-e-link--color-inherit '
                                    'nova-legacy-e-link--theme-bare').text
    return title


def parse_single_pub_site(publication):
    """
    Scraping site of individual publication from given search page
    :param publication:
    :return site:
    """
    site = "https://www.researchgate.net/" + publication.find('a', class_='nova-legacy-e-link '
                                                                          'nova-legacy-e-link--color-inherit '
                                                                          'nova-legacy-e-link--theme-bare').get(
        'href')
    return site


def parse_single_pub_journal(publication):
    """
    Scraping publishing journal of individual publication from given search page
    :param publication:
    :return journal:
    """
    if publication.find('div',
                        class_='nova-legacy-e-text nova-legacy-e-text--size-m nova-legacy-e-text--family-sans-serif '
                               'nova-legacy-e-text--spacing-none nova-legacy-e-text--color-grey-900 '
                               'nova-legacy-e-text--clamp') is None:
        journal = "No journal indicated"
    else:
        journal = publication.find('div',
                                   class_='nova-legacy-e-text nova-legacy-e-text--size-m '
                                          'nova-legacy-e-text--family-sans-serif nova-legacy-e-text--spacing-none '
                                          'nova-legacy-e-text--color-grey-900 nova-legacy-e-text--clamp').text
    return journal


def parse_single_pub_authors(publication):
    """
    Scraping authors of individual publication from given search page
    :param publication:
    :return authors:
    """
    authors = publication.findAll('span', class_='nova-legacy-v-person-inline-item__fullname')
    authors_list = []
    for author in authors:
        author = author.text
        authors_list.append(author)
    return authors_list


def parse_single_pub_monthyear(publication):
    """
    Scraping month and year of publishing of individual publication from given search page
    :param publication:
    :return monthyear:
    """
    monthyear = publication.findAll('li', class_='nova-legacy-e-list__item nova-legacy-v-entity-item__meta-data-item')[
        0].text
    monthyear = datetime.strptime(monthyear, '%B %Y')
    return monthyear


def parse_single_pub_reads(publication):
    """
    Scraping number of reads of individual publication from given search page
    :param publication:
    :return reads:
    """
    reads = publication.findAll('li', class_='nova-legacy-e-list__item nova-legacy-v-entity-item__meta-data-item')[
        1].text
    return reads


def parse_single_pub_citations(publication):
    """
    Scraping number of citations of individual publication from given search page
    :param publication:
    :return citations:
    """
    citations = publication.findAll('li', class_='nova-legacy-e-list__item nova-legacy-v-entity-item__meta-data-item')[
        2].text
    return citations


def next_page(browser1, browser2):
    """
    This function makes the click on the next/chosen page number,
    as the main URL address is not updated by changing page.
    :param browser:
    :return:
    """
    # Looking for the button, that has an inner span element, with the number of the page as its value
    # i.e. < span class ="nova-legacy-c-button__labelf" > {next_page_number} < /span >
    browser1.execute_script("window.scrollBy(0, 1000);")
    browser2.execute_script("window.scrollBy(0, 1000);")
    # xpath_expression = f'//button[.//span[contains(text(),{next_page_number})]]'
    while True:
        try:
            # browser.find_element(By.XPATH, xpath_expression).click()
            browser1.find_element(By.XPATH,
                                 '/html/body/div[1]/div[3]/div[1]/div/div/div/div/div/div[3]/div/div[1]/div/div[3]/div[2]/div/nav/button[2]').click()
            browser2.find_element(By.XPATH,
                                  '/html/body/div[1]/div[1]/div[1]/div/div/div[2]/div[2]/div/div[2]/div/div[1]/div/div/div[11]/div/div[9]').click()
            break
        except selenium.common.exceptions.ElementClickInterceptedException and selenium.common.exceptions.ElementNotInteractableException and selenium.common.exceptions.WebDriverException:
            sleep(1)
    return


def get_publications_info(publication):
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
            "citations": citations}
    return data



def main():
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
    # Check if topic exists already, if not insert it to DB.
    db_manager = DatabaseManager.DatabaseManager(conf, topic, db_source)

    """Costructor function"""
    # Initializing our container for parsed info of publications
    publications_info_list = []
    doi_list = []
    # Launching chrome and signing in
    browser1, url = get_url(topic)
    browser2, url = get_url(topic)
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

    # Looping through pages (with finding all pubs on each page) of Researchgate site
    for p in range(1, num_pages + 1):
        print("Page proccessing: ", p)
        sleep(1)
        pubs1 = find_all_pubs_on_page(browser1)

        for pub in pubs1:
            dictionary = get_publications_info(pub)
            # Preprints are excluded as they break the uniqueness of titles
            # when article with same title exists.
            if dictionary["publication_type"] != 'Preprint':
                publications_info_list.append(dictionary)
        for pub in pubs2:
            pub_type = pub.find('div', class_='nova-legacy-v-publication-item__meta-left').text
            if pub_type != 'Preprint':
                doi = pub.findAll('li', class_='nova-legacy-e-list__item')[1].text
                doi_list.append(doi[5:])
        print("Total publications parsed: ", len(publications_info_list))

        if p % 100 == 0 or p == num_pages:
            db_manager.insert_publications_info(db_source, publications_info_list)
            publications_info_list = []  # initiation of the list prior to accepting new batch of publications info.

        # navigating to the next page, by pressing the next page button on the bottom of the page
        if p < num_pages + 1:
            next_page(browser1, browser2)

        # Accumulating data
    for index, pub in enumerate(publications_info_list):
        pub["doi"] = doi_list[index]

    print(*publications_info_list, sep="\n")
    end_time = time.time()
    print(f"It took {end_time - start_time} sec")

    browser1.close()
    browser2.close()

    ##### PUBMED #########
    # Retrieving information from Pubmed site
    db_pubmed = pubmed_wrapper.PubmedWrapper(topic)
    db_source = 1  # Pubmed
    num_pubs_requested = p * 10
    # Fetching publications info including doi, and pubmed_id
    pubmed_info = db_pubmed.fetch_pubs_info(num_pubs_requested)
    #pubmed_countries = db_pubmed.fetch_countries()
    db_manager.insert_publications_info(db_source, pubmed_info)

    return publications_info_list


if __name__ == '__main__':
    main()
