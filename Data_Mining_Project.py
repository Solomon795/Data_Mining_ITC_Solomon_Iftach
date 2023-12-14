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

import Configuration
import DatabaseManager

# import grequests


# CONSTANTS used in the program
conf = Configuration.Configuration()


# num_pages = int(num_pages_str)

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
    # options.add_extension("C:/Users/solom/AppData/Local/Google/Chrome/User Data/Default/Extensions/gighmmpiobklfepjocnamgkkbiglidom/5.15.0_0/adblock.css")
    browser = webdriver.Chrome(options=options)
    url = f"https://www.researchgate.net/search/publication?q={topic_name}&page=1"
    return browser, url


def sign_in(my_url, my_chrome):
    """
    Signing in ResearchGate, so we could scrape more info just from main page
    :param my_url:
    :param my_chrome:
    :return:
    """
    email, password = conf.get_user_credentials()

    my_chrome.get(my_url)
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

    ### range of years


def find_all_pubs_on_page(my_chrome):
    """
    Finding all publications on each individual page by class via BS4
    so that the pubs contain all the URLs of each publication in the page
    :param my_chrome:
    :return pubs: List of URLs
    """
    soup = BeautifulSoup(my_chrome.page_source, 'lxml')

    pubs = soup.findAll('div', class_='nova-legacy-v-entity-item__stack nova-legacy-v-entity-item__stack--gutter-m')
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


def parse_single_pub_year(publication):
    """
    Scraping month and year of publishing of individual publication from given search page
    :param publication:
    :return monthyear:
    """
    monthyear = publication.findAll('li', class_='nova-legacy-e-list__item nova-legacy-v-entity-item__meta-data-item')[
        0].text
    # monthyear = datetime.strptime(monthyear, '%B %Y')
    year = 2023
    return year


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


def next_page(browser, next_page_number):
    """
    This function makes the click on the next/chosen page number,
    as the main URL address is not updated by changing page.
    :param browser:
    :param next_page_number:
    :return:
    """
    # Looking for the button, that has an inner span element, with the number of the page as its value
    # i.e. < span class ="nova-legacy-c-button__labelf" > {next_page_number} < /span >
    browser.execute_script("window.scrollBy(0, 500);")
    xpath_expression = f'//button[.//span[contains(text(),{next_page_number})]]'
    while True:
        try:
            browser.find_element(By.XPATH, xpath_expression).click()
            break
        except selenium.common.exceptions.ElementClickInterceptedException and selenium.common.exceptions.ElementNotInteractableException and selenium.common.exceptions.WebDriverException:
            sleep(1)
    return


def get_publications_info(pubs, publications_info_list):
    """
    This function updates publications_info_list
    Scraping relevant info of all individual publications from given search page
    and appending their values as dictionary to our list container,

    :param pubs:
    :param publications_info_list:
    :return None:
    """
    for pub in pubs:  # responses:
        publication_type = parse_single_pub_material(pub)
        title = parse_single_pub_title(pub)
        site = parse_single_pub_site(pub)
        pub_id = int(site[41:50])
        journal = parse_single_pub_journal(pub)
        authors = parse_single_pub_authors(pub)
        year = parse_single_pub_year(pub)
        try:
            reads = int(parse_single_pub_reads(pub).split()[0])
        except IndexError:
            reads = 0
        try:
            citations = int(parse_single_pub_citations(pub).split()[0])
        except IndexError:
            citations = 0
        publications_info_list.append(
            {"publication_type": publication_type, "title": title, "site": site, "journal": journal, "id": pub_id,
             "authors": authors, "year": year, "reads": reads,
             "citations": citations})


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

    """Costructor function"""
    # Initializing our container for parsed info of publications
    publications_info_list = []
    # Launching chrome and signing in
    browser, url = get_url(topic)
    while True:
        try:
            sign_in(url, browser)
            break
        except selenium.common.exceptions.ElementClickInterceptedException and selenium.common.exceptions.ElementNotInteractableException and selenium.common.exceptions.WebDriverException:
            sleep(1)

    # Headers needed for the parallel downloading by grequests
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}

    # Looping through pages (with finding all pubs on each page)
    for p in range(1, num_pages+1):
        print("Page proccessing: ", p)
        # url_page = f"https://www.researchgate.net/search/publication?q={topic}&page={p}"
        # browser.get(url_page)
        # sleep(3)

        pubs = find_all_pubs_on_page(browser)
        get_publications_info(pubs, publications_info_list)
        print("Total publications parsed: ", len(publications_info_list))

        if p % 100 == 0 or p == num_pages:
            db_manager.insert_publications_info(publications_info_list)
            publications_info_list=[]   # initiation of the list prior to accepting new batch of publications info.

        # navigating to the next page, by pressing the next page button on the bottom of the page
        next_page(browser, p)

        # Accumulating data

    print(*publications_info_list, sep="\n")
    end_time = time.time()
    print(f"It took {end_time - start_time} sec")
    return publications_info_list


if __name__ == '__main__':
    main()
