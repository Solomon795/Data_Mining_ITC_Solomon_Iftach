"""Parsing Research-Gate publications on Energy Market subject
For now we scraped info from search pages only not going to page of every individual
publication (didn't use grequests yet, planning to use it)"""

# we use combo of bs4, selenium and requests at this point
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from time import sleep
import Configuration
#import grequests


# CONSTANTS used in the program
conf = Configuration.Configuration()
topic_name, num_pages = conf.get_topic_settings()
#num_pages = int(num_pages_str)

def get_url():
    """
    Going to our homepage
    :return browser, url:
    """
    browser = webdriver.Chrome()
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
    sleep(3)


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
    authors = publication.find('ul',
                               class_='nova-legacy-e-list nova-legacy-e-list--size-m nova-legacy-e-list--type-inline '
                                      'nova-legacy-e-list--spacing-none nova-legacy-v-entity-item__person-list').text
    return authors


def parse_single_pub_monthyear(publication):
    """
    Scraping month and year of publishing of individual publication from given search page
    :param publication:
    :return monthyear:
    """
    monthyear = publication.findAll('li', class_='nova-legacy-e-list__item nova-legacy-v-entity-item__meta-data-item')[
        0].text
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


def next_page(browser, next_page_number):
    """
    This function makes the click on the next/chosen page number,
    as the main URL address is not updated by changing page.
    :param next_button:
    :return:
    """
    # Looking for the button, that has an inner span element, with the number of the page as its value
    # i.e. < span class ="nova-legacy-c-button__labelf" > {next_page_number} < /span >
    xpath_expression = f'//button[.//span[contains(text(),{next_page_number})]]'
    browser.find_element(By.XPATH, xpath_expression).click()
    return


def main():
    """Costructor function"""
    # Initializing our container for parsed info of publications
    data = []
    # Launching chrome and signing in
    browser, url = get_url()
    sign_in(url, browser)

    # Headers needed for the parallel downloading by grequests
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}

    # Looping through pages (with finding all pubs on each page)
    for p in range(1, num_pages):
        print("Page proccessing: ", p)
        pubs = find_all_pubs_on_page(browser)

        # Scraping relevant info of individual publication from given search page
        # and appending it to our list container as a dictionary

        # Downloading in parallel way all the 10 publications of a single page into
        # my_requests.
        # my_requests = (grequests.get(u, headers=headers) for u in pubs)
        # responses = grequests.map(my_requests)

        for pub in pubs:  #responses:
            material = parse_single_pub_material(pub)
            title = parse_single_pub_title(pub)
            site = parse_single_pub_site(pub)
            journal = parse_single_pub_journal(pub)
            authors = parse_single_pub_authors(pub)
            monthyear = parse_single_pub_monthyear(pub)
            reads = parse_single_pub_reads(pub)
            citations = parse_single_pub_citations(pub)
            data.append({"material": material, "title": title, "site": site, "journal": journal,
                         "authors": authors, "month - year": monthyear, "reads": reads,
                         "citations": citations})
        print("Total publications parsed: ", len(data))

        # navigating to the next page, by pressing the next page button on the bottom of the page
        next_page(browser, p)

        sleep(3)
    print(*data, sep="\n")
    return data


if __name__ == '__main__':
    main()
