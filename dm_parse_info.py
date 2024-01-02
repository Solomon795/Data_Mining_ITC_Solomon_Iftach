

# we use combo of bs4, selenium and requests at this point
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from time import sleep
from datetime import datetime
import logging

# Initialize the logger according to the module name
logger = logging.getLogger(__name__)

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
    try:
        monthyear = datetime.strptime(monthyear, '%B %Y')
    except ValueError as e:
        logger.debug("in parse_single_pub_monthyear, the following exception raised:", e)
        special_year = 2100
        monthyear = datetime(year=special_year, month=1, day=1)
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

def parse_single_pub_doi(driver):

    doi = driver.find_elements(By.XPATH, "//*[contains(text(), '10.')]")[0].text
    return doi