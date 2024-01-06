# we use combo of bs4, selenium and requests at this point
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from time import sleep
from datetime import datetime
import logging
import Configuration

# Initialize the logger according to the module name
logger = logging.getLogger(__name__)
conf = Configuration.Configuration()


def find_all_pubs_on_page(my_chrome):
    """
    Finding all publications on each individual page by class via BS4
    so that the pubs contain all the URLs of each publication in the page
    :param my_chrome:
    :return pubs: List of URLs
    """
    pub_class = conf.get_parse_tag("pub_class")
    soup = BeautifulSoup(my_chrome.page_source, 'lxml')
    pubs = soup.findAll('div', class_=pub_class)
    return pubs


def parse_single_pub_material(publication):
    """
    Scraping type of individual publication from given search page
    :param publication:
    :return:
    """
    material_class = conf.get_parse_tag("material_class")
    material = publication.find('a', class_=material_class).text
    return material


def parse_single_pub_title(publication):
    """
    Scraping title of individual publication from given search page
    :param publication:
    :return title:
    """
    title_class = conf.get_parse_tag("title_class")
    title = publication.find('a', class_=title_class).text
    return title


def parse_single_pub_site(publication):
    """
    Scraping site of individual publication from given search page
    :param publication:
    :return site:
    """
    site_class = conf.get_parse_tag("site_class")
    site = "https://www.researchgate.net/" + publication.find('a', class_=site_class).get('href')
    return site


def parse_single_pub_journal(publication):
    """
    Scraping publishing journal of individual publication from given search page
    :param publication:
    :return journal:
    """
    journal_class = conf.get_parse_tag("journal_class")
    if publication.find('div', class_=journal_class) is None:
        journal = "No journal indicated"
    else:
        journal = publication.find('div', class_=journal_class).text
    return journal


def parse_single_pub_authors(publication):
    """
    Scraping authors of individual publication from given search page
    :param publication:
    :return authors:
    """
    authors_class = conf.get_parse_tag("authors_class")
    authors = publication.findAll('span', class_=authors_class)
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
    monthyear_class = conf.get_parse_tag("monthyear_class")
    monthyear = publication.findAll('li', class_=monthyear_class)[0].text
    # try:
    monthyear = datetime.strptime(monthyear, '%B %Y')
    # except ValueError as e:
    #     logger.debug("in parse_single_pub_monthyear, the following exception raised:", e)
    #     special_year = 2100
    #     monthyear = datetime(year=special_year, month=1, day=1)
    return monthyear


def parse_single_pub_reads(publication):
    """
    Scraping number of reads of individual publication from given search page
    :param publication:
    :return reads:
    """
    reads_class = conf.get_parse_tag("reads_class")
    reads = publication.findAll('li', class_=reads_class)[1].text
    return reads


def parse_single_pub_citations(publication):
    """
    Scraping number of citations of individual publication from given search page
    :param publication:
    :return citations:
    """
    citations_class = conf.get_parse_tag("citations_class")
    citations = publication.findAll('li', class_=citations_class)[2].text
    return citations


def parse_single_pub_doi(driver):
    doi_xpath = conf.get_parse_tag("doi_xpath")
    doi = driver.find_element(By.XPATH, doi_xpath).text
    return doi
