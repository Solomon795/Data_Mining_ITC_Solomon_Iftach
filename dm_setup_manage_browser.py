# we use combo of bs4, selenium and requests at this point
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import time
from time import sleep

import Configuration


# CONSTANTS used in the program
conf = Configuration.Configuration()


def get_url(topic_name):
    """
    Going to our homepage
    :return browser, url:
    """
    options = uc.ChromeOptions()
    options.add_experimental_option("prefs", {
        # block image loading
        "profile.managed_default_content_settings.images": 2,
    })
    # options.add_argument("--headless")
    user_agent = conf.get_headers()
    options.add_argument(f'user-agent={user_agent}')
    browser = uc.Chrome(options=options)
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
    sleep(1)
    my_chrome.find_elements(By.CLASS_NAME, 'nova-legacy-e-text')[7].click()
    my_chrome.find_elements(By.CLASS_NAME, 'nova-legacy-c-dropdown__action')[0].click()




def next_page(browser1):
    """
    This function makes the click on the next/chosen page number,
    as the main URL address is not updated by changing page.
    :param browser:
    :return:
    """
    # Looking for the button, that has an inner span element, with the number of the page as its value
    # i.e. < span class ="nova-legacy-c-button__labelf" > {next_page_number} < /span >
    browser1.execute_script("window.scrollBy(0, 1000);")
    # xpath_expression = f'//button[.//span[contains(text(),{next_page_number})]]'
    while True:
        try:
            # browser.find_element(By.XPATH, xpath_expression).click()
            browser1.find_element(By.XPATH,
                                 '/html/body/div[1]/div[3]/div[1]/div/div/div/div/div/div[3]/div/div[1]/div/div[3]/div[2]/div/nav/button[2]').click()
            break
        except selenium.common.exceptions.ElementClickInterceptedException and selenium.common.exceptions.ElementNotInteractableException and selenium.common.exceptions.WebDriverException:
            sleep(1)
    return