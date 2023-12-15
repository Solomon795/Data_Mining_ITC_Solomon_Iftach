from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from time import sleep
import Configuration
import argparse
import time

options = webdriver.ChromeOptions()
options.add_experimental_option("prefs", {
    # block image loading
    "profile.managed_default_content_settings.images": 2,
})

browser = webdriver.Chrome(options=options)
url = f"https://www.researchgate.net/search/publication?q=Energy&page=1"
sleep(1)
browser.get(url)
sleep(1)
# Finding button "Log In" by its text and pressing it
browser.find_element(By.LINK_TEXT, "Log in").click()

# Finding login input text field "Log In" by its XPATH
input_login = browser.find_element(By.XPATH, '//*[@id="input-header-login"]')
# Typing my registered email into login input text field
input_login.send_keys("Solomon.Iashuvaev@bakerhughes.com")
# Finding password input text field "Log In" by its XPATH
input_pass = browser.find_element(By.XPATH, '//*[@id="input-header-password"]')
# Typing my registered password into password input text field
input_pass.send_keys("Solmars1@")
# Findind and clicking final login
browser.find_element(By.CSS_SELECTOR,
                     '#headerLoginForm > div.nova-legacy-l-flex__item.nova-legacy-l-flex.nova-legacy-l-flex'
                     '--gutter-m'
                     '.nova-legacy-l-flex--direction-column\@s-up.nova-legacy-l-flex--align-items-stretch\@s-up'
                     '.nova'
                     '-legacy-l-flex--justify-content-flex-start\@s-up.nova-legacy-l-flex--wrap-nowrap\@s-up > '
                     'div:nth-child(1) > button').click()
sleep(2)
xpath_expression = 'search-dropdown-head__placeholder'
browser.find_elements(By.CLASS_NAME, xpath_expression)[1].click()
sleep(2)
browser.find_elements(By.CLASS_NAME, 'nova-legacy-e-text')[14].click()
sleep(2)
input_start_year = browser.find_elements(By.CLASS_NAME, 'nova-legacy-e-input__field')[1]
input_start_year.send_keys("2019")
input_end_year = browser.find_elements(By.CLASS_NAME, 'nova-legacy-e-input__field')[2]
input_end_year.send_keys("2020")
browser.find_elements(By.CLASS_NAME, 'nova-legacy-c-button__label'
# for f in for1:
#     print(f)


# print(browser.find_elements(By.CLASS_NAME, 'nova-legacy-c-button__label'))
# <span class="search-dropdown-head__placeholder">Any time</span>
"""//*[@id="rgw3_6575e5344f35d"]
nova-legacy-e-text nova-legacy-e-text--size-m nova-legacy-e-text--family-sans-serif nova-legacy-e-text--spacing-none nova-legacy-e-text--color-inherit"""
# rgw3_6575flex__item--grow > div > div > span
# /html/body/div[1]/div[3]/div[1]/div/div/div/div/div/div[3]/div/div[1]/div/div[2]/div/div[2]/span/span/div/div/div/div[1]/div/div/span
