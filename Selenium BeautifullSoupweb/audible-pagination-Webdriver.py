from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd

# webdriver manager
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, NoSuchFrameException, StaleElementReferenceException
import pandas as pd
import os
import time

# Headless mode
options = Options()  # Initialize an instance of the Options class
options.headless = True  # True -> Headless mode activated
# options.add_argument('window-size=1920x1080')  # Set a big window size, so all the data will be displayed

web = "https://www.audible.com/adblbestsellers?ref=a_hp_t1_navTop_pl0cg1c0r0&pf_rd_p=c592ea51-fd36-4dc9-b9af-f665ee88670b&pf_rd_r=2RQ0V8Q32K3QPB4YPEP6&pageLoadId=uQvoCEnirAyEH7RG&creativeId=711b5140-9c53-4812-acee-f4c553eb51fe"
# path = '/Users/frank/Downloads/chromedriver'

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)

# add the "options" argument to make sure the changes are applied
driver.get(web)
# driver.maximize_window()

# Pagination
pagination = driver.find_element(
    By.XPATH, '//ul[contains(@class,"pagingElements")]')

pages = pagination.find_elements(By.TAG_NAME, 'li')
last_page = int(pages[-2].text)

current_page = 1

book_title = []
book_author = []
book_length = []

while current_page <= last_page:
    time.sleep(2)
    bookTitle = driver.find_elements(
        By.XPATH, '//h3[contains(@class, "bc-heading")]/a')

    for bk in bookTitle:
        book_title.append(bk.text)
        # print(bk.text)

        bookAuthor = driver.find_elements(
            By.XPATH, '//div[contains(@class,"adbl-impression-container ")]//li[contains(@class,"authorLabel")]')

    for ba in bookAuthor:
        book_author.append(ba.text)
        # print(ba.text)

    bookLength = driver.find_elements(
        By.XPATH, '//div[contains(@class,"adbl-impression-container ")]//li[contains(@class,"runtimeLabel")]')

    for bl in bookLength:
        book_length.append(bl.text)
        # print(bl.text)

    current_page = current_page+1

    try:
        next_page = WebDriverWait(driver, 5).until(EC.presence_of_element_located(
            By.XPATH, '//span[contains(@class,"nextButton")]'))
        # driver.find_element(
            # By.XPATH, '//span[contains(@class,"nextButton")]')

        # next_page = driver.find_element(
        #     By.XPATH, '//span[contains(@class,"nextButton")]')
        next_page.click()

    except:
        pass


# print('book title length :',len(book_title))
# print('book author length :',len(book_author))
# print('book time length :',len(book_length))

df=pd.DataFrame({'book_title':book_title,'book_author':book_author,'book_length':book_length })
print(df)
df.to_csv('d:\\TEMP\\BooksAuthor.csv',index=False)

driver.quit()
