from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd

# webdriver manager
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from selenium.common.exceptions import NoSuchElementException, NoSuchFrameException, StaleElementReferenceException
import pandas as pd
import os
import time

# Headless mode
options = Options()  # Initialize an instance of the Options class
options.headless = True  # True -> Headless mode activated
# options.add_argument('window-size=1920x1080')  # Set a big window size, so all the data will be displayed

web = "https://www.audible.com/search"
# path = '/Users/frank/Downloads/chromedriver'
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
# add the "options" argument to make sure the changes are applied
driver.get(web)
# driver.maximize_window()

container=None
products=None

# container = driver.find_element(By.CLASS_NAME,'adbl-impression-container ')
# products = container.find_elements(By.XPATH,'./li')

book_title = []
book_author = []
book_length = []

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

# print('book title length :',len(book_title))
# print('book author length :',len(book_author))
# print('book time length :',len(book_length))

df=pd.DataFrame({'book_title':book_title,'book_author':book_author,'book_length':book_length })
print(df)
df.to_csv('BooksAuthor.csv',index=False)

driver.quit()
