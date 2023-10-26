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

web = 'https://twitter.com/search?q=python&src=recent_search_click'
driver = webdriver.Chrome(service=Service(
    ChromeDriverManager().install()))

# add the "options" argument to make sure the changes are applied
driver.get(web)
# driver.maximize_window()

tweets = driver.find_elements(By.XPATH, '//article[@role="article"]')
for tweet in tweets:


