#review package and module
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
# import chromedriver_autoinstaller
from selenium.webdriver.chrome.service import Service
import time

#relevant website
website_1='https://www.geekbuying.com/searchword/laptop/'

#initialize chrome
# driver=webdriver.Chrome(executable_path=ChromeDriverManager().install())

# chromedriver_autoinstaller.install()
# driver = webdriver.Chrome(service=Service())

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

#open website
driver.get(website_1)
driver.maximize_window()
price = driver.find_elements(by=By.XPATH, value=(
    '(//li[@class="searchResultItem"]/div/div[3])[1]'))

print(price.text)