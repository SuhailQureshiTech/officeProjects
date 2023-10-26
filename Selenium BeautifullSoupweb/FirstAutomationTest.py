
from selenium import webdriver

# don't need following line, when using webdriver manager, it manage browser versioning
# driver=webdriver.Chrome(executable_path="c:\\BrowserDrivers\\chromedriver_win32\\chromedriver.exe")

from webdriver_manager.chrome import ChromeDriverManager
driver=webdriver.Chrome(ChromeDriverManager().install())

driver.get("https://rcvacademy.com")
driver.maximize_window()

print(driver.title)
driver.close()
