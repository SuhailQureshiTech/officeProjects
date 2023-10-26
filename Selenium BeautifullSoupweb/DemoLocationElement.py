
import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

class DemoFindElementById():
    def Locate_by_id_demo(self):
        driver=webdriver.Chrome(executable_path=ChromeDriverManager().install())
        driver.get("https://secure.yatra.com/social/common/yatra/signin.htm")
        driver.find_element_by_id("login-input").send_keys("test@test.com")
        time.sleep(4)

findbyid=DemoFindElementById()
findbyid.Locate_by_id_demo()
