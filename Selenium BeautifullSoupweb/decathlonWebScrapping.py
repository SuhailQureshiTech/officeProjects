#review package and module
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
# import chromedriver_autoinstaller
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException,NoSuchFrameException,StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
import pandas as pd
import os
import time

productNames=[]
productPrice=[]
subCategory=[]
imageSrc=[]
# driver=None

class _scrpWebWait:
    global productNames, productPrice,imageSrc
#     ,driver
    def __init__(self, website):
        self.website = website
        driver = webdriver.Chrome(service=Service(
            ChromeDriverManager().install()))

        # open website
        driver.get(website)
        productNames1 = []
        productPrice1 = []
        imageSrc1=[]

        currentPage = 0
        pagination = driver.find_element(
            by=By.XPATH, value=('//div[@class="paginate-bottom"]'))
        pages = pagination.find_elements(by=By.TAG_NAME, value='span')
        lastPage = int(pages[-2].text)

        while currentPage<=lastPage:

            imageSrc1 =WebDriverWait(driver, 60).until(
                EC.presence_of_all_elements_located((By.XPATH
                        , '//div[@class="de-productTile-showcaseImageContainer"]//img[1]')))

            for pImage in imageSrc1:
                imageSrc.append(pImage.get_attribute("data-src"))

            productNames1 =WebDriverWait(driver, 60).until(
                EC.presence_of_all_elements_located((By.XPATH
                        ,'//div[@class="grid__item large--seven-tenths right js-de-ProductTilesObserver"]//h4[1]')))

            for pname in productNames1:
                productNames.append(pname.text)

            productPrice1=WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH
                                    ,'//span[@class="js-de-ProductTile-currentPrice"][1]')))

            for price in productPrice1:
                productPrice.append(price.text)

            time.sleep(60)
            currentPage=currentPage+1
            try:
                productNames1 = []
                productPrice1 = []
                imageSrc1=[]
                nextPage=WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//span[@class="next"]')))
#                 driver.find_element(by=By.XPATH, value=('//span[@class="next"]'))

                nextPage.click()
            except TimeoutException:
                pass
        driver.close()

def extrctData():
    global productNames, productPrice, driver, imageSrc
    productsDict={}
    productNames=[]
    productPrice=[]
    imageSrc=[]
    subCategory=[]

    subCategory=[
    'https://www.decathlon.com/collections/bikes-cycling-deals'
    ]

    for subCat in range(0,len(subCategory)):

        webPage=subCategory[subCat]
        category=webPage.replace('https://www.decathlon.com/collections/','')
        a=_scrpWebWait(webPage)

        # creating DataFrame
        product_list=[]
        for p in productNames:
            product_list.append(p)

        product_price=[]
        for pr in productPrice:
            product_price.append(pr)

        product_image=[]
        for pim in imageSrc:
            product_image.append(pim)

#         productsDict={'Product':product_list,'Product_Price':product_price}


        productsDict={'Product':product_list,'Product_Price':product_price,'Product_Image':product_image}
        df=pd.DataFrame.from_dict(productsDict)
        print(df)

    #     df.insert(loc=0,column='Category',value=category)
    #     df.insert(loc=0,column='Head',value='Bikes & Cycling')

    #     df
    #     checkFolder('webscrapping')
    #     df.to_csv(f'c://WebScrapping1/{category}.csv',index=False)
        df.to_csv(f'c://WebScrapping1/Bikes & Cycling.csv',index=False)
    #     driver.close()

extrctData()
