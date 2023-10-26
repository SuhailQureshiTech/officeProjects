class _scrpWeb:
    def __init__(self, website):
        self.website = website
        driver = webdriver.Chrome(service=Service(
            ChromeDriverManager().install()))

        # open website
        driver.get(website)
        global productNames, productPrice
        productNames1 = []
        productPrice1 = []

        currentPage = 1
        pagination = driver.find_element(
            by=By.XPATH, value=('//div[@class="paginate-bottom"]'))
        pages = pagination.find_elements(by=By.TAG_NAME, value='span')
        lastPage = int(pages[-2].text)

        while currentPage <= lastPage:
            productNames1 = driver.find_elements(by=By.XPATH, value=(
                '//div[@class="grid__item large--seven-tenths right js-de-ProductTilesObserver"]//h4[1]'))

    #             productPrice1=driver.find_elements(by=By.XPATH, value=('//span[@class="js-de-ProductTile-currentPrice"][1]'))

        for pname in productNames1:
            productNames.append(pname.text)

    #             for price in productPrice1:
    #                 productPrice.append(price)

    #             productNames1=None
    #             productPrice1=None

            time.sleep(40)
            currentPage = currentPage+1
        try:
            nextPage = driver.find_element(
                by=By.XPATH, value=('//span[@class="next"]'))
            nextPage.click()
        except:
            pass
    #             finally:
# #                 time.sleep(100)
    driver.close()
