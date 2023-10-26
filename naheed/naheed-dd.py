from sqlite3 import connect
from traceback import print_tb
from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
from datetime import date

productss=[]
for x in range(1,10):
    url='https://www.naheed.pk/deal-of-the-day?p='
    r= requests.get(url+str(x))  
    
    soup = BeautifulSoup(r.content,'html.parser')
    Products=soup.find_all('div',class_="product-item-info per-product category-products-grid")
        
    for product in Products:
        product_name= product.find('h2',class_="product name product-name product-item-name").text
        
        product_price= product.find('span',class_="price").text.replace('  ','')
        product_info = {
            'Product_Name':product_name,
            'Product_Price':product_price,
            'Date': date.today(),
            'Category': "Deals Of The Day"
        }
        productss.append( product_info)

        
   
df=pd.DataFrame(productss)
print(df.head())
writer = pd.ExcelWriter('naheed-dd.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='sheet1', index=False)
writer.save()

