from sqlite3 import connect
from traceback import print_tb
from bs4 import BeautifulSoup
from openpyxl import load_workbook
import requests
import time 
import pandas as pd
from datetime import date
from openpyxl import Workbook #reading/loading workbooks
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows #appending dataframes to rows

productss=[]
for x in range(1,324):
    url='https://www.naheed.pk/health-beauty?p='
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
            'Category': 'Health & Beauty'
        }
        productss.append( product_info)

        
   
df=pd.DataFrame(productss)
print(df.head())
writer = pd.ExcelWriter('naheed-hb.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
df.to_excel(writer, sheet_name='sheet1', index=False)

# Close the Pandas Excel writer and output the Excel file.
writer.save()



