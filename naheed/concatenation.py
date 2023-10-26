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
import glob
from datetime import date
location='G:\\PythonLab\\naheed\\*.xlsx'
excel_files=glob.glob(location)
pd.set_option('display.max_rows',264)
df1=pd.DataFrame()
for excel_file in excel_files:
    df2= pd.read_excel(excel_file)
    df1=pd.concat([df1,df2],ignore_index=True) 
df1.to_csv("G:\\PythonLab\\naheed\\Data\\All_Naheed_Data.csv",index=False)
print(df1)