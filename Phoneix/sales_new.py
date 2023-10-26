import os
import glob
from datetime import date,datetime,timedelta
import pydoc
import pypyodbc as odbc
import pandas as pd
import sqlalchemy as sa
import urllib
import math
global df,df1,cursor_Phoneix

os.system('cls')
today = date.today()
# today = date.today()
d6 = today.strftime("%d-%b-%Y")
print('d6 :',d6)

# Month abbreviation, day and year   11-Sep-21
d5 = today.strftime("%d-%b-%Y")
vEndDate = datetime.date(datetime.today()-timedelta(days=2)).strftime("%Y%m%d")
vEndDate = "'"+vEndDate+"'"
print('end date : ',vEndDate)

# pd.read_csv(
#     r'D:\Google Drive - Office\PythonLab\Usefull_Lib\Employee.csv').query(
#             'joindate=='+"'"+d3+"'" +" & empName=="+"'"+vempName+"'"
                                        
#                                 )

#  r'\\192.168.130.78\f\Qlik\Extraction\United Brands\SALES_DATA.csv',
#  r'\\D:\\temp\\tSALES_DATA.csv',
df = pd.read_csv(
                   r'\\192.168.130.78\f\Qlik\Extraction\United Brands\SALES_DATA.csv'
                    ,usecols=[
                        'ITEM_CATEGORY',
                        'BILLING_TYPE']
                   ,low_memory=False )
df1 = pd.DataFrame(df)   

   # Changing Type
# df["ITEM_CATEGORY"] = df["ITEM_CATEGORY"].astype(str)
# df["BILLING_TYPE"] = df["BILLING_TYPE"].astype(str)
# df["UP_KEY"] = df["UP_KEY"].astype(str)
# df["DOCUMENT_NO"] = df["DOCUMENT_NO"].astype(str)
# df["CANCELLED_FLAG"] = df["CANCELLED_FLAG"].astype(str)
# df["COMPANY_CODE"] = df["COMPANY_CODE"].astype(str)
# df["DOCUMENT_CONDITION"] = df["DOCUMENT_CONDITION"].astype(str)
# df["CHANNEL"] = df["CHANNEL"].astype(str)
# df["CUSTOMER_CODE"] = df["CUSTOMER_CODE"].astype(str)


# print(df.info())
# print(df.head())
# def dataCleaning():
#     df.columns = df.columns.str.strip()

# print(df)
# print(df.dtypes)

def insertDataRecords():

      sqlConnectionPhoneix = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                        "Server=192.168.130.81\sqldw;"
                                        "Database=ibl_dw    ;"
                                        "uid=pbironew;pwd=pbiro345-")

      cursor_Phoneix = sqlConnectionPhoneix.cursor()
      deleteStatement =f'''
                            delete from PHNX_SALES_DETAIL_DATA
                        '''

      cursor_Phoneix.execute(deleteStatement)
      cursor_Phoneix.commit()
      print('delete done')

      db_table_nm='PHNX_SALES_DETAIL_DATA'
      qry = "BULK INSERT " + db_table_nm + " FROM '" +  r'\\192.168.130.78\f\Qlik\Extraction\United Brands\SALES_DATA.csv' + "' WITH (FORMAT = 'CSV', FIRSTROW = 2)"

      cursor_Phoneix.execute(qry)
      sqlConnectionPhoneix.commit()
      cursor_Phoneix.close()
      
insertDataRecords()
print('done........')