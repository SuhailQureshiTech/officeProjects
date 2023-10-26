import os
import glob
from datetime import date,datetime,timedelta
import pypyodbc as odbc
import pandas as pd
import sqlalchemy as sa
import urllib
import math
global df

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


df = pd.read_csv(
                    r'\\192.168.130.78\f\Qlik\Extraction\United Brands\VBKD.csv',
                    usecols=[
                        'MANDT','VBELN','FKDAT','BSTKD'
                    ]
                    ,low_memory=False
                )

# print(df.info())

# def dataCleaning():
#     df.columns = df.columns.str.strip()

# print(df)
# print(df.dtypes)

def insertDataRecords():

    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                    "SERVER=192.168.130.81\sqldw;"
                                    "DATABASE=ibl_dw;"
                                    "UID=pbironew;"
                                    "PWD=pbiro345-")

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params) 
      ,fast_executemany=True)
    # print(engine)
    # print(df)

    sql = "DELETE  FROM VBKD"
    result = engine.execute(sql)

    df_num_of_cols = len(df.columns)
    chunknum = math.floor(1000/df_num_of_cols)

    df.to_sql('VBKD',
            schema='dbo',
            con=engine,
            chunksize=chunknum,
            method='multi',
            index=False,
            if_exists='append',

            )

insertDataRecords()