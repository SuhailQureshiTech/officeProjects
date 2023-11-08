import requests as req
# from pandas import json_normalize,DataFrame as df
import pandas as pd
import time
import os
from datetime import date, datetime, timedelta
import urllib
import sqlalchemy
import connectionClass

# from sqlalchemy.dialects.mssql.pymssql import MSDialect_pymssql


conClass = connectionClass
pioneerEngine = conClass.pioneerSqlAlchmy()
attendance66=conClass.attendanceMachine66()

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
creationDate = datetime.today()

# server = '192.168.130.66'
# db = 'iblgrp'
# user = 'sa'
# password =urllib.parse.quote_plus ('abc@123')
# schema = 'dbo'
# connect_string = f'''mssql+pymssql://{user}:{password}@{server}:1433/{db}'''
# engine = sqlalchemy.create_engine(connect_string)

def getBulkRecords():
    api_url = 'http://pioneerattendance.com:94/api/EmployeeData/DateRange/2023-11-01/2023-11-06'
    # 
    # print(api_url)
    response = req.get(url=api_url)
    r = response.json()
    df = pd.DataFrame.from_dict(r)

    # print(df)

    # print('working dir ',os.getcwd())
    # os.chdir('d:\\Google Drive - Office\\PythonLab\\PoineerAttendance\\')
    # print('working dir ',os.getcwd())
    # df.to_csv('bulkRecords.csv',index=False)

    df = df.rename(
        columns={'No': 'transaction_id', 'Employee ID': 'employee_id', 'PunchDatetime': 'punch_datetime'
                 ,'Device No': 'device_no', 'Status': 'status'
                 }
    )

    df['dateKey']=pd.to_datetime(df['punch_datetime']).dt.date
    df['dateKey']=pd.to_datetime(df['dateKey'],format='%y%m%d').dt.date.astype('str')
    
    df['dateKey']=df['dateKey'].str.replace('-','')
    df['rec_key']=df['employee_id'].astype(str)+ df['dateKey'].astype(str)+df['device_no'].astype(str)+df['status'].astype(str)


    df.drop(['dateKey'],axis=1,inplace=True)

    print(df.info())
    # print(df['rec_key'])
    
    df['record_datetime'] = creationDate
    df.to_sql('attendance_records',
              schema='dbo',
              con=attendance66,
              index=False,
              if_exists='replace'
              )

getBulkRecords()
