import requests as req
# from pandas import json_normalize,DataFrame as df
import pandas as pd
import time
import os
from datetime import date, datetime, timedelta
import os
import connectionClass
from sqlalchemy import create_engine

os.system('cls')

vStartDate=None
vEndDate=None

conClass = connectionClass
sapConnString=conClass.sapConnAlchemy()
pioneerEngine = conClass.pioneerSqlAlchmy()

sapConnEngine=create_engine(sapConnString)

today = date.today()
vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
creationDate = datetime.today()

def getDate():
    global vStartDate, vEndDate
    if vTodayDate <= 5:
        from dateutil.relativedelta import relativedelta
        print('if block')
        from dateutil.relativedelta import relativedelta
        d = today - relativedelta(months=2)
        vStartDate = date(d.year, d.month, 1)
        # vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
        vStartDate = vStartDate
        vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)

    else:

        vStartDate = datetime.date(datetime.today().replace(day=1))
        vEndDate = datetime.date(datetime.today()-timedelta(days=0))
        
    print('else : from date :', vStartDate)
    print('else : enmd date :', vEndDate)



def truncatePeriodData():    
    global vStartDate,vEndDate
    
    # vStartDate1=vStartDate
    # vEndDate1=vEndDate
    
    # vStartDate1='20230901'
    # vEndDate1='20231023'

    # vStartDate1="'"+str(vStartDate1.strftime('%Y-%m-%d'))+"'"
    # vEndDate1 = "'"+str(vEndDate1.strftime('%Y-%m-%d'))+"'"
   
    delSql = f'''
                delete from pioneer_schema.pioneer_attendance 
                where cast(punch_datetime as date) between {vStartDate1} and {vEndDate1}
                '''
    # print(delSql)
    pioneerEngine.execute(delSql)

    vStartDate1 = vStartDate
    vEndDate1 = vEndDate

    vStartDate1 = "'"+str(vStartDate1.strftime('%Y%m%d'))+"'"
    vEndDate1 = "'"+str(vEndDate1.strftime('%Y%m%d'))+"'"

    delSql = f'''
               DELETE FROM sapabap1.ZTMPOR 
               WHERE DATE1  between {vStartDate1} and {vEndDate1}
                '''

    sapConnEngine.execute(delSql)
    
def getBulkRecords():

    # urlText=f'''http://pioneerattendance.com:94/api/EmployeeData/DateRange/{vStartDate}/{vEndDate}/'''
    urlText=f'''http://pioneerattendance.com:94/api/EmployeeData/DateRange/2023-09-01/2023-10-23/'''

    api_url=urlText
    response = req.get(url=api_url)
    
    r = response.json()
    df = pd.DataFrame.from_dict(r)

    # os.chdir('d:\\Google Drive - Office\\PythonLab\\PoineerAttendance\\')
    # print('working dir ',os.getcwd())
    df.to_csv('bulkRecords.csv',index=False)

    df = df.rename(
        columns={'No': 'transaction_id', 'Employee ID': 'employee_id', 'PunchDatetime': 'punch_datetime'
                 ,'Device No': 'device_no', 'Status': 'status'
                 }
    )

    df['record_datetime'] = creationDate
    df.to_sql('pioneer_attendance',
              schema='pioneer_schema',
              con=pioneerEngine,
              index=False,
              if_exists='append'
              )

    attnSql = 'SELECT * FROM pioneer_schema.vw_attendance_inout_rec'
    attendanceDf=pd.read_sql(attnSql,pioneerEngine)
    attendanceDf.insert(0, 'mandt', '300')

    print(df.info())
    print(df)

    # print(attendanceDf.info())

    print(attendanceDf)    
    print(attendanceDf.info())
    
    # attendanceDf['mandt'].astype('str')
    # attendanceDf['tmid'].astype('str')
    # attendanceDf['cardno'].astype('str')
    # attendanceDf['date1'].astype('str')
    # attendanceDf['p_day'].astype('str')
    # attendanceDf['ismanual'].astype('str')
    # attendanceDf['machine'].astype('str')
    # attendanceDf['time'].astype('str')
    # attendanceDf['inout1'].astype('str')
    # # attendanceDf['flag'].astype('str')
    
    attendanceDf.to_sql('ztmpor',
              schema='sapabap1',
              con=sapConnEngine,
              index=False,
              if_exists='append'
              )

getDate()

# truncatePeriodData()
getBulkRecords()
