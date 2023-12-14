import requests as req
# from pandas import json_normalize,DataFrame as df
import pandas as pd
import time
import os
from datetime import date, datetime, timedelta
 

# import connectionClass
# conClass = connectionClass
# pioneerEngine = conClass.pioneerSqlAlchmy()
# attendance66=conClass.attendanceMachine66()

 

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
creationDate = datetime.today() 

def getBulkRecords():

    api_url = 'http://pioneerattendance.com:94/api/EmployeeData/DateRange/2023-07-01/2023-12-14'
    response = req.get(url=api_url
                       )
    r = response.json()
    df = pd.DataFrame.from_dict(r)
    df = df.rename(
        columns={'No': 'transaction_id', 'Employee ID': 'employee_id', 'PunchDatetime': 'punch_datetime'
                 ,'Device No': 'device_no', 'Status': 'status'
                 }
                )

 
    df['record_datetime'] = creationDate

    # os.chdir('d:\\Google Drive - Office\\PythonLab\\PoineerAttendance\\')

    print('working dir ',os.getcwd())

    df.to_csv('bulkRecordsAPI.csv',index=False)

    # df.to_sql('attendance_records',
    #           schema='pioneer_schema',
    #           con=attendance66,
    #           index=False,
    #           if_exists='append'
    #           )

 

getBulkRecords()

 