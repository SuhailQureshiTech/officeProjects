import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)
from IBLOps import LogMergingTrans
from Email import smtpEmailObjectHtml
from typing import Iterator
import pypyodbc as odbc
import pandas as pd
import time
import datetime
import cx_Oracle as xo
from datetime import date, datetime, timedelta

from pandas.core.frame import DataFrame
from pandas.core.reshape.concat import concat



global connectionDB, sqlConnection, records, vStartDate, numberOfRecords
global oracleServerAdd, oraclePort, oracleServiceName, oracleUserName, oraclePassword
oracleConnectionDB = None
sqlConnection = None
records = None
sqlQuery = None
cursor = None
oracleCursor = None
dataFrame1 = pd.DataFrame()
dataFrame2 = pd.DataFrame()
vday = 1
numberOfRecords = 0
total = 0
# vPath = r"\\192.168.130.81\\Searle\\"
vPath="D:\\TEMP\\SEARLEDATAEXT\\"

vTodayDate = datetime.date(datetime.today())
vTodayDate=int( vTodayDate.strftime("%d"))
print(type(vTodayDate))

print(vTodayDate)
if  vTodayDate==2:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=2))
    vdayDiff = int(vEndDate.strftime("%d"))+1
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

else:

    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print('Start Date : ', vStartDate)
print('End   Date : ', vEndDate)

