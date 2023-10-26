from datetime import datetime
from datetime import date, datetime, timedelta
from typing import Iterator
import cx_Oracle as xo
import pypyodbc as odbc
import pandas as pd
import time
import datetime

import sys
import os
import inspect
import pypyodbc as odbc

global connectionDB, sqlConnection, records, vStartDate, numberOfRecords
global oracleServerAdd, oraclePort, oracleServiceName, oracleUserName, oraclePassword
oracleConnectionDB = None

global sqlConnectionCandela
global sqlConnectionHabitt

records = None
global cursor_candela
global cursor_Habitt

oracleCursor = None
vday = 1
numberOfRecords = 0
total = 0
vStartDate = date.today()
vStartDate = vStartDate-timedelta(days=10)

# vStartDate = vStartDate.strftime('%Y%m%d')
vStartDate1 = date.today()
vStartDate1 = vStartDate1-timedelta(days=1)

# vStartDate = datetime.now().date().replace(day=1).strftime('%Y%m%d')

# vStartDate = "'"+str(vStartDate)+"'"

# print(vStartDate)
print(vStartDate)
print(vStartDate1)


