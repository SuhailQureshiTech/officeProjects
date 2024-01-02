
# import datetime
from datetime import date, datetime,timedelta,timezone
import sys
import os
from suhailLib import ftpUploadDirTransfer,returnDataDate

# vFirstDate,vLastDate=returnDataDate(15)
# print(vFirstDate.strftime("%d-%b-%Y"))
# print(vLastDate.strftime("%d-%b-%Y"))

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
creationDate = date + timedelta(hours=5)
print(creationDate)