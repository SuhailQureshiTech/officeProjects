
import sys
import os
from suhailLib import ftpUploadDirTransfer,returnDataDate

vFirstDate,vLastDate=returnDataDate(15)

print(vFirstDate.strftime("%d-%b-%Y"))
print(vLastDate.strftime("%d-%b-%Y"))