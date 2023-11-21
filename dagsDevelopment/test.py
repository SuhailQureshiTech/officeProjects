
import sys
import os
from suhailLib import ftpUploadDirTransfer,returnDataDate

vFirstDate,vLastDate=returnDataDate()

print(vFirstDate.strftime("%d-%b-%Y"))
print(vLastDate.strftime("%d-%b-%Y"))