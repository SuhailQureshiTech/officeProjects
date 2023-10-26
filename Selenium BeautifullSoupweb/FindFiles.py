import os.path
import time
from datetime import date, datetime, timedelta
import glob
import re
import pandas as pd

totalFiles=0
allFiles=[]
path = 'c:\\webscrapping\\'
os.system('cls')
# todayDate = datetime.datetime.today()  # gets current time
os.chdir(path)  # changing path to current path(same as cd command)

#we are taking current folder, directory and files
#separetly using os.walk function
for root, directories, files in os.walk(path, topdown=False):
    # global totalFiles
    # totalFiles=0
    for name in files:
        # fileName = str(name.lower())
        # x = re.findall("[^naheed]",fileName)
        # if x:
        #     print(fileName)
        #     totalFiles=totalFiles+1

        if  not name.lower().__contains__('naheed'):
            fileName = str(name.lower())
            totalFiles = totalFiles+1
            allFiles.append(fileName)
            # print(fileName )


    print((totalFiles))

df_from_each_file=(pd.read_csv(f,sep=",") for f in allFiles)
df_merged=pd.concat(df_from_each_file,ignore_index=True)
df_merged.to_csv('merged.csv')

# for f in allFiles:
#     print(f)