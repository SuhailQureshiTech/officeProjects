# import

import os
import shutil

# import --> End
def getSubDir(rootDir):
    rootDir=rootDir
    for dirList in os.scandir(rootDir):
        if dirList.is_dir() and dirList.path!='/home/airflow/airflow/logs/scheduler':
            print(dirList.path)
            # shutil.rmtree(dirList.path)


getSubDir('/home/airflow/airflow/logs')

