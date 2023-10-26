# import

import os
import shutil


# import --> End
def getSubDir(rootDir):
    dirListData=[]
    rootDir=rootDir
    for dirList in os.scandir(rootDir):
        if dirList.is_dir() and dirList.path!='/home/airflow/airflow/logs/scheduler':
            # print(dirList.path)
            dirListData.append(dirList.path)
            # shutil.rmtree(dirList.path)
    return dirListData        


getListDir=getSubDir('/home/airflow/airflow/logs')
for x in getListDir:
    print(x)
    print('----------')
    getSubDirList=getSubDir(x)
    for y in getSubDirList:
        print(y)
        shutil.rmtree(y)

