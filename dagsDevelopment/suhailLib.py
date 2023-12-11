# import

import paramiko,os,glob
from pathlib import Path

#  import -- End

class ftpTransfer():
    hostName=None
    userName=None
    password=None
    localDirectory=None
    remoteDirectory=None

    def __init__(self,hostName,userName,password,localDirectory,remoteDirectory):
        self.hostName=hostName
        self.userName=userName
        self.password=password
        self.localDirectory=localDirectory
        self.remoteDirectory=remoteDirectory

    def sftpFileTransfer(self):
        source='d:\\TEMP\\Loreal\\*.*'
        destination='/distributor_catalogue'

        # print('current directory :',os.getcwd())
        # os.chdir('D://Google Drive - Office/PythonLab/sftp-Ftp/')
        os.chdir(self.localDirectory)
        # print('changed directory :',os.getcwd())

        client=paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=self.hostName,username=self.userName,password=self.password
                    ,allow_agent=False,look_for_keys=False,timeout=60)

        sftp=client.open_sftp()
        # sftp.chdir('/distributor_catalogue/')

        sftp.chdir(self.remoteDirectory)
        # print('sftp current directory ',sftp.getcwd())
        print('connection established successfully')

        # files = sftp.listdir('/distributor_catalogue')
        # print(files)
        files = sftp.put(source,source)

        sftp.close()
        client.close()
        print('File transfer completed successfully...')

class ftpTransfer():
    def sftpFileTransfer(hostName,userName,password,localDirectory,remoteDirectory):

        source='name.txt'
        destination='/distributor_catalogue'
        os.chdir(localDirectory)
        try:
            client=paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(hostname=hostName,username=userName,password=password
                        ,allow_agent=False,look_for_keys=False,timeout=60)

            sftp=client.open_sftp()
            sftp.chdir(remoteDirectory)

            # files = sftp.listdir('/distributor_catalogue')
            # print(files)

            files = sftp.put(source,source)
        except Exception as ex:
            print('Exception : ',ex)   
        finally:     
            sftp.close()
            client.close()
            print('File transfer completed successfully...')

class ftpUploadDirTransfer():
    def sftpDirTransfer(hostName,userName,password,localDirectory,remoteDirectory):
        os.chdir(localDirectory) 
        filelist = glob.glob(os.path.join(localDirectory, "*.*"))
        try:
                client=paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(hostname=hostName,username=userName,password=password
                            ,allow_agent=False,look_for_keys=False,timeout=60)

                sftp=client.open_sftp()
                sftp.chdir(remoteDirectory)

                for fileName in filelist:
                    # directory,filename=os.path.split(fileName)
                    # directory, filename = os.path.split(filename)
                    file=Path(fileName).name
                    print(file)
                    
                    files = sftp.put(file,file)

        except Exception as ex:
               print('Exception : ',ex)   
        finally:     
            sftp.close()
            client.close()

        print('File transfer completed successfully...')

def returnDataDate(pDay=1):
    vStartDate=None
    vEndDate=None
    from datetime import date, datetime, timedelta
    from datetime import date
    from datetime import datetime,date,timezone
    from dateutil.relativedelta import relativedelta
    from dateutil import parser

    vTodayDate = datetime.date(datetime.today())
    vTodayDate = int(vTodayDate.strftime("%d"))

    if vTodayDate<=4:   

        # from Previous month to current...
        # print('vtoday :',vTodayDate)
        today = date.today()
        d = today - relativedelta(months=1)
        # print('d ',d)
        vStartDate = date(d.year, d.month, 1)

        import dateutil.relativedelta
        first = today.replace(day=1)
        # vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)
        vEndDate = datetime.date(datetime.today()-timedelta(days=1))

    else:
        if pDay==1:
            print('else')
            vStartDate = datetime.date(datetime.today().replace(day=1))
            vEndDate = datetime.date(datetime.today()-timedelta(days=1))
        else:
            print('new pday : ',pDay)
            vStartDate = datetime.date(datetime.today()-timedelta(days=pDay))
            vEndDate = datetime.date(datetime.today()-timedelta(days=1))



    # print('Start Date : ', vStartDate)
    # print('End   Date : ', vEndDate)

    return vStartDate,vEndDate

def dirList(rootDir):
    dirListData=[]
    rootDir=rootDir
    for dirList in os.scandir(rootDir):
        if dirList.is_dir(): 
        # and dirList.path!='/home/airflow/airflow/logs/scheduler':
            # print(dirList.path)
            dirListData.append(dirList.path)
            # shutil.rmtree(dirList.path)
    return dirListData   
