
import paramiko,os

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
        source='name.txt'
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

        client=paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=hostName,username=userName,password=password
                    ,allow_agent=False,look_for_keys=False,timeout=60)

        sftp=client.open_sftp()
        sftp.chdir(remoteDirectory)

        # files = sftp.listdir('/distributor_catalogue')
        # print(files)

        files = sftp.put(source,source)
        sftp.close()
        client.close()
        print('File transfer completed successfully...')


# sftp.chdir(r"d:\\Google Drive - Office\PythonLab\sftp-Ftp\\")

# sftp.put(r"d:\\Google Drive - Office\PythonLab\sftp-Ftp\\name.txt"
#         ,destination)
# files = sftp.listdir('/distributor_catalogue')
# for f in files:
#     print(f)

# all_files_in_path = os.listdir(r"d:\\Google Drive - Office\PythonLab\sftp-Ftp\\")
# print(all_files_in_path)
