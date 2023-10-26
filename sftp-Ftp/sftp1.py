
import paramiko,os

source='name.txt'
destination='/distributor_catalogue'

print('current directory :',os.getcwd())
os.chdir('D://Google Drive - Office/PythonLab/sftp-Ftp/')
print('changed directory :',os.getcwd())


client=paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname='52.116.35.89',username='LOPAK_IBL_SFTP'
               ,password='heE&j528Hy'
            ,allow_agent=False,look_for_keys=False)

sftp=client.open_sftp()
sftp.chdir('/distributor_catalogue/')

print('sftp current directory ',sftp.getcwd())

print('connection established successfully')


# files = sftp.listdir('/distributor_catalogue')
# print(files)
files = sftp.put(source,source)

sftp.close()
client.close()


# sftp.chdir(r"d:\\Google Drive - Office\PythonLab\sftp-Ftp\\")

# sftp.put(r"d:\\Google Drive - Office\PythonLab\sftp-Ftp\\name.txt"
#         ,destination)
# files = sftp.listdir('/distributor_catalogue')
# for f in files:
#     print(f)

# all_files_in_path = os.listdir(r"d:\\Google Drive - Office\PythonLab\sftp-Ftp\\")
# print(all_files_in_path)
