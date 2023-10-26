
import ftpSftpObject
# import connectionClass

# sftp=ftpSftpObject.ftpTransfer('52.116.35.89',userName='LOPAK_IBL_SFTP',password='heE&j528Hy'
#                 ,localDirectory='D://Google Drive - Office/PythonLab/sftp-Ftp/'
#                 ,remoteDirectory='/distributor_catalogue/'
#                 )
# sftp.sftpFileTransfer()
# # sftp.ftpTransfer()

a=ftpSftpObject.ftpTransfer
# b=connectionClass

class utility(a):
   None

a=utility
b=a.sftpFileTransfer(hostName='52.116.35.89',userName='LOPAK_IBL_SFTP',password='heE&j528Hy'
                ,localDirectory='D://Google Drive - Office/PythonLab/sftp-Ftp/'
                ,remoteDirectory='/distributor_catalogue/'
                )