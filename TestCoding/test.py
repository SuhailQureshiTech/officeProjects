
import sys
import os
from suhailLib import ftpTransfer,ftpUploadDirTransfer


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
print('scrpt ',SCRIPT_DIR)
sys.path.append(os.path.dirname(SCRIPT_DIR))

ftpUploadDirTransfer.sftpDirTransfer(hostName='52.116.35.89',userName='LOPAK_IBL_SFTP',password='heE&j528Hy'
                ,localDirectory='D:\\TEMP\\Loreal\\'
                ,remoteDirectory='/'
                )
