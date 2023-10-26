from pyrfc import Connection

import pprint
import json

import pandas as pd


conn = Connection(user='INTGUSER', passwd='123456',
                ashost='10.210.166.202', sysnr='21', client='597')


b_result = conn.call('ZWA_MARKIT_INV_INTG')


#Converting to Json--

jsonData = json.dumps(b_result, sort_keys=True)

print(jsonData)
