
from pyrfc import Connection
import  pprint,json
import pandas as pd


# Working One
# conn = Connection(user='INTGUSER', passwd='123456',
#             ashost='10.210.166.202', sysnr='21', client='590')
# print('Connection : ',conn)

conn = Connection(user='INTGUSER', passwd='Abc@12345',
                    ashost='10.210.150.201', sysnr='11', client='150')
# print('Connection : ', conn)

b_result = conn.call('ZEMP_INFO')

#pretty printer --
# pprint.pprint(b_result)

#Converting to Json--
jsonData=json.dumps(b_result,sort_keys=True)
print(jsonData)

# Converting to DataFrame--
# df=pd.DataFrame(b_result)
# print(df.head())



