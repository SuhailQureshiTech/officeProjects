
from numpy import blackman
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

# Calling BAPI--
b_result = conn.call('ZEMP_INFO')

#pretty printer --
# pprint.pprint(b_result)

response = conn.call(
    'RFC_READ_TABLE',
    QUERY_TABLE='GT_EMP_INFORMATION',
    DELIMITER=','
    # FIELDS=fields,
    # OPTIONS=options,
    # ROWCOUNT=rowcount,
    # ROWSKIPS=rowskips,
)


# print(b_result.get('BUKRS'))

# Loop in dictionary
# for keyList, value in b_result.items():
#     print (value)

# for items in b_result:
#     print(items)

# Checking result type--
# print('Object Type',type(b_result))



#Converting to Json--
# jsonData=json.dumps(b_result,sort_keys=True)
# print(jsonData)

# Converting to DataFrame--
# df=pd.DataFrame(b_result)8
# print(df)



