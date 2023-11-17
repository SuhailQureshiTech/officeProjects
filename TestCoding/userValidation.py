from hashing import Hash
from connectionClass import FranchiseAlchmy
import pandas as pd

newPassword='ibl@12345'
# hashNewPassword="'"+str(Hash.passwordHash(newPassword))+"'"

hashNewPassword=Hash.passwordHash(newPassword)


print('new password :',newPassword)
print('hash password : ',hashNewPassword)

# franchiseEngine=FranchiseAlchmy()

# qryGetUser=f'''select * from users
#             where 1=1 and distributor_id ='2000090617' 
#  '''

# # print(franchiseEngine)
# df=pd.DataFrame()
# df= pd.read_sql_query(qryGetUser,con=franchiseEngine)
# print('data frame...........')
# userPassword=str(df['password'].values[0])
# print('user password........',userPassword)


# if Hash.verifyPassword(userPassword,'ibl@12345')==True:
#     qryUpdateUser=f'''
#                 update users set password={hashNewPassword}
#                 where 1=1 and distributor_id='2000090617'
#     '''
#     franchiseEngine.execute(qryUpdateUser)
#     print('hoya....')

# franchiseEngine=None


