from hashing import Hash
from connectionClass import FranchiseAlchmy
import pandas as pd


def hashingPassword():
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

def report():

    franchiseEngine=FranchiseAlchmy()

    qryGetUser=f'''select 
        franchise_customer_invoice_date
           from franchise."franchise_sales" fs2 where franchise_code = '2000226180' 
            and date(fs2.franchise_customer_invoice_date) between '2023-11-01' and '2023-11-18'
     '''

    # # print(franchiseEngine)
    df=pd.DataFrame()
    df= pd.read_sql_query(qryGetUser,con=franchiseEngine)
    print('data frame...........')
    df.to_dict(orient='records')
    # print(df)

# hashingPassword()
report()