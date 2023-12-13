
import urllib
from sqlalchemy import create_engine
import pyodbc 
import pandas as pd
import sqlalchemy

server = '192.168.130.66'
db = 'iblgrp'
user = 'sa'
password =urllib.parse.quote_plus ('abc@123')
schema = 'dbo'
driver='ODBC Driver 18 for SQL Server'

constring = f"mssql+pyodbc://{user}:{password}@{server}/{db}?driver={driver}"
print (f"Connection String: {constring}")

dbEngine = sqlalchemy.create_engine(constring, fast_executemany=True, connect_args={'connect_timeout': 10, 'TrustServerCertificate': 'yes'}, echo=False) 
engine=dbEngine.connect()
sqlGetRec=f'''
        SELECT
            300 mandt,Tran_MachineRawPunchId ,CardNo ,cast(Dateime1  as date)Date1,P_Day,ISManual            
        from Tran_MachineRawPunch trn
        where 1=1 and cast(PunchDatetime as date) ='2023-12-10'

        '''
franchiseDf=pd.read_sql(sqlGetRec,con=engine)
print(franchiseDf)

# try:
#     with dbEngine.connect() as con:
#         # con.execute("SELECT * from tran_machinerawpunch")
#             sqlGetRec=f'''
#             SELECT
#                 300 mandt,Tran_MachineRawPunchId ,CardNo ,cast(Dateime1  as date)Date1,P_Day,ISManual            
#             from Tran_MachineRawPunch trn
#             where 1=1 and cast(PunchDatetime as date) ='2023-12-10'
    
#             '''
#             franchiseDf=pd.read_sql(sqlGetRec,con=con)
#             print(franchiseDf)
# except Exception as e:
#     print(f'Engine invalid: {str(e)}')