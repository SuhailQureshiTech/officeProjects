
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd 

print('connecting to SAP ....')

hdb_user = 'ETL'
hdb_password = 'EtlIbl12345'

# --private ip
# hdb_host = '10.210.134.43' 
hdb_host='54.179.85.93'
hdb_port = '33015'
connection_string ='hana://%s:%s@%s:%s/?encrypt=true&sslvalidatecertificate=false' % (
    hdb_user, hdb_password, hdb_host, hdb_port)
engine=sqlalchemy.create_engine(connection_string)

sql=f'''
    select *  from sap_customer_franchise
'''

df=pd.read_sql(sql,con=engine)
print(df)

# def getSapCustomers():
#     print('hello customers...')