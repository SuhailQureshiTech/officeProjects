import pyodbc
from sqlalchemy import create_engine
import pandas as pd
import cx_Oracle

cx_Oracle.init_oracle_client(
    lib_dir=r"C:\oraclexe\app\oracle\product\11.2.0\server\bin")


server = '192.168.130.81\sqldw'
database = 'ibl_dw'
username = 'pbironew'
password = 'pbiro345-'
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server +
                    ';DATABASE='+database+';UID='+username+';PWD=' + password)
cursor = cnxn.cursor()
query = "SELECT ITEM_CODE FROM  SEARLE_SALES ST;"
df = pd.read_sql(query, cnxn)


# you do data manipulation that is needed here
# then insert data into oracle


conn = create_engine(
    'oracle+cx_oracle://iblgrphcm:HRAPPS1406@196.16.16.106:1521/?service_name=sir')
conn.connect()

df.to_sql('EBS_INV_DATA_TBL', conn, index=False, if_exists="replace")
