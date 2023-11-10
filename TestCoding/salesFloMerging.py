
import  cx_Oracle as xo
import  oracledb
import pandas as pd
# from sqlalchemy.engine import create_engine
from sqlalchemy import create_engine, inspect
import sys

xo.init_oracle_client(
    lib_dir=r"C:\oraclexe\app\oracle\product\11.2.0\server\bin")

DIALECT = 'oracle'
SQL_DRIVER = 'cx_oracle'
USERNAME = 'loreal1' #enter your username
PASSWORD = 'Loreal1106' #enter your password
HOST = 'Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com' #enter the oracle db host url
PORT = 6464 # enter the oracle port number
SERVICE = 'cdb1' # enter the oracle db service name
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

try:
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    test_df = pd.read_sql_query(f'''
                    SELECT
                        DISTRIBUTOR_CODE,
                        TRANSACTION_TYPE,
                        FINAL_CUSTOMER_NUMBER,
                        SKU_DESCRIPTION,
                        DISTRIBUTOR_PROD_CODE,
                        BARCODE,
                        date_,
                        DESTINATION_WAREHOUSE,
                        QTY,
                        DISTRIBUTOR_CAT,
                        NET_SALE,
                        CURRENCY_CODE,
                        VENDOR_CODE,
                        DISCOUNT_VALUE
                    FROM loreal_sales
                    WHERE 1=1 AND Date_='20230902'
                            '''                    
                            , engine)
    print(test_df)

except  Exception as ex:
    print('ex : ',ex)

