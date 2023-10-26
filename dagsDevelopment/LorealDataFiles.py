
from datetime import date, datetime, timedelta
from datetime import date
from datetime import datetime,date,timezone

import  cx_Oracle 
import pandas as pd
from sqlalchemy.engine import create_engine
import os
# xo.init_oracle_client(
#     lib_dir=r"C:\oraclexe\app\oracle\product\11.2.0\server\bin")

# os.system('clear')
import sqlalchemy

now=datetime.now()
today=date.today().strftime("%Y%m%d")
current_time = now.time()
currentTime=str(current_time)
timeString=currentTime[0:8].replace(':','')
fileSubString='_CE014_'+today+timeString

DIALECT = 'oracle'
ORACLE_DRIVER = 'cx_oracle'
USERNAME = 'loreal1' #enter your username
PASSWORD = 'Loreal1106' #enter your password
HOST = 'Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com' #enter the oracle db host url
PORT = 6464 # enter the oracle port number
SERVICE = 'cdb1' # enter the oracle db service name
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + ORACLE_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

# engine=create_engine(ENGINE_PATH_WIN_AUTH)

engine = sqlalchemy.create_engine("oracle+cx_oracle://loreal1:Loreal1106@Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com/?service_name=cdb1", arraysize=100)

def lorealSales():
    print('block..........')
    lorealDf = pd.read_sql_query(f'''                                 
                                SELECT
                            DISTRIBUTOR_CODE,
                            TRANSACTION_TYPE,
                            FINAL_CUSTOMER_NUMBER,
                            SKU_DESCRIPTION,
                            DISTRIBUTOR_PROD_CODE,
                            BARCODE,
                            dATE_,
                            DESTINATION_WAREHOUSE,
                            QTY,
                            DISTRIBUTOR_CAT,
                            NET_SALE,
                            CURRENCY_CODE,
                            VENDOR_CODE                        
                        FROM (
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
                            VENDOR_CODE
                                FROM
                                    LOREAL1.loreal_sales x
                                    )
                                WHERE 1=1 AND date_ between '20230902' and '20230910'
                        '''                    
                                , engine)
    print(lorealDf)

    # # Columns
    lorealDf.columns=['Distributor_code','Transaction_type','Final_Customer_Code','Sku_Description','Distributor_Product_Code'
                    ,'Bar_code','Date' ,'Destination_Warehouse','Quantity','Distributor_Cat','Net_Sale_Amount','Currency_Code','Vendor_Code'
                    ]

    # fileName='/home/airflow/Documents/FSLS'+fileSubString+'.txt'
    fileName='FSLS'+fileSubString+'.txt'    
    print('file Name : ',fileName)
    lorealDf.to_csv(fileName,index=False,sep=';')          

# Loreal FCST
def lorealCustomer():
    lorealFcstDf = pd.read_sql_query(f'''
                                select * from LOREAL1.VW_FCST_LOREAL_1
                                '''                    
                                , engine)
    
    fileName='/home/airflow/Documents/FCST'+fileSubString+'.txt'
    print('file Name : ',fileName)

    lorealFcstDf.to_csv(fileName,index=False,sep=';')

# LOREAL FSTK 
def lorealStock():
    lorealFstktDf = pd.read_sql_query(f'''
                                select * from LOREAL1.VW_FSTK_LOREAL_1
                                '''                    
                                , engine)

    fileName='/home/airflow/Documents/FSTK'+fileSubString+'.txt'
    print('file Name : ',fileName)

    lorealFstktDf.to_csv(fileName,index=False,sep=';')

lorealSales()
# lorealCustomer()
# lorealStock()

print('done.....')