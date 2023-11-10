import cx_Oracle
from sqlalchemy import create_engine
import pandas as pd
import pandas_gbq
from pandas_gbq import schema

from google.cloud import bigquery
from datetime import date, datetime, timedelta
from google.oauth2 import service_account
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from sqlalchemy.orm import sessionmaker

# DIALECT = 'oracle'
# SQL_DRIVER = 'cx_oracle'
# USERNAME = 'IBLGRPHCM' #enter your username
# PASSWORD = 'iblgrp106hcm' #enter your password
# HOST = 'Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com' #enter the oracle db host url
# PORT = 6464 # enter the oracle port number
# SERVICE = 'cdb1' # enter the oracle db service name
# ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE
# engine = create_engine(ENGINE_PATH_WIN_AUTH)

engine = create_engine('oracle+cx_oracle://IBLGRPHCM:iblgrp106hcm@Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com:6464/?service_name=cdb1')

# test_df = pd.read_sql_query('SELECT * FROM cat', engine)
# print(test_df)



Base = declarative_base()
class MAPPER(Base):
    __tablename__ = 'ebs_invoice_data_tbl1' #string

    # bill_dt = Column(String(500), primary_key = True)
    # item_code = Column(String(500))
    bill_dt = Column(String(100), primary_key = True)
    item_code = Column(String(100))
    prod_nm = Column(String(100))
    org_sap_id = Column(String(100))
    org_desc = Column(String(100))
    channel = Column(String(100))
    data_flag = Column(String(100))
    sold_qty=Column(float)
    gross=Column(float)
    unit_selling_price=Column(float)

def oraRec4():
    print('inside......')

    dataFrame1=pd.DataFrame()

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
        )

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    # >={vStartDate}

#        SELECT
        # FORMAT_DATE('%d-%b-%y',BILLING_DATE) BILL_DT,
        # item_code ITEM_CODE,item_desc PROD_NM,
        # ORG_ID ORG_SAP_ID,    ORG_DESC ORG_DESC,
        # CHANNEL_DESC CHANNEL,DATA_FLAG,
        # sum(QUANTITY) SOLD_QTY
        # ,sum(AMOUNT) GROSS,round(IFNULL(unit_selling_price,0))  unit_selling_price
        # FROM `data-light-house-prod.EDW.VW_SEARLE_SALES`
        # WHERE 1=1 and BILLING_DATE  between '2023-08-01' and '2023-08-05'
        #  GROUP BY
        # FORMAT_DATE('%d-%b-%y',BILLING_DATE) ,        item_code ,item_desc ,
        # ORG_ID ,    ORG_DESC ,  CHANNEL_DESC,DATA_FLAG,unit_selling_price
    
    
# SELECT
#         FORMAT_DATE('%d-%b-%y',BILLING_DATE) BILL_DT,
#         item_code ITEM_CODE,item_desc PROD_NM,
#         ORG_ID ORG_SAP_ID,    ORG_DESC ORG_DESC,
#         CHANNEL_DESC CHANNEL,DATA_FLAG,
#         sum(QUANTITY) SOLD_QTY
#         ,sum(AMOUNT) GROSS,round(IFNULL(unit_selling_price,0))  unit_selling_price
#         FROM `data-light-house-prod.EDW.VW_SEARLE_SALES`
#         WHERE 1=1 and BILLING_DATE  between '2023-08-01' and '2023-08-27'
#          GROUP BY
#         FORMAT_DATE('%d-%b-%y',BILLING_DATE) ,        item_code ,item_desc ,
#         ORG_ID ,    ORG_DESC ,  CHANNEL_DESC,DATA_FLAG,unit_selling_price


    sqlQuery = f'''
            SELECT
                FORMAT_DATE('%d-%b-%y',BILLING_DATE) BILL_DT,
                item_code ITEM_CODE,item_desc PROD_NM,
                ORG_ID ORG_SAP_ID,    ORG_DESC ORG_DESC,
                CHANNEL_DESC CHANNEL,DATA_FLAG,
                sum(QUANTITY) SOLD_QTY
                ,sum(AMOUNT) GROSS,round(IFNULL(unit_selling_price,0))  unit_selling_price
                FROM `data-light-house-prod.EDW.VW_SEARLE_SALES`
                WHERE 1=1 and BILLING_DATE  between '2023-08-01' and '2023-08-27'
                GROUP BY
                FORMAT_DATE('%d-%b-%y',BILLING_DATE) ,        item_code ,item_desc ,
                ORG_ID ,    ORG_DESC ,  CHANNEL_DESC,DATA_FLAG,unit_selling_price

              '''
    
    dataFrame1=pd.read_gbq(sqlQuery,credentials=credentials)
    dataFrame1['BILL_DT'] = pd.to_datetime(dataFrame1['BILL_DT'])   
    dataFrame1.columns=dataFrame1.columns.str.lower()
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    print(dataFrame1.info())
    dataFrame1.to_csv("./docs/oracleSearle.csv")
    data = pd.read_csv('./docs/oracleSearle.csv')
    lists = data.to_dict(orient='records')
    table = sqlalchemy.Table('ebs_invoice_data_tbl1', Base.metadata, autoreload=True)
    engine.execute(table.insert(), lists)
    session.commit()
    session.close()

    # dataFrame1.to_sql(name='ebs_invoice_data_tbl1'
    #                   ,schema='IBLGRPHCM'
    #                   ,con=engine,if_exists='append',index=False)
    
    print('done............')
    
    
oraRec4()