# Importing Libraries
import pyodbc
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
import cx_Oracle
from dateutil import parser
# cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\muhammad.arslan\Downloads\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3")
from datetime import datetime,timedelta

from sqlalchemy import column

# Declare and Intialize Variables IN Python
date_time =str(datetime.now().date().replace(day=1))
FirstDayofMonth = parser.parse(date_time)
date_time_minus1 =str(datetime.now().date() + timedelta(days=-1))
day_minus_1 = parser.parse(date_time_minus1)


# Creating connection with SQL SERVER
# server = '192.168.130.81\sqldw' 
# database = 'ibl_dw' 
# username = 'pbironew' 
# password = 'pbiro345-' 
# conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

# # Incremental Load Query with date check
# extract_data = f''' SELECT INSTITUTION,
#                                 BRANCH_ID,
#                                 BRANCH_NAME,
#                                 DISTRIBUTOR,
#                                 INS_TYPE,
#                                 PRODUCT,
#                                 ITEM_CODE,
#                                 SKU,
#                                 SELLING_PRICE,
#                                 CLAIMABLE_DISCOUNT,
#                                 UN_CLAIMABLE_DISCOUNT,
#                                 INST_DISCOUNT,
#                                 [Month],
#                                 ORDER_REF_NO,
#                                 DATE_OF_ORDER,
#                                 ORDER_QUANTITY,
#                                 FOC,
#                                 TOTAL_QTY,
#                                 SALES_VLAUE,
#                                 NET_SALE_VALUE,
#                                 INVOICE_DATE_IBL,
#                                 INVOICE_NO_IBL,
#                                 CUSTOMER_TRX_ID
#                 FROM EBS_INVOICE_ORDER_VW 
#                 where INVOICE_DATE_IBL>='{FirstDayofMonth}' and INVOICE_DATE_IBL<='{day_minus_1}' '''
# sql_query = pd.read_sql_query(extract_data, conn)


# First time load query - Full load
# sql_query = pd.read_sql_query(""" SELECT INSTITUTION,
#                                 BRANCH_ID,
#                                 BRANCH_NAME,
#                                 DISTRIBUTOR,
#                                 INS_TYPE,
#                                 PRODUCT,
#                                 ITEM_CODE,
#                                 SKU,
#                                 SELLING_PRICE,
#                                 CLAIMABLE_DISCOUNT,
#                                 UN_CLAIMABLE_DISCOUNT,
#                                 INST_DISCOUNT,
#                                 [Month],
#                                 ORDER_REF_NO,
#                                 DATE_OF_ORDER,
#                                 ORDER_QUANTITY,
#                                 FOC,
#                                 TOTAL_QTY,
#                                 SALES_VLAUE,
#                                 ISNULL(SALES_VLAUE,0)+(ISNULL(CLAIMABLE_DISCOUNT,0)+ISNULL(UN_CLAIMABLE_DISCOUNT,0))+TAX_RECOVERABLE  NET_SALE_VALUE,
#                                 INVOICE_DATE_IBL,
#                                 INVOICE_NO_IBL
#                                         FROM EBS_INVOICE_ORDER_VW
#             """, conn)

# # df = pd.DataFrame(sql_query, columns=['INSTITUTION','BRANCH_ID','BRANCH_NAME','DISTRIBUTOR','INS_TYPE','PRODUCT','ITEM_CODE','SKU',
# # 'SELLING_PRICE','CLAIMABLE_DISCOUNT','UN_CLAIMABLE_DISCOUNT','INST_DISCOUNT','Month','ORDER_REF_NO','DATE_OF_ORDER',
# # 'ORDER_QUANTITY','FOC','TOTAL_QTY','SALES_VLAUE','NET_SALE_VALUE','INVOICE_DATE_IBL','INVOICE_NO_IBL'])


# df = pd.DataFrame(sql_query, columns=['INSTITUTION'])

# df['INSTITUTION'] = df['INSTITUTION'].fillna('')

# # df = pd.DataFrame(sql_query, columns=['INSTITUTION'])

# print(df.columns)


# #Creating connection with oracle
# dsn_tns = cx_Oracle.makedsn('196.16.16.106', '1521', 'sir')
# orConn = cx_Oracle.connect(user= 'IBLGRPHCM', password= 'HRAPPS1406', dsn=dsn_tns)
# c= orConn.cursor()

# # Deleting data from Target Table
# # c.execute(f''' DELETE FROM EBS_INVOICE_ORDER_TBL WHERE TO_CHAR(INVOICE_DATE_IBL,'YYYY-MM-DD HH24:MI:SS')>='{FirstDayofMonth}' ''')

# # With Truncate option
# c.execute('truncate table EBS_INVOICE_ORDER_TBL')   
# orConn.commit()

# # for row in df.itertuples():
# #     c.execute ('insert into IBLGRPHCM.EBS_INVOICE_ORDER_TBL values (:INSTITUTION,:BRANCH_ID,:BRANCH_NAME,:DISTRIBUTOR,:INS_TYPE,:PRODUCT,:ITEM_CODE,:SKU,:SELLING_PRICE,:CLAIMABLE_DISCOUNT,:UN_CLAIMABLE_DISCOUNT,:INST_DISCOUNT, :Month,:ORDER_REF_NO,:DATE_OF_ORDER,:ORDER_QUANTITY,:FOC,:TOTAL_QTY,:SALES_VLAUE,:NET_SALE_VALUE,:INVOICE_DATE_IBL,:INVOICE_NO_IBL)',
# #     (row.INSTITUTION,row.BRANCH_ID,row.BRANCH_NAME,row.DISTRIBUTOR,row.INS_TYPE,row.PRODUCT,row.ITEM_CODE,row.SKU,row.SELLING_PRICE,row.CLAIMABLE_DISCOUNT,row.UN_CLAIMABLE_DISCOUNT,
# #     row.INST_DISCOUNT, row.Month,row.ORDER_REF_NO,row.DATE_OF_ORDER,row.ORDER_QUANTITY,row.FOC,row.TOTAL_QTY,row.SALES_VLAUE,row.NET_SALE_VALUE,row.INVOICE_DATE_IBL,row.INVOICE_NO_IBL))
# #     orConn.commit()

# for row in df.itertuples():
#     c.execute ('insert into IBLGRPHCM.EBS_INVOICE_ORDER_TBL values(:INSTITUTION)', (row.INSTITUTION)
#     )
#     orConn.commit()