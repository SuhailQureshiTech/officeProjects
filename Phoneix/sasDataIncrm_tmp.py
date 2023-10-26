# from IBLOps import LogMergingTrans
# from Email import smtpEmailObjectHtml
from typing import Iterator
import pypyodbc as odbc
import pandas as pd
import time
from datetime import date, datetime, timedelta
from pandas.core.frame import DataFrame
from pandas.core.reshape.concat import concat
import os
import sys
import inspect
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from pydantic import conset
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)

# import datetime


global connectionDB, sqlConnection, records, vStartDate, numberOfRecords
global oracleServerAdd, oraclePort, oracleServiceName, oracleUserName, oraclePassword
oracleConnectionDB = None
sqlConnection = None
records = None
sqlQuery = None
cursor = None
oracleCursor = None
dataFrame1 = pd.DataFrame()
dataFrame2 = pd.DataFrame()
vday = 1
numberOfRecords = 0
total = 0
vPath = r"\\192.168.130.81\\Searle\\"
# vPath = "D:\\TEMP\\SEARLEDATAEXT\\"

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
print(type(vTodayDate))

print(vTodayDate)
if vTodayDate == 1:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=21))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

else:

    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print('Start Date : ', vStartDate)
print('End   Date : ', vEndDate)

conString = '$SPL$'

# oracleConnectionDB = xo.connect(
#     "apps", 'apps', "192.168.130.41/prod",  encoding="UTF-8")

sqlConnection = odbc.connect("Driver={SQL Server Native Client 11.0};"
					"Server=192.168.130.81\sqldw;"
					"Database=ibl_dw    ;"
					"uid=pbironew;pwd=pbiro345-")


def oraRec4():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    sqlQuery = f'''
	SELECT
    *
    FROM(
        select BR_CD,
        TRX_NUMBER AS BILL_NO,
        TRX_DATE AS BILL_DT
        ,
        CUSTOMER_NUMBER AS EBS_CUST,
        CUSTOMER_NAME AS CUSTOMER_NAME
        ,
        ADD1,
        ISNULL(ADD2,
            '-') ADD2,
        ISNULL(ADD3,
            '-') ADD3
        ,
        CHANNEL AS CH_CD,
        ITEM_CODE AS ITEM_CODE,
        DESCRIPTION AS description,
        ' '
        AS BATCH_NO
        --MEASURE
        ,
        unit_selling_price AS price
        ,
        SUM(sold_qty) AS SOLD_QTY,
        SUM(BONUS_QTY) AS BON_QTY,
        SUM(DISCOUNT) AS disc_amt,
        SUM(NET_AMT) AS NET_amt,
        SUM(GROSS_VALUE) AS GROSS_VALUE,
        SUM(DISCOUNTED_RATE) AS discounted_rate
        ,
        case when esa.SALES_ORDER_TYPE = 'Bill Near Exp Sales'
        then 'Near Expiry'
        when esa.SALES_ORDER_TYPE in ('Cancel Bill NE Sales',
            'OPS Cancel. Invoice', 'OPS-Cancel Cred Memo') then 'Cancel'
        when esa.SALES_ORDER_TYPE = 'OPS Sales Tax Cash'
        then 'Sale'
        when esa.SALES_ORDER_TYPE = 'OPS-Sales Returns'
        then 'Return'
        end as reason from EBS_SAS_ALL_LOC_DATA_NEW ESA
        where 1 = 1
        AND trx_date >={vStartDate}
        ---and trx_number='8300029992'
        GROUP BY BR_CD,
        TRX_NUMBER,
        TRX_DATE      ,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        ESA.UNIT_SELLING_PRICE,
        SALES_ORDER_TYPE,
        ADD1,
        ADD2,
        ADD3,
        CHANNEL,
        ITEM_CODE,
        DESCRIPTION
		) A
	ORDER BY
	BILL_DT
    '''
    
    dataFrame1 = pd.read_sql_query(sqlQuery, sqlConnection)

    # Removing special characters in dataframe
    dataFrame1['add1'] = dataFrame1['add1'].str.replace(r'\W+', " ")
    dataFrame1['add2'] = dataFrame1['add2'].str.replace(r'\W+', " ")
    dataFrame1['add3'] = dataFrame1['add3'].str.replace(r'\W+', " ")


def genSalesTextFile():
    # global dataFrame1, dataFrame2
    print('inside salestext file')

    # print(dataFrame1.info())

 
    var = 'BR_CD'+conString+'BILL_NO'+conString + \
        'BILL_DT'+conString+'EBS_CUST'+conString+'CH_CD'+conString+'PROD_CD'+conString+'PROD_NM'+conString+'BATCH_NO'+conString+'PRICE'+conString+'SOLD_QTY'+conString+'BON_QTY'+conString+'DISC_AMT'+conString+'NET_AMT'+conString+'GROSS_VALUE'+conString+'DISCOUNTED_RATE'+conString+'REASON'

    dataFrame2[var] = dataFrame1['br_cd']+conString+dataFrame1['bill_no']+conString+dataFrame1['bill_dt']+conString+dataFrame1['ebs_cust']+conString+dataFrame1['ch_cd']+conString+dataFrame1['item_code']+conString+dataFrame1['description']+conString+dataFrame1['batch_no']+conString+dataFrame1['price'].astype(str)+conString+dataFrame1['sold_qty'].astype(str)+conString+dataFrame1['bon_qty'].astype(str)+conString+dataFrame1['disc_amt'].astype(str)+conString+dataFrame1['net_amt'].astype(str)+conString+dataFrame1['gross_value'].astype(str)+conString+dataFrame1['discounted_rate'].astype(str)+conString+dataFrame1['reason']

    dataFrame2[{var}].to_csv(f'''{vPath}ALL_LOC_SALE.txt''',
                        index=False, header=True
                        )

def genCustomerData():
    dataFrame1.drop(columns=[
        'bill_no',  'item_code', 'description', 'batch_no', 'price', 'sold_qty', 'bon_qty', 'disc_amt', 'gross_value', 'net_amt', 'discounted_rate'
    ], inplace=True)

    dataFrame1.drop_duplicates(inplace=True)

    global dataFrame2
    dataFrame2 = dataFrame1[['br_cd', 'ebs_cust', 'customer_name',
                            'ch_cd', 'add1', 'add2', 'add3']]
 
    var = 'BR_CD'+conString+'EBS_CUST'+conString+'CUST_NM'+conString+'CH_CD'+conString+'ADD1'+conString+'ADD2'+conString+'ADD3'

    # +'EBS_CUST'+conString+'CUSTOMER_NAME'+conString+'CH_CD'+conString+'ADD1'+conString+'ADD2'+conString+'ADD3'

    dataFrame2[var] = dataFrame1['br_cd']+conString+dataFrame1['ebs_cust']+conString+dataFrame2['customer_name']+conString+dataFrame2['ch_cd']+conString+dataFrame2['add1']+conString+dataFrame2['add2']+conString+dataFrame2['add3']


    dataFrame2[{var}].to_csv(f'''{vPath}ALL_LOC_INV_CUSTOMER.txt ''',
                        index=False, header=True
                        # , sep='\t'
                    )
oraRec4()
genSalesTextFile()
genCustomerData()
