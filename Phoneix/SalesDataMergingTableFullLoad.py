import inspect
import sys
import os
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)

from Email import smtpEmailObjectHtml
import glob
from datetime import date,datetime,timedelta
import pypyodbc as odbc
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
global df

os.system('cls')
vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
print(type(vTodayDate))

print(vTodayDate)
if vTodayDate == 7:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=7))
    vdayDiff = int(vEndDate.strftime("%d"))
    # vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))
    vStartDate = datetime.date(datetime.today()-timedelta(days=1))
else:

    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print("StartDate =",vStartDate)
print("EndtDate =",vEndDate)

mypath = r'\\192.168.130.78\f\Qlik\Extraction\United Brands\SALES_DATA.csv'
# print(glob.glob(mypath))

# print(min(glob.glob(mypath), key=os.path.getmtime))
# print(max(glob.glob(mypath), key=os.path.getmtime))

def dataCleaning():
    df.columns = df.columns.str.strip()
    # Changing Type
    df["ITEM_CATEGORY"] = df["ITEM_CATEGORY"].astype(str)
    df["BILLING_TYPE"] = df["BILLING_TYPE"].astype(str)
    df["UP_KEY"] = df["UP_KEY"].astype(str)
    df["DOCUMENT_NO"] = df["DOCUMENT_NO"].astype(str)
    df["CANCELLED_FLAG"] = df["CANCELLED_FLAG"].astype(str)
    df["COMPANY_CODE"] = df["COMPANY_CODE"].astype(str)
    df["DOCUMENT_CONDITION"] = df["DOCUMENT_CONDITION"].astype(str)
    df["CHANNEL"] = df["CHANNEL"].astype(str)
    df["CUSTOMER_CODE"] = df["CUSTOMER_CODE"].astype(str)
    df["BILLING_CANCELLED"] = df["BILLING_CANCELLED"].astype(str)
    df["ITEM_NO"] = df["ITEM_NO"].astype(str)
    df["QUANTITY"].fillna("0", inplace=True)
    df["UNIT"] = df["UNIT"].astype(str)

    df["GROSS_AMOUNT"].fillna("0", inplace=True)
    df["GROSS_AMOUNT"] = df["GROSS_AMOUNT"].astype(str)
    df["UNIT_SELLING_PRICE"].fillna("0", inplace=True)
    df["UNIT_SELLING_PRICE"] = df["UNIT_SELLING_PRICE"].astype(str)
    df["AMOUNT"].fillna("0", inplace=True)
    df["AMOUNT"] = df["AMOUNT"].astype(str)
    df["MATERIAL_CODE"] = df["MATERIAL_CODE"].astype(str)
    df["COST_CENTRE"] = df["COST_CENTRE"].astype(str)
    df["PROFIT_CENTRE"] = df["PROFIT_CENTRE"].astype(str)
    df["ORG_ID"] = df["ORG_ID"].astype(str)
    df["SALES_ORDER_NO"] = df["SALES_ORDER_NO"].astype(str)
    df["BUSINESS_LINE_ID"] = df["BUSINESS_LINE_ID"].astype(str)
    df["BOOKER_ID"] = df["BOOKER_ID"].astype(str)
    df["SUPPLIER_ID"] = df["SUPPLIER_ID"].astype(str)
    df["SALES_ORDER_TYPE"] = df["SALES_ORDER_TYPE"].astype(str)
    df["ITEM_DESC"] = df["ITEM_DESC"].astype(str)
    df["UNCLAIM_BONUS_QUANTITY"].fillna("0", inplace=True)
    df["CLAIM_BONUS_QUANTITY"].fillna("0", inplace=True)
    df["CLAIMABLE_DISCOUNT"].fillna("0", inplace=True)
    df["UNCLAIMABLE_DISCOUNT"].fillna("0", inplace=True)
    df["TAX_RECOVERABLE"].fillna("0", inplace=True)

    df["BILLING_TYPE_TEXT"] = df["BILLING_TYPE_TEXT"].astype(str)
    df["ITEM_CATEGORY_TEXT"] = df["ITEM_CATEGORY_TEXT"].astype(str)

def insertData():

        sqlConnectionPhoneix = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                        "Server=192.168.130.81\sqldw;"
                                        "Database=ibl_dw;"
                                        "uid=pbironew;pwd=pbiro345-")

        cursor_Phoneix = sqlConnectionPhoneix.cursor()

        deleteStatement =f'''
                        DELETE FROM PHNX_SALES_DATA
                        WHERE 1 = 1                       
                        '''

        cursor_Phoneix.execute(deleteStatement)
        cursor_Phoneix.commit()

        insertStatement = f'''
        INSERT INTO PHNX_SALES_DATA(ITEM_CATEGORY
        , ORG_ID
        , ORG_DESC
        , TRX_DATE
        , TRX_NUMBER
        , BOOKER_ID
        , BOOKER_NAME
        , SUPPLIER_ID
        , SUPPLIER_NAME
        , BUSINESS_LINE_ID
        , BUSINESS_LINE
        , CHANNEL
        , CUSTOMER_ID
        , CUSTOMER_NUMBER
        , CUSTOMER_NAME
        , SALES_ORDER_TYPE
        , INVENTORY_ITEM_ID
        , ITEM_CODE
        , DESCRIPTION
        , UNIT_SELLING_PRICE
        , SOLD_QTY
        , BONUS_QTY
        , CLAIMABLE_DISCOUNT
        , UNCLAIMABLE_DISCOUNT
        , TAX_RECOVERABLE
        , NET_AMOUNT
        , GROSS_AMOUNT
        , TOTAL_DISCOUNT
        , CUSTOMER_TRX_ID
        , REASON_CODE
        , BILL_TYPE_DESC
        , COMPANY_CODE
        , BILLING_TYPE
        , SALES_ORDER_NO
        , ADD1
        , ADD2
        , ADD3
        , BSTNK
        , ITEM_NO
        ,RETURN_REASON_CODE
        )
        SELECT
            ITEM_CATEGORY
            , ORG_ID
            , ORG_DESC
            , TRX_DATE
            , TRX_NUMBER
            , BOOKER_ID
            , BOOKER_NAME
            , SUPPLIER_ID
            , SUPPLIER_NAME
            , BUSINESS_LINE_ID
            , BUSINESS_LINE
            , CHANNEL
            , CUSTOMER_ID
            , CUSTOMER_NUMBER
            , CUSTOMER_NAME
            , SALES_ORDER_TYPE
            , INVENTORY_ITEM_ID
            , ITEM_CODE
            , DESCRIPTION
            , UNIT_SELLING_PRICE
            , SOLD_QTY
            , BONUS_QTY
            , CLAIMABLE_DISCOUNT
            , UNCLAIMABLE_DISCOUNT
            , TAX_RECOVERABLE
            , NET_AMOUNT
            , GROSS_AMOUNT
            , TOTAL_DISCOUNT
            , CUSTOMER_TRX_ID
            , REASON_CODE
            , BILL_TYPE_DESC
            , COMPANY_CODE
            , BILLING_TYPE
            , SALES_ORDER_NO
            , ADD1
            , ADD2
            , ADD3
            , BSTNK
            , ITEM_NO
            , RETURN_REASON_CODE
            FROM PHNX_SALES_VIEW
           '''
        cursor_Phoneix.execute(insertStatement)
        cursor_Phoneix.commit()
        cursor_Phoneix.close()
        sqlConnectionPhoneix.close()

# insertData()

# smtpEmailObjectHtml.smtpSendEmail('dna@iblgrp.com', 'muhammad.suhail@iblgrp.com,Muhammad.Aamir@iblgrp.com',
#                                 'Phoneix Sales Merging', 'Phoneix Sales Merging Completed Successfully...')
