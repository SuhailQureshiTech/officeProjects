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
today = date.today()

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))



vStartDate = datetime.date(datetime.today()-timedelta(days=10))
vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print("StartDate =",vStartDate)
print("EndtDate =",vEndDate)

# vStartDate = datetime.date(datetime.today()-timedelta(days=45))
# print(vStartDate)

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
                                        "uid=pbironew;pwd=pbiro1234_456")

        cursor_Phoneix = sqlConnectionPhoneix.cursor()

        deleteStatement =f'''
                        DELETE FROM BOOKING_EXEC_DATA
                        WHERE 1 = 1
                        AND DELIVERY_DATE BETWEEN {vStartDate} AND {vEndDate}
                       
                        '''

        cursor_Phoneix.execute(deleteStatement)
        cursor_Phoneix.commit()

        insertStatement = f'''
        INSERT INTO BOOKING_EXEC_DATA (COMPANY_CODE
        , ORG_ID
        , ORG_DESC
        , ITEM_CODE
        , ITEM_DESC
        , BUSINESS_LINE_ID
        , BUSINESS_LINE
        , STATUS
        , ORDER_NUMBER
        , SALES_ORDER_NO
        , ORDER_DATE
        , DELIVERY_DATE
        , CUSTOMER_NUMBER
        , CUSTOMER_NAME
        , BOOKER_CODE
        , BOOKER_NAME
        , SUPPLIER_ID
        , SUPPLIER_NAME
        , ORDER_AMOUNT
        , ORDER_UNITS
        , SOLD_QTY
        , UNIT_SELLING_PRICE
        , RETURN_AMOUNT
        , CANCELLED
        , GROSS_AMOUNT
        , DATA_FLAG
        , RETURN_REASON_CODE
        , PJP_CODE
        , PJP_NAME)
  SELECT
    COMPANY_CODE,
    ORG_ID,
    ORG_DESC,
    ITEM_CODE,
    ITEM_DESC,
    BUSINESS_LINE_ID,
    BUSINESS_LINE,
    STATUS,
    ORDER_NUMBER,
    SALES_ORDER_NO,
    ORDER_DATE,
    DELIVERY_DATE,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    BOOKER_CODE,
    BOOKER_NAME,
    SUPPLIER_ID,
    SUPPLIER_NAME,
    ORDER_AMOUNT,
    ORDER_UNITS,
    SOLD_QTY,
    UNIT_SELLING_PRICE,
    RETURN_AMOUNT,
    CANCELLED,
    GROSS_AMOUNT,
    DATA_FLAG,
    RETURN_REASON_CODE,
    PJP_CODE,
    PJP_NAME
  FROM (SELECT
    SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4) COMPANY_CODE,
    SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 5, 4) ORG_ID,
    SO.ORG_DESC,
    CAST(OI.SAP_SKU_CODE AS varchar(30)) ITEM_CODE,
    SIB.ITEM_DESC,
    SIB.BUSINESS_LINE_ID,
    SIB.BUSINESS_LINE,
    OI.STATUS,
    OI.ORDER_NUMBER,
    NULL SALES_ORDER_NO,
    OI.ORDER_DATE,
    OI.DELIVERY_DATE,
    OI.SAP_STORE_CODE CUSTOMER_NUMBER,
    OI.STORE_NAME CUSTOMER_NAME,
    OI.ORDER_BOOKER_CODE BOOKER_CODE,
    OI.ORDER_BOOKER_NAME BOOKER_NAME,
    NULL SUPPLIER_ID,
    NULL SUPPLIER_NAME,
    OI.ORDER_AMOUNT,
    OI.ORDER_UNITS,
    0 SOLD_QTY,
    0 UNIT_SELLING_PRICE,
    NULL RETURN_AMOUNT,
    NULL CANCELLED,
    NULL GROSS_AMOUNT,
    1 DATA_FLAG,
    NULL RETURN_REASON_CODE,
    OI.PJP_CODE,  
    OI.PJP_NAME 
  FROM ORDER_INFO OI
  LEFT OUTER JOIN SAP_ORG SO
    ON (SO.ORG_ID = SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 5, 4))
  LEFT OUTER JOIN SAP_ITEM_BUSLINE SIB
    ON (SIB.ITEM_CODE = OI.SAP_SKU_CODE)
  WHERE 1 = 1
  AND DELIVERY_DATE BETWEEN {vStartDate} and {vEndDate}
  AND ORDER_NUMBER NOT IN (SELECT
  DISTINCT
    BSTNK
  FROM PHNX_SALES_DATA psd
  WHERE TRX_DATE BETWEEN  {vStartDate} and {vEndDate}
  AND SUBSTRING(PSD.ITEM_CODE, 9, 10) = OI.SAP_SKU_CODE
  )
  UNION ALL
  SELECT
    COMPANY_CODE,
    ORG_ID,
    ORG_DESC,
    ITEM_CODE,
    ITEM_DESC,
    BUSINESS_LINE_ID,
    BUSINESS_LINE,
    STATUS,
    ORDER_NUMBER,
    SALES_ORDER_NO,
    ORDER_DATE,
    DELIVERY_DATE,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    BOOKER_CODE,
    BOOKER_NAME,
    SUPPLIER_ID,
    SUPPLIER_NAME,
    sum(ORDER_AMOUNT),
    SUM(ORDER_UNITS) ORDER_UNITS,
    SUM(SOLD_QTY) SOLD_QTY,
    CASE
      WHEN (ISNULL(SUM(SOLD_QTY), 0) = 0) THEN 0
      ELSE SUM(GROSS_AMOUNT) / SUM(SOLD_QTY)
    END UNIT_SELLING_PRICE,
    SUM(RETURN_AMOUNT) RETURN_AMOUNT,
    SUM(CANCELLED) CANCELLED,
    SUM(GROSS_AMOUNT) GROSS_AMOUNT,
    DATA_FLAG,
    RETURN_REASON_CODE,
    NULL PJP_CODE,
    NULL PJP_NAME
  FROM (SELECT
    PSD.COMPANY_CODE,
    PSD.ORG_ID,
    PSD.ORG_DESC,
    SUBSTRING(PSD.ITEM_CODE, 9, 10) ITEM_CODE,
    PSD.DESCRIPTION ITEM_DESC,
    PSD.BUSINESS_LINE_ID,
    PSD.BUSINESS_LINE,
    'Invoiced' STATUS,
    CASE
      WHEN DATALENGTH(BSTNK) = 1 THEN PSD.SALES_ORDER_NO
      ELSE BSTNK
    END ORDER_NUMBER,
    PSD.SALES_ORDER_NO,
    CASE
      WHEN
        DATALENGTH(BSTNK) = 1 THEN PSD.TRX_DATE
      ELSE (SELECT
        DISTINCT
          OI.ORDER_DATE
        FROM ORDER_INFO OI
        WHERE 1 = 1
        AND OI.ORDER_NUMBER = PSD.BSTNK
        AND OI.SAP_SKU_CODE = SUBSTRING(PSD.ITEM_CODE, 9, 10))
    END ORDER_DATE,
    PSD.TRX_DATE DELIVERY_DATE,
    PSD.CUSTOMER_NUMBER,
    PSD.CUSTOMER_NAME,
    PSD.BOOKER_ID BOOKER_CODE,
    PSD.BOOKER_NAME,
    PSD.SUPPLIER_ID,
    PSD.SUPPLIER_NAME,
    ISNULL
    (CASE
      WHEN DATALENGTH(BSTNK) = 1 THEN PSD.GROSS_AMOUNT
      ELSE CASE
          WHEN UPPER(PSD.SALES_ORDER_TYPE) LIKE '%CANCEL%' THEN 0
          ELSE (SELECT
              OI.ORDER_AMOUNT
            FROM ORDER_INFO OI
            WHERE 1 = 1
            AND OI.ORDER_NUMBER = PSD.BSTNK
            AND OI.SAP_SKU_CODE = SUBSTRING(PSD.ITEM_CODE, 9, 10))
        END
    END, 0) ORDER_AMOUNT,
    0 ORDER_UNITS,
    PSD.SOLD_QTY,
    PSD.UNIT_SELLING_PRICE,
    ISNULL(
    CASE
      WHEN
        SALES_ORDER_TYPE IN
        (
        'OPS-Sales Returns', 'IBL UB Billing Retur'
        ) THEN (PSD.GROSS_AMOUNT)
    END, 0) RETURN_AMOUNT,
    ISNULL(
    CASE
      WHEN
        SALES_ORDER_TYPE IN
        (
        'Cancel Bill NE Sales',
        'OPS Cancel. Invoice',
        'OPS-Cancel Cred Memo',
        'Cancel of Cred Memo',
        'UB Cancel. Invoice',
        'UB-Cancel Cred Memo'
        ) THEN (PSD.GROSS_AMOUNT)
    END, 0) CANCELLED,
    PSD.GROSS_AMOUNT,
    2 DATA_FLAG,
    RETURN_REASON_CODE
  FROM PHNX_SALES_DATA PSD
  WHERE 1 = 1
  AND PSD.TRX_DATE BETWEEN  {vStartDate} and {vEndDate}
  AND ITEM_CATEGORY NOT IN ('ZFOU', 'ZFCL', 'ZBRU', 'ZREI', 'ZBRC')
  ) A
  GROUP BY COMPANY_CODE,
           ORG_ID,
           ORG_DESC,
           ITEM_CODE,
           ITEM_DESC,
           BUSINESS_LINE_ID,
           BUSINESS_LINE,
           STATUS,
           ORDER_NUMBER,
           SALES_ORDER_NO,
           ORDER_DATE,
           DELIVERY_DATE,
           CUSTOMER_NUMBER,
           CUSTOMER_NAME,
           BOOKER_CODE,
           BOOKER_NAME,
           SUPPLIER_ID,
           SUPPLIER_NAME,
           DATA_FLAG,
           RETURN_REASON_CODE
           ) A
        WHERE 1 = 1

        '''
        # vStartDate = datetime.date(datetime.today()-timedelta(days=45))
        # print(vStartDate)
        updateStatement=f'''
            UPDATE BOOKING_EXEC_DATA
            SET ORDER_UNITS = (SELECT
            OI.ORDER_UNITS
            FROM ORDER_INFO OI
            WHERE 1 = 1
            AND OI.DELIVERY_DATE BETWEEN {vStartDate} and {vEndDate}
            AND OI.ORDER_NUMBER = BOOKING_EXEC_DATA.ORDER_NUMBER
            AND OI.SAP_SKU_CODE = BOOKING_EXEC_DATA.ITEM_CODE
            AND SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4) = BOOKING_EXEC_DATA.COMPANY_CODE)
            WHERE DELIVERY_DATE BETWEEN  {vStartDate} and {vEndDate}
            ;              
            '''
        
        cursor_Phoneix.execute(insertStatement)
        cursor_Phoneix.commit()
        cursor_Phoneix.execute(updateStatement)
        cursor_Phoneix.commit()
        
        cursor_Phoneix.close()
#         sqlConnectionPhoneix.close()


insertData()

# smtpEmailObjectHtml.smtpSendEmail('dna@iblgrp.com', 'muhammad.suhail@iblgrp.com,Muhammad.Aamir@iblgrp.com',
#                                 'Phoneix Sales Merging', 'Phoneix Sales Merging Completed Successfully...')
