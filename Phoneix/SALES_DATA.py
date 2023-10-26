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
from datetime import date
import pypyodbc as odbc
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
global df

os.system('cls')
today = date.today()


# Month abbreviation, day and year   11-Sep-21
d5 = today.strftime("%d-%b-%Y")
print("d5 =", d5)
mypath = r'\\192.168.130.78\f\Qlik\Extraction\United Brands\SALES_DATA.csv'
# print(glob.glob(mypath))

# print(min(glob.glob(mypath), key=os.path.getmtime))
print(max(glob.glob(mypath), key=os.path.getmtime))

df = pd.read_csv(
    mypath
    # r'D:\\Google Drive - Office\\Phoenix\\Sales\\DataFiles\\SALES_DATA.csv'                    ,sep=',', error_bad_lines=False, index_col=False, dtype='unicode'
                    , error_bad_lines=False,dtype='unicode'
                    , usecols=[
                        'ITEM_CATEGORY',
                        'BILLING_TYPE',
                        'UP_KEY',
                        'DOCUMENT_NO',
                        'CANCELLED_FLAG',
                        'COMPANY_CODE',
                        'DOCUMENT_CONDITION',
                        # 'BILLING_DATE',
                        # 'CHANNEL',
                        # 'CUSTOMER_CODE',
                        # 'BILLING_CANCELLED',
                        # 'PAYMENT_TYPE',
                        # 'ITEM_NO',
                        # 'QUANTITY',
                        # 'UNIT',
                        # 'GROSS_AMOUNT',
                        # 'UNIT_SELLING_PRICE',
                        # 'AMOUNT',
                        # 'MATERIAL_CODE',
                        # 'COST_CENTRE',
                        # 'PROFIT_CENTRE',
                        # 'ORG_ID',
                        # 'SALES_ORDER_NO',
                        # 'BUSINESS_LINE_ID',
                        # 'BOOKER_ID',
                        # 'SUPPLIER_ID',
                        # 'SALES_ORDER_TYPE',
                        # 'BSTNK',
                        # 'ITEM_DESC',
                        # 'UNCLAIM_BONUS_QUANTITY',
                        # 'CLAIM_BONUS_QUANTITY',
                        # 'CLAIMABLE_DISCOUNT',
                        # 'UNCLAIMABLE_DISCOUNT',
                        # 'TAX_RECOVERABLE',
                        # 'BILLING_TYPE_TEXT',
                        # 'ITEM_CATEGORY_TEXT'
                            ]
                )

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
    # df["CHANNEL"] = df["CHANNEL"].astype(str)
    # df["CUSTOMER_CODE"] = df["CUSTOMER_CODE"].astype(str)
    # df["BILLING_CANCELLED"] = df["BILLING_CANCELLED"].astype(str)
    # df["ITEM_NO"] = df["ITEM_NO"].astype(str)
    # df["QUANTITY"].fillna("0", inplace=True)
    # df["UNIT"] = df["UNIT"].astype(str)

    # df["GROSS_AMOUNT"].fillna("0", inplace=True)
    # df["GROSS_AMOUNT"] = df["GROSS_AMOUNT"].astype(str)
    # df["UNIT_SELLING_PRICE"].fillna("0", inplace=True)
    # df["UNIT_SELLING_PRICE"] = df["UNIT_SELLING_PRICE"].astype(str)
    # df["AMOUNT"].fillna("0", inplace=True)
    # df["AMOUNT"] = df["AMOUNT"].astype(str)
    # df["MATERIAL_CODE"] = df["MATERIAL_CODE"].astype(str)
    # df["COST_CENTRE"] = df["COST_CENTRE"].astype(str)
    # df["PROFIT_CENTRE"] = df["PROFIT_CENTRE"].astype(str)
    # df["ORG_ID"] = df["ORG_ID"].astype(str)
    # df["SALES_ORDER_NO"] = df["SALES_ORDER_NO"].astype(str)
    # df["BUSINESS_LINE_ID"] = df["BUSINESS_LINE_ID"].astype(str)
    # df["BOOKER_ID"] = df["BOOKER_ID"].astype(str)
    # df["SUPPLIER_ID"] = df["SUPPLIER_ID"].astype(str)
    # df["SALES_ORDER_TYPE"] = df["SALES_ORDER_TYPE"].astype(str)
    # df["BSTNK"] = df["BSTNK"].astype(str)

    # df["ITEM_DESC"] = df["ITEM_DESC"].astype(str)
    # df["UNCLAIM_BONUS_QUANTITY"].fillna("0", inplace=True)
    # df["CLAIM_BONUS_QUANTITY"].fillna("0", inplace=True)
    # df["CLAIMABLE_DISCOUNT"].fillna("0", inplace=True)
    # df["UNCLAIMABLE_DISCOUNT"].fillna("0", inplace=True)
    # df["TAX_RECOVERABLE"].fillna("0", inplace=True)

    # df["BILLING_TYPE_TEXT"] = df["BILLING_TYPE_TEXT"].astype(str)
    # df["ITEM_CATEGORY_TEXT"] = df["ITEM_CATEGORY_TEXT"].astype(str)

    

def insertData():

        sqlConnectionPhoneix = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                        "Server=192.168.130.81\sqldw;"
                                        "Database=ibl_dw    ;"
                                        "uid=pbironew;pwd=pbiro345-")

        # sqlConnectionHabitt = odbc.connect("Driver={SQL Server Native Client 11.0};"
        #                                 "Server=192.168.130.59;"
        #                                 "Database=habitttarzdb;"
        #                                 "uid=habittid;pwd=habitt123")

        cursor_Phoneix = sqlConnectionPhoneix.cursor()

        deleteStatement =f'''
                            delete from PHNX_SALES_DETAIL_DATA
                        '''

        cursor_Phoneix.execute(deleteStatement)
        cursor_Phoneix.commit()
        # x=1
        for index, row in df.iterrows():

                # x=x+1
                # df.to_csv('D:\\Google Drive - Office\\GLOBAL BUSINESS DEPT - GBD\\Secondary Sales Data\\salescsv'+str(x)+'.csv',index=False)

                insertStatement = f'''
                insert into PHNX_SALES_DETAIL_DATA
                        (ITEM_CATEGORY,
                            BILLING_TYPE,
                            UP_KEY,
                            DOCUMENT_NO,
                            CANCELLED_FLAG,
                            COMPANY_CODE,
                            DOCUMENT_CONDITION,
                            BILLING_DATE
                        )
                        values
                        (
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?
                        )
                        '''
        cursor_Phoneix.executemany(insertStatement, df.values.tolist())
        cursor_Phoneix.commit()
        cursor_Phoneix.close()
        sqlConnectionPhoneix.close()

dataCleaning()
insertData()
print('Merging Done..........')

# smtpEmailObjectHtml.smtpSendEmail('dna@iblgrp.com', 'muhammad.suhail@iblgrp.com,Muhammad.Aamir@iblgrp.com',
#                                 'Phoneix Sales Merging', 'Phoneix Sales Merging Completed Successfully...')
