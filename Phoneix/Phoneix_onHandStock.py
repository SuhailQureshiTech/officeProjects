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

df = ''

os.system('cls')
today = date.today()
d6 = today.strftime("%d-%b-%Y")


df = pd.read_csv(
    # r'D:\\Google Drive - Office\\Phoenix\\stock\\DataFiles\\STOCK_04-Oct-2021.csv',
    r'\\192.168.130.78\f\Qlik\Extraction\United Brands\STOCK_'+d6+'.csv',
    sep=',', error_bad_lines=False, index_col=False, dtype='unicode',
    usecols=[
        'DATE',
        'MATERIAL',
        'MATERIAL_DESC',
        'STORAGE_LOCATION',
        'DESCRIPTION',
        'MATERIAL_GROUP_DESC',
        'MATERIAL_GROUP',
        'MATERIAL_GROUP_1',
        'TP',
        'BATCH',
        'EXPIRY_DATE',
        'UNRESTRICTED',
        'VALUE_UNRESTRICTED',
        'TRANSIT',
        'RESTRICTED_USE',
        'BLOCKED'

    ]
)

def dataCleaning():
    # pd.set_option('display.width',None)
    # pd.set_option('display.max_columns',None)
    df.columns = df.columns.str.strip()

    df.loc[df['EXPIRY_DATE'].str.contains('00.00.000'), 'EXPIRY_DATE'] = '01-jan-1999'
    df['EXPIRY_DATE'] =pd.to_datetime(df.EXPIRY_DATE)

    # Changing Type
    df["MATERIAL"] = df["MATERIAL"].astype(str)
    df["MATERIAL_DESC"] = df["MATERIAL_DESC"].astype(str)
    df["STORAGE_LOCATION"] = df["STORAGE_LOCATION"].astype(str)
    df["DESCRIPTION"] = df["DESCRIPTION"].astype(str)
    df["MATERIAL_GROUP_DESC"] = df["MATERIAL_GROUP_DESC"].astype(str)
    df["MATERIAL_GROUP"] = df["MATERIAL_GROUP"].astype(str)
    df["MATERIAL_GROUP_1"] = df["MATERIAL_GROUP_1"].astype(str)
    df["BATCH"] = df["BATCH"].astype(str)

    df["MATERIAL"].fillna("",  inplace=True)
    df["MATERIAL_DESC"].fillna("",  inplace=True)
    df["STORAGE_LOCATION"].fillna("",  inplace=True)
    df["DESCRIPTION"].fillna("",  inplace=True)
    df["MATERIAL_GROUP_DESC"].fillna("",  inplace=True)
    df["MATERIAL_GROUP"].fillna("",  inplace=True)
    df["MATERIAL_GROUP_1"].fillna("",  inplace=True)
    df["TP"].fillna("0",  inplace=True)
    df["BATCH"].fillna("",  inplace=True)
    # df["EXPIRY_DATE"].fillna("",  inplace=True)
    df["UNRESTRICTED"].fillna("0",  inplace=True)
    df["VALUE_UNRESTRICTED"].fillna("0",  inplace=True)
    df["TRANSIT"].fillna("0",  inplace=True)
    df["RESTRICTED_USE"].fillna("0",  inplace=True)
    df["BLOCKED"].fillna("0",  inplace=True)
    # print(df)
    # writing data into csv for debug
    # df.to_csv('D:\\Google Drive - Office\\Phoenix\\Stock\\DataFiles\\stockres.csv', index=False)

def insertData():

        sqlConnectionPhoneix = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                        "Server=192.168.130.81\sqldw;"
                                        "Database=ibl_dw;"
                                        "uid=pbironew;pwd=pbiro345-")

        # sqlConnectionPhoneix = odbc.connect("Driver={SQL Server Native Client 11.0};"
        #                                 "Server=192.168.130.59;"
        #                                 "Database=habitttarzdb;"
        #                                 "uid=habittid;pwd=habitt123")

        cursor_Phoneix = sqlConnectionPhoneix.cursor()

        deleteStatement =f'''
                            delete from PHNX_ONHAND_STOCK
                        '''

        cursor_Phoneix.execute(deleteStatement)
        cursor_Phoneix.commit()
        # x=1

        for index, row in df.iterrows():

                # x=x+1
                # df.to_csv('D:\\Google Drive - Office\\GLOBAL BUSINESS DEPT - GBD\\Secondary Sales Data\\salescsv'+str(x)+'.csv',index=False)

                insertStatement = f'''
                insert into PHNX_ONHAND_STOCK
                        (TRANS_DATE,
                        MATERIAL,
                        MATERIAL_DESC,
                        STORAGE_LOCATION,
                        Storage_DESCRIPTION,
                        MATERIAL_GROUP_DESC,
                        MATERIAL_GROUP,
                        MATERIAL_GROUP_1,
                        TP,
                        BATCH,
                        EXPIRY_DATE,

                        UNRESTRICTED,
                        VALUE_UNRESTRICTED,
                        TRANSIT,
                        RESTRICTED_USE,
                        BLOCKED
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
                            ?,
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

smtpEmailObjectHtml.smtpSendEmail('dna@iblgrp.com', 'muhammad.suhail@iblgrp.com,Muhammad.Aamir@iblgrp.com',
                                'Phoneix Onhand Stock', 'Phoneix Onhand Stock Completed Successfully...')
