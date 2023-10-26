import os
import glob
from datetime import date
import pypyodbc as odbc
import pandas as pd
import sqlalchemy as sa
import urllib
import math
global df

os.system('cls')
today = date.today()


# Month abbreviation, day and year   11-Sep-21
d5 = today.strftime("%d-%b-%Y")
mypath = r'\\192.168.130.78\f\Qlik\Extraction\United Brands\ADRC.csv'
# print(max(glob.glob(mypath), key=os.path.getmtime))

df = pd.read_csv(
    mypath
                    , usecols=[
                        'CLIENT',
                        'ADDRNUMBER',
                        'DATE_FROM',
                        'DATE_TO',
                        'STR_SUPPL1',
                        'STR_SUPPL2',
                        'STR_SUPPL3'
                            ]
                )

def dataCleaning():
    df.columns = df.columns.str.strip()
    # Changing Type

    # df["MANDT"] = df["MANDT"].astype(str)
    # df["SPRAS"] = df["SPRAS"].astype(str)
    # df["KDGRP"] = df["KDGRP"].astype(str)
    # df["KTEXT"] = df["KTEXT"].astype(str)

# print(df)
# print(df.dtypes)

def insertDataRecords():


    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                    "SERVER=192.168.130.81\sqldw;"
                                    "DATABASE=ibl_dw;"
                                    "UID=pbironew;"
                                    "PWD=pbiro345-")

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params) ,fast_executemany=True)
    # print(engine)
    sql = "DELETE  FROM ADRC"
    result = engine.execute(sql)
    
    df_num_of_cols = len(df.columns)
    chunknum = math.floor(2100/df_num_of_cols)

    df.to_sql('ADRC',
            # schema='dbo',
            con=engine,
            chunksize=chunknum,
            method='multi',
            index=False,
            if_exists='append'
            )

def insertData():

        sqlConnectionPhoneix = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                        "Server=192.168.130.81\sqldw;"
                                        "Database=ibl_dw    ;"
                                        "uid=pbironew;pwd=pbiro345-")

        cursor_Phoneix = sqlConnectionPhoneix.cursor()

        deleteStatement =f'''
                            delete from ADRC
                        '''

        cursor_Phoneix.execute(deleteStatement)
        cursor_Phoneix.commit()
        # x=1
        for index, row in df.iterrows():

                insertStatement = f'''
                insert into ADRC
                        (MANDT,
                        ADDRNUMBER,
                        DATE_FROM,
                        DATE_TO,
                        STR_SUPPL1,
                        STR_SUPPL2,
                        STR_SUPPL3
                        )
                        values
                        (
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,?
                        )
                        '''
        cursor_Phoneix.executemany(insertStatement, df.values.tolist())
        cursor_Phoneix.commit()
        cursor_Phoneix.close()
        sqlConnectionPhoneix.close()

# dataCleaning()
insertDataRecords();
# insertData()
