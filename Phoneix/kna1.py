import os
import glob
from datetime import date
import pypyodbc as odbc
import pandas as pd
import warnings
import psycopg2 as pg
warnings.simplefilter(action='ignore', category=FutureWarning)
global df

os.system('cls')
today = date.today()


# Month abbreviation, day and year   11-Sep-21
d5 = today.strftime("%d-%b-%Y")
print("d5 =", d5)
engine = pg.connect("dbname='hanadb_prd' user='postgres' host='192.168.130.81' port='5432' password='ibl@123@456'")
    # print(engine)
df = pd.read_sql("SELECT \"MANDT\",\"KUNNR\",\"LAND1\" ,\"NAME1\" ,\"NAME2\" ,\"ORT01\" ,\"PSTLZ\" ,\"REGIO\" ,\"SORTL\" ,\"STRAS\" ,\"TELF1\" ,\"TELFX\" ,\"XCPDK\" ,\"ADRNR\" ,\"MCOD1\" ,\"MCOD2\" ,\"MCOD3\"  FROM hanadb_prd.\"KNA1\"", con=engine)


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
                            delete from KNA1
                        '''

        cursor_Phoneix.execute(deleteStatement)
        cursor_Phoneix.commit()
        # x=1
        for index, row in df.iterrows():

                # x=x+1
                # df.to_csv('D:\\Google Drive - Office\\GLOBAL BUSINESS DEPT - GBD\\Secondary Sales Data\\salescsv'+str(x)+'.csv',index=False)

                insertStatement = f'''
                insert into KNA1
                        (
                        MANDT,
                        KUNNR,
                        LAND1,
                        NAME1,
                        NAME2,
                        ORT01,
                        PSTLZ,
                        REGIO,
                        SORTL,
                        STRAS,
                        TELF1,
                        TELFX,
                        XCPDK,
                        ADRNR,
                        MCOD1,
                        MCOD2,
                        MCOD3

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
                            ?,
                            ?
                        )
                        '''
        cursor_Phoneix.executemany(insertStatement, df.values.tolist())
        cursor_Phoneix.commit()
        cursor_Phoneix.close()
        sqlConnectionPhoneix.close()

# dataCleaning()
insertData()
