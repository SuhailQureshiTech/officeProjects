import os
import glob
from datetime import date
import pypyodbc as odbc
import pandas as pd
import psycopg2 as pg
global df

os.system('cls')
today = date.today()


# Month abbreviation, day and year   11-Sep-21
d5 = today.strftime("%d-%b-%Y")
print("d5 =", d5)
engine = pg.connect("dbname='hanadb_prd' user='postgres' host='192.168.130.81' port='5432' password='ibl@123@456'")
    # print(engine)
df = pd.read_sql("SELECT \"MANDT\",\"SPRAS\",\"MVGR1\" ,\"BEZEI\"  FROM hanadb_prd.\"TVM1T\"", con=engine)



# print(df)
# print(df.dtypes)

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
                            delete from TVM1T
                        '''

        cursor_Phoneix.execute(deleteStatement)
        cursor_Phoneix.commit()
        # x=1
        for index, row in df.iterrows():

                # x=x+1
                # df.to_csv('D:\\Google Drive - Office\\GLOBAL BUSINESS DEPT - GBD\\Secondary Sales Data\\salescsv'+str(x)+'.csv',index=False)

                insertStatement = f'''
                insert into TVM1T
                        (MANDT,
                        SPRAS,
                        MVGR1,
                        BEZEI
                        )
                        values
                        (
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

insertData()
