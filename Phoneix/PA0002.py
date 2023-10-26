import os
import glob
from datetime import date, datetime, timedelta
import pydoc
import pypyodbc as odbc
import pandas as pd
import sqlalchemy as sa
import urllib
import math
import psycopg2 as pg
global df, df1, cursor_Phoneix

os.system('cls')


def insertDataRecords():

    sqlConnectionPhoneix = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                        "Server=192.168.130.81\sqldw;"
                                        "Database=ibl_dw    ;"
                                        "uid=pbironew;pwd=pbiro345-")

    cursor_Phoneix = sqlConnectionPhoneix.cursor()
    deleteStatement = f'''
                            delete from PA0002
                        '''

    cursor_Phoneix.execute(deleteStatement)
    cursor_Phoneix.commit()
    print('delete done')

    engine = pg.connect(
        "dbname='hanadb_prd' user='postgres' host='192.168.130.81' port='5432' password='ibl@123@456'")
    # print(engine)
    df = pd.read_sql("SELECT \"MANDT\",\"PERNR\",\"ENDDA\" ,\"BEGDA\" ,\"SEQNR\" ,\"AEDTM\" ,\"NACHN\" ,\"NAME2\" ,\"NACH2\" ,\"VORNA\" ,\"GBDAT\" ,\"NATIO\" ,\"SPRSL\" ,\"GBJHR\" ,\"GBMON\" ,\"GBTAG\" ,\"NCHMC\" ,\"VNAMC\" ,\"NAMZ2\"  FROM hanadb_prd.\"PA0002\"", con=engine)
    # print(df.head())
    for index, row in df.iterrows():
        insertStatement = f'''
                insert into PA0002
                        (   MANDT,
                            PERNR,
                            ENDDA,
                            BEGDA,
                            SEQNR,
                            AEDTM,
                            NACHN,
                            NAME2,
                            NACH2,
                            VORNA,
                            GBDAT,
                            NATIO,
                            SPRSL,
                            GBJHR,
                            GBMON,
                            GBTAG,
                            NCHMC,
                            VNAMC,
                            NAMZ2

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
                            ?,
                            ?,
                            ?
                        )
                        '''
    cursor_Phoneix.executemany(insertStatement, df.values.tolist())
    cursor_Phoneix.close()
    cursor_Phoneix.commit()
    sqlConnectionPhoneix.close()

# Execute the "SELECT *" query

    # Connect to the database

    #   print(df.head())

    #   db_table_nm='PA0002'
    #   qry = "BULK INSERT " + db_table_nm + " FROM '" +   r'\\192.168.130.78\f\Qlik\Extraction\United Brands\PA0002.csv' + "' WITH (FORMAT = 'CSV', FIRSTROW = 2)"

    #   cursor_Phoneix.execute(qry)
    #   sqlConnectionPhoneix.commit()
    #   cursor_Phoneix.close()


insertDataRecords()
print('done........')
