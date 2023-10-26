import pypyodbc as odbc
import pandas as pd


global conn, recseq, cursor,sql_getSeq,id_tuple

excelFileData=pd.read_excel('d:/temp/EMP.xlsx',sheet_name='Sheet1')
columns = ['id', 'name', 'salary']

df_data = excelFileData[columns]
newCol =  df_data["id"].astype(str) + df_data["name"]
df_data.insert(0,"idName",newCol)
id_tuple=tuple(df_data["idName"])

# tuplequery_string = 'SELECT * FROM data WHERE id IN {};'.format(id_tuple)


DRIVER = 'SQL Server Native Client 11.0'
SERVER_NAME = 'GROUP-MSSQL1'  # 'GROUP-MSSQL1'
DATABASE_NAME = 'IBL_DW'
PASSWORD = 'karachi123+1'
UID = 'bi'

def connect_String(driver, server, database, uid, password):
    conn_string = f"""
        Driver={{{driver}}};
        Server={server};
        Database={database};
        UID={uid};
        PWD={password};
                """
    return conn_string

sql_getSeq = '''
            select next value for target_seq
                '''
try:
    conn = odbc.connect(connect_String(
            DRIVER, SERVER_NAME, DATABASE_NAME, UID, PASSWORD))
    cursor = conn.cursor()

    sql_insert0 = '''
        insert into emptest1(id,name,salary) select id,name,salary from emptest 
        WHERE CONCAT(id,name) IN {}; '''.format(id_tuple) +''' '''
    print(sql_insert0)
    cursor.execute(sql_insert0)
    delRec='delete FROM emptest WHERE CONCAT(id,name) IN {};'.format(id_tuple)
    records = df_data.values.tolist()

    cursor.execute(delRec)

    cursor.execute(sql_getSeq)
    recseq = cursor.fetchone()
    # print('recSeq :',recseq)

    sql_insert1 = '''
        delete from emptest
        '''
    cursor.execute(sql_insert1)
    
    df_data.pop("idName")
    records = df_data.values.tolist()
    sql_insert = '''
        insert into emptest
        values(?,?,?)
        '''
    cursor.executemany(sql_insert,records)
    cursor.commit()

except Exception as er:
    print(er)
    cursor.rollback()

finally:

    cursor.close()
    conn.close()
    