from ast import Index
from operator import index
import  requests as req
# from pandas import json_normalize,DataFrame as df
import pandas as pd
import sqlalchemy as sa
import urllib
import math
import databases
import sqlalchemy
import time
import datetime


df=pd.DataFrame()


# sales MREP
start = time.perf_counter()
api_url = 'http://192.168.130.54:9000/sales?fromDate=2022-01-01&toDate=2022-01-31'
response = req.get(api_url)
# print(response.json())

r = response.json()
df = pd.DataFrame.from_dict(r)
    # print(df)


start = time.perf_counter()

def apiToCsv():

    df.to_csv("E:\\TEMP\\MREP_JAN.csv",index=False)

    # writer = pd.ExcelWriter("D:\\TEMP\\MrepData.xlsx",engine='xlsxwriter')
    # df.to_excel(writer)
    # writer.save()

    GROUP_LENGTH = 1000000  # set nr of rows to slice df

    # with pd.ExcelWriter('D:\\TEMP\\MrepData.xlsx') as writer:
    #   for i in range(0, len(df), GROUP_LENGTH):
    #       df[i: i+GROUP_LENGTH].to_excel(writer,
    #                                   sheet_name='Row {}'.format(i), index=False, header=True)


def insertDataRecords():

    DATABASE_URL = "postgresql://apiuser:Api_Ibl_123_456@192.168.130.81:5432/DATAWAREHOUSE"
    DatabaseConnect = databases.Database(DATABASE_URL)
    metadata = sqlalchemy.MetaData()
    engine = sqlalchemy.create_engine(DATABASE_URL)

    print(engine)
    # sql = "DELETE  FROM ADRC"
    # result = engine.execute(sql)

    df_num_of_cols = len(df.columns)
    chunknum = math.floor(2100/df_num_of_cols)

    df.to_sql('sas_mrep_data_test',
            schema='DW',
            con=engine,
            chunksize=chunknum,
            method='multi',
            index=False,
            if_exists='append'
            )

    finish = time.perf_counter()
    totalExecutionTime = datetime.timedelta(seconds=finish-start)
    print(f'Done in {totalExecutionTime} ')


# insertDataRecords()
apiToCsv()