import os
import glob
# from datetime import date
from datetime import date, datetime, timedelta
import pypyodbc as odbc
import pandas as pd
import sqlalchemy
import urllib
df = pd.DataFrame()

df = pd.read_csv("D:\\TEMP\\RDS.csv", encoding='unicode_escape')
df.columns = df.columns.str.strip()
df.replace(',', '', regex=True, inplace=True)

# following block working.....
# spec_chars = ["!", '"', "#", "%", "&", "'", "(", ")",
#                 "*", "+", ",", "-", ".", "/", ":", ";", "<",
#                 "=", ">", "?", "@", "[", "\\", "]", "^", "_",
#                 "`", "{", "|", "}", "~", "–"]
# for char in spec_chars:
#     df['CUSTOMER_ADDRESS'] = df['CUSTOMER_ADDRESS'].str.replace(char, ' ')

# df['CUSTOMER_ADDRESS'] = df['CUSTOMER_ADDRESS'].str.split().str.join(" ")

connect_string = 'postgresql+psycopg2://apiuser:apiIbl$$123$$456@35.216.168.189:5433/DATAWAREHOUSE'
engine1 = sqlalchemy.create_engine(connect_string)

sql = f'''
                truncate table "DW"."FRANCHISE_ADDRESS"
            '''
result = engine1.execute(sql)

df.to_sql('FRANCHISE_ADDRESS',
        schema='DW',
        con=engine1,
        index=False,
        if_exists='append',
        )


# df['suhail'] = df['CUSTOMER_ADDRESS'].str.replace('\*', '', regex=True)
# df.replace('\*', '', regex=True)

# df.to_csv("D:\\TEMP\\TEKKO.csv",index=False)
# print(df.info())


