
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import databases
import sqlalchemy
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.sql.schema import MetaData
from sqlalchemy.engine.url import URL
import pyodbc,urllib
import cx_Oracle

# cx_Oracle.init_oracle_client(
#     lib_dir=r"C:\oraclexe\app\oracle\product\11.2.0\server\bin")

# server = '192.168.130.81\SQLDW,1433'  # to specify an alternate port
# database = 'ibl_dw'
# username = 'pbironew'
# password = 'pbiro345-'

# params = urllib.parse.quote_plus(
#     "'DRIVER={SQL Server Native Client 11.0};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password")

# oracleEngine = create_engine(
#     "mssql+pyodbc://pbironew:pbiro345-@192.168.130.81:1433/ibl_dw?driver=ODBC+Driver+17+for+SQL+Server")
# print(oracleEngine)


# Postgres Database
DATABASE_URL = "postgresql://apiuser:Api_Ibl_123_456@192.168.130.81:5432/DATAWAREHOUSE"
database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()
oracleEngine = sqlalchemy.create_engine(
    DATABASE_URL
)

#
# SQLALCHEMY_DATABASE_URL = 'mysql+pymysql://pbironew:pbiro345-@192.168.130.81/ibl_dw'
#cnx = create_engine('mysql+pymysql://<username>:<password>@<host>/<dbname>')
# oracleEngine = create_engine(SQLALCHEMY_DATABASE_URL)
# SessionLocal=sessionmaker(autocommit=False,autoflush=False,bind=engine)

# connect_url = URL(
#     "oracle+cx_oracle",
#     username="apps",
#     password="apps",
#     host="192.168.130.41",
#     port="1521",
#     database="prod"
# )

# oracleEngine =create_engine(connect_url)
oracleSessionLocal=sessionmaker(autocommit=False,autoflush=False,bind=oracleEngine)

Base=declarative_base()

def get_Oracledb():
    db=oracleSessionLocal()
    try:
        yield db
    finally:
        db.close()



