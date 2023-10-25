
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import databases
import sqlalchemy
from fastai import FastAPI
from pydantic import BaseModel
from sqlalchemy.sql.schema import MetaData
from sqlalchemy.engine.url import URL
import pyodbc,urllib
import cx_Oracle

# Postgres Database
DATABASE_URL = "postgresql://apiuser:apiIbl$$123$$456@35.216.168.189:5433/DATAWAREHOUSE"
DatabaseConnect = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()
oracleEngine = sqlalchemy.create_engine(
    DATABASE_URL
)

# oracleEngine =create_engine(connect_url)
oracleSessionLocal=sessionmaker(autocommit=False,autoflush=False,bind=oracleEngine)

Base=declarative_base()

def get_Oracledb():
    db=oracleSessionLocal()
    try:
        yield db
    finally:
        db.close()



