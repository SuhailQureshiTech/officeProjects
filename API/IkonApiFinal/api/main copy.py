from ast import Str
# import imp
from sqlite3 import Date
from tokenize import String
from flask import session
from marshmallow import Schema
from pendulum import date
from sqlalchemy.orm import sessionmaker,Session
from doctest import Example
from lib2to3.pytree import Base
# from re import M
import uvicorn
# from xmlrpc.client import DateTime

# import datetime
from datetime import date, datetime
import uuid
import sqlalchemy
from sqlalchemy import Column, Integer, alias, text,func
from fastapi import FastAPI,Depends, Query
from pydantic import BaseModel, Field
from typing import List, Optional
import databases
from databaserepo.databaseFactory import DatabaseConnect
from models.oracleModels import sales
# from routers import routMrepSasData
global db

# metadata = sqlalchemy.MetaData()


# engine = sqlalchemy.create_engine(
#     DATABASE_URL
# )

# metadata.create_all(engine)

# sales = sqlalchemy.Table(
#     "sas_mrep_data"
#     ,metadata
#     ,sqlalchemy.Column('ind', sqlalchemy.String,primary_key=True)
#     ,sqlalchemy.Column('br_cd', sqlalchemy.String)
#     ,sqlalchemy.Column('sold_qty',sqlalchemy.Integer)
#     ,sqlalchemy.Column('bill_dt',sqlalchemy.Date,index=True)
#     ,schema='DW'
# )


# Models

# class phnxsales(Base):
#     __tablename__ = 'sas_mrep_data'
#     br_cd=Column(String)
#     bill_no = Column(String)

#     sold_qty=Column(Integer)
#     # Schema='DW'
app = FastAPI()

@app.on_event("startup")
async def startup():
    # await database.connect()
    await DatabaseConnect.connect()


@app.on_event('shutdown')
async def shutdown():
    await DatabaseConnect.disconnect()

class SalesList(BaseModel):    
    DistributorCode:Optional[str]=None
    InvoiceNumber:Optional[str]=None
    InvoiceDate:Optional[date]=None
    CustomerCode:Optional[str]=None
    CustomerName:Optional[str]=None
    CustomerAddress:Optional[str]=None
    CustomerType:Optional[str]=None
    ProductCode:Optional[str]=None
    ProductName:Optional[str]=None
    BatchNumber:Optional[str]=None
    TradePrice:Optional[float]=None
    Units:Optional[float]=None
    Bonus:Optional[int]=None
    Discount:Optional[float]=None
    NetAmount:Optional[float]=None
    TransactionType:Optional[str]=None
    BrickCode:Optional[str]=None
    BrickName:Optional[str]=None

    # class Config():
    #     orm_mode = True
    #     allow_population_by_field_name = True


@app.get("/sales",response_model=List[SalesList])
async def all_sales(fromDate:date,toDate:date, limit=50):
    query = sales.select().where(sales.c.InvoiceDate>=fromDate 
     , sales.c.InvoiceDate<=toDate 
    )
    return await  DatabaseConnect.fetch_all(query)
    # return DatabaseConnect.fetch_all(query)

# @app.get("/users", response_model=List[UserList])
# async def find_all_users():
#     query = users.select()
#     return await database.fetch_all(query)


# @app.post('/users', response_model=UserList)
# async def register_user(user: UserEntry):
#     gID = str(uuid.uuid1())
#     gDate = str(datetime.datetime.now())
#     query = users.insert().values(
#         id=gID,
#         username=user.username,
#         password=user.password,
#         first_name=user.first_name,
#         last_name=user.last_name,
#         gender=user.gender,
#         create_at=gDate,
#         status='1'
#     )
#     await database.execute(query)
#     return {
#         'id': gID,
#         **user.dict(),
#         'create_at':gDate,
#         'status':'1'
#     }


# Old Code
# import sourcedefender
# from datetime import date, datetime
# from fastapi import FastAPI
# from routers import routMrepSasData
# from apikey import apiKey
# import  uvicorn
# app=FastAPI()
# app.include_router(routMrepSasData.router)

if __name__=="__main__":
    uvicorn.run(app,host="192.168.130.54",port=9000)
