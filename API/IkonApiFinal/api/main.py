from ast import Str
from asyncio.windows_events import NULL
import string
from tkinter.tix import MAX
from unittest import case
import sourcedefender
# import imp
from sqlite3 import Date
from tokenize import String
# from flask import session
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
# from databaserepo.databaseFactory import DatabaseConnect
from databaserepo.databaseFactory import client
from models.oracleModels import sales
# from routers import routMrepSasData
from google.cloud import bigquery
import uvicorn
from google.oauth2 import service_account
from fastapi import FastAPI,HTTPException,status

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis
from fastapi.middleware.cors import CORSMiddleware
global db



api_keys = [
    "iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup"
]  # This is encrypted in the database


app = FastAPI()

app = FastAPI(debug=False)
origins = [
    'http://10.172.0.3:9000',
    'http://34.65.6.130:9000',
    'http://localhost:9000',
    'http://localhost:9000'
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

# @app.on_event("startup")
# async def startup():
#     # await database.connect()
#     await DatabaseConnect.connect()


# @app.on_event('shutdown')
# async def shutdown():
#     await DatabaseConnect.disconnect()




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
    TradePrice:Optional[str]=None
    Units:Optional[int]=None
    Bonus:Optional[int]=None
    Discount:Optional[float]=None
    NetAmount:Optional[float]=None
    TransactionType:Optional[str]=None
    BrickCode:Optional[str]=None
    BrickName:Optional[str]=None
    DATAFLAG: Optional[str] = None

    class Config():
        from_attributes = True
        populate_by_name = True

class PhnxSalesList(BaseModel):
    trx_date:Optional[date]=None
    trx_number:Optional[str]=None
    class Config():
        from_attributes = True
        populate_by_name = True

class mrepSalesList(BaseModel):
    org_id:Optional[str]=None
    org_desc:Optional[str]=None
    trx_number:Optional[str]=None
    trx_date:Optional[date]=None

    class Config():
        from_attributes = True
        populate_by_name = True

@app.get("/sales/dist"
         , response_model=List[SalesList]
         )
async def all_sales(fromDate: date, toDate: date,  apikeyCode: str, distCode: Optional[str] = None
                    , dataFlag: Optional[str] = None, limit=100):

    vStartDate = fromDate
    vEndDate = toDate
    vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
    vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"
    if apikeyCode != 'iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized Key"
        )
    else:
            sQlQuery = f'''select  
                '999-999' ind
               , DistributorCode,InvoiceNumber,InvoiceDate,CustomerCode,CustomerName
                ,CustomerAddress,CustomerType
                ,ProductCode,ProductName,BatchNumber
                ,TradePrice,TransactionType,BrickCode,BrickName,DATAFLAG
                ,sum(Units)Units,sum(Bonus)Bonus,sum(Discount)Discount,sum(NetAmount)NetAmount
                from `data-light-house-prod.EDW.VW_SAS_MREP_DATA`
                WHERE InvoiceDate between {vStartDate} and {vEndDate}
                group by 
                DistributorCode,InvoiceNumber,InvoiceDate,CustomerCode,CustomerName
                ,CustomerAddress,CustomerType
                ,ProductCode,ProductName,BatchNumber
                ,TradePrice,TransactionType,BrickCode,BrickName,DATAFLAG
             '''

        # if  distCode==None and  dataFlag==None:
        #     sQlQuery = f'''select  * from `data-light-house-prod.EDW.VW_SAS_MREP_DATA`
        #                 WHERE InvoiceDate between {vStartDate} and {vEndDate}; '''

        # elif distCode != None:
        #     vdistCode = distCode
        #     vdistCode = "'"+vdistCode+"'"
        #     sQlQuery = f'''select  * from `data-light-house-prod.EDW.VW_SAS_MREP_DATA`
        #             WHERE InvoiceDate between {vStartDate} and {vEndDate} and DistributorCode={vdistCode}; '''
        #     print(sQlQuery)
        # elif dataFlag != None:
        #     vDataFlag = "'"+str(dataFlag)+"'"
        #     sQlQuery = f'''select  * from `data-light-house-prod.EDW.VW_SAS_MREP_DATA`
        #             WHERE InvoiceDate between {vStartDate} and {vEndDate} and DATAFLAG={vDataFlag}; '''

    # print(sQlQuery)
    Full_Load_IBL_PRODUCTS = client.query(sQlQuery)

    Full_Load_IBL_PRODUCTS_results = Full_Load_IBL_PRODUCTS.result()
    Full_Load_IBL_PRODUCTS_df1 = Full_Load_IBL_PRODUCTS_results.to_dataframe()
    Full_Load_IBL_PRODUCTS_df = Full_Load_IBL_PRODUCTS_df1.to_dict(
        orient='records')

    return Full_Load_IBL_PRODUCTS_df



# if __name__=="__main__":
#    uvicorn.run(app,host="10.172.0.3",port=9000)





