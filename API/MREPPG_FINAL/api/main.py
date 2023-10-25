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
from fastai import FastAPI,Depends, Query
from pydantic import BaseModel, Field
from typing import List, Optional
import databases
from databaserepo.databaseFactory import DatabaseConnect
from models.oracleModels import sales
# from routers import routMrepSasData
from google.cloud import bigquery
import uvicorn
from google.oauth2 import service_account
from fastai import FastAPI,HTTPException,status
global db


# credentials = service_account.Credentials.from_service_account_file('E:/ars\data-light-house-prod-0baa98f57152.json')

api_keys = [
    "iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup"
]  # This is encrypted in the database

credentials = service_account.Credentials.from_service_account_file('c:\googlekey\data-light-house-prod-0baa98f57152.json')

project_id = 'data-light-house-prod'
client = bigquery.Client(credentials= credentials,project=project_id)


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

@app.get("/sales",response_model=List[SalesList])
async def all_sales(fromDate:date,toDate:date, limit=1000):
    query = sales.select().where(sales.c.InvoiceDate>=fromDate,sales.c.InvoiceDate<=toDate
    )
    return await  DatabaseConnect.fetch_all(query)


@app.get("/sales/dist", response_model=List[SalesList])
async def all_sales(fromDate: date, toDate: date,  apikeyCode: str, distCode: Optional[str] = None, dataFlag: Optional[str] = None , limit=100):

    if  apikeyCode!='iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

    else:

            if  distCode==None and  dataFlag==None:
                query = sales.select().where(sales.c.InvoiceDate >= fromDate,
                                            sales.c.InvoiceDate <= toDate)
            elif distCode!=None:
                query = sales.select().where(sales.c.InvoiceDate >= fromDate,
                                            sales.c.InvoiceDate <= toDate, sales.c.DistributorCode == distCode)
            elif dataFlag!=None:
                query = sales.select().where(sales.c.InvoiceDate >= fromDate,
                                                sales.c.InvoiceDate <= toDate, sales.c.DATAFLAG == dataFlag)

            return await DatabaseConnect.fetch_all(query)

########################################### Branches  ###############################################################
branches_job = client.query("""
   SELECT *
   FROM `data-light-house-prod.EDW.IBL_BRANCH`""")

branches_results = branches_job.result() #
branches_df = branches_results.to_dataframe()

########################################### Full_Load_IBL_CUSTOMER  ###############################################################
Full_Load_IBL_CUSTOMER = client.query("""
    SELECT
    client_number,
    customer_number,
    customer_country_key,
    customer_name,
    customer_name2,
    customer_city,
    customer_region,
    customer_sortfield,
    customer_street_houseno,
    customer_telephoneno,
    customer_address,
    customer_searchmatchcode1,
    customer_searchmatchcode2,
    customer_searchmatchcode3,
    customer_title,
    customer_internationallocation_1,
    customer_internationallocation_2,
    customer_authorization,
    customer_industry_key,
    customer_internationallocation_checkdigit,
    customer_createdon,
    person_created_the_object,
    customer_accountgroup,
    customer_classification,
    accountno_vendor,
    customer_name3,
    customer_name4,
    customer_district,
    customer_citycode,
    regional_market,
    customer_languagekey,
    customer_taxno1,
    customer_taxno2,
    customer_liableforVAT,
    customer_telephoneno2,
    VAT_registrationno,
    competitor,
    sales_partner,
    sales_prospect,
    customer_type4,
    legal_status,
    initial_contact,
    customer_tax_jurisdiction,
    plant,
    customer_paymentblock,
    is_customer_ICMSexempt,
    is_customer_IPIexempt,
    customer_CFOP_category,
    RG_no,
    issued_by,
    state,
    RG_issue_date,
    RIC_no,
    foreign_national_registration,
    RNE_issue_date,
    CNAE,
    legal_nature,
    CRT_no,
    ICMS_taxpayer,
    name1,
    name2,
    name3,
    first_name,
    title,
    dealer,
    expiry_date,
    company_code,
    personnel_number,
    'OPS' DATA_FLAG
    FROM
    `data-light-house-prod.EDW.IBL_CUSTOMER`
    WHERE
    date>='2022-05-01'
    AND company_code='6300'
    UNION ALL
    SELECT
    NULL client_number,
    ref_customer_number customer_number,
    NULL customer_country_key,
    ibl_customer_name customer_name,
    NULL customer_name2,
    NULL customer_city,
    NULL customer_region,
    NULL customer_sortfield,
    NULL customer_street_houseno,
    NULL customer_telephoneno,
    NULL customer_address,
    NULL customer_searchmatchcode1,
    NULL customer_searchmatchcode2,
    NULL customer_searchmatchcode3,
    NULL customer_title,
    NULL customer_internationallocation_1,
    NULL customer_internationallocation_2,
    NULL customer_authorization,
    NULL customer_industry_key,
    NULL customer_internationallocation_checkdigit,
    NULL customer_createdon,
    NULL person_created_the_object,
    NULL customer_accountgroup,
    NULL customer_classification,
    NULL accountno_vendor,
    NULL customer_name3,
    NULL customer_name4,
    NULL customer_district,
    NULL customer_citycode,
    NULL regional_market,
    NULL customer_languagekey,
    NULL customer_taxno1,
    NULL customer_taxno2,
    NULL customer_liableforVAT,
    NULL customer_telephoneno2,
    NULL VAT_registrationno,
    NULL competitor,
    NULL sales_partner,
    NULL sales_prospect,
    NULL customer_type4,
    NULL legal_status,
    NULL initial_contact,
    NULL customer_tax_jurisdiction,
    NULL plant,
    NULL customer_paymentblock,
    NULL is_customer_ICMSexempt,
    NULL is_customer_IPIexempt,
    NULL customer_CFOP_category,
    NULL RG_no,
    NULL issued_by,
    NULL state,
    NULL RG_issue_date,
    NULL RIC_no,
    NULL foreign_national_registration,
    NULL RNE_issue_date,
    NULL CNAE,
    NULL legal_nature,
    NULL CRT_no,
    NULL ICMS_taxpayer,
    NULL name1,
    NULL name2,
    NULL name3,
    NULL first_name,
    NULL title,
    NULL dealer,
    NULL expiry_date,
    NULL company_code,
    NULL personnel_number,
    'SD' DATA_FLAG
    FROM
    `data-light-house-prod.EDW.VW_FRANCHISE_SALES_DATA`
    WHERE
    ref_customer_number LIKE '%-%'
    """)

# Full_Load_IBL_CUSTOMER_results = Full_Load_IBL_CUSTOMER.result() #
# Full_Load_IBL_CUSTOMER_df = Full_Load_IBL_CUSTOMER_results.to_dataframe()

Full_Load_IBL_CUSTOMER_results = Full_Load_IBL_CUSTOMER.result() #
Full_Load_IBL_CUSTOMER_df1 = Full_Load_IBL_CUSTOMER_results.to_dataframe()
Full_Load_IBL_CUSTOMER_df = Full_Load_IBL_CUSTOMER_df1.to_dict(orient = 'records')

########################################### Incremental_Load_IBL_CUSTOMER  ###############################################################
Incremental_Load_IBL_CUSTOMER = client.query("""
select
client_number,
customer_number,
customer_country_key,
customer_name,
customer_name2,
customer_city,
customer_region,
customer_sortfield,
customer_street_houseno,
customer_telephoneno,
customer_address,
customer_searchmatchcode1,
customer_searchmatchcode2,
customer_searchmatchcode3,
customer_title,
customer_internationallocation_1,
customer_internationallocation_2,
customer_authorization,
customer_industry_key,
customer_internationallocation_checkdigit,
customer_createdon,
person_created_the_object,
customer_accountgroup,
customer_classification,
accountno_vendor,
customer_name3,
customer_name4,
customer_district,
customer_citycode,
regional_market,
customer_languagekey,
customer_taxno1,
customer_taxno2,
customer_liableforVAT,
customer_telephoneno2,
VAT_registrationno,
competitor,
sales_partner,
sales_prospect,
customer_type4,
legal_status,
initial_contact,
customer_tax_jurisdiction,
plant,
customer_paymentblock,
is_customer_ICMSexempt,
is_customer_IPIexempt,
customer_CFOP_category,
RG_no,
issued_by,
state,
RG_issue_date,
RIC_no,
foreign_national_registration,
RNE_issue_date,
CNAE,
legal_nature,
CRT_no,
ICMS_taxpayer,
name1,
name2,
name3,
first_name,
title,
dealer,
expiry_date,
company_code,
personnel_number
from `data-light-house-prod.EDW.IBL_CUSTOMER`
where customer_createdon = (current_date-1) or CONFIRMEDCHANGES_DATE =  (current_date-1); """)

Incremental_Load_IBL_CUSTOMER_results = Incremental_Load_IBL_CUSTOMER.result() #
Incremental_Load_IBL_CUSTOMER_df1 = Incremental_Load_IBL_CUSTOMER_results.to_dataframe()
Incremental_Load_IBL_CUSTOMER_df = Incremental_Load_IBL_CUSTOMER_df1.to_dict(orient = 'records')

########################################### Full_Load_IBL_PRODUCTS  ###############################################################
Full_Load_IBL_PRODUCTS = client.query("""
    SELECT item_code,item_desc
    FROM `data-light-house-prod.EDW.SAP_ITEM_MATERIAL_GROUP`; """)

Full_Load_IBL_PRODUCTS_results = Full_Load_IBL_PRODUCTS.result() #
Full_Load_IBL_PRODUCTS_df1 = Full_Load_IBL_PRODUCTS_results.to_dataframe()
Full_Load_IBL_PRODUCTS_df =Full_Load_IBL_PRODUCTS_df1.to_dict(orient = 'records')



########################################### Incremental_Load_IBL_PRODUCTS  ###############################################################
Incremental_Load_IBL_PRODUCTS = client.query("""
 select *  from `data-light-house-prod.EDW.IBL_PRODUCTS`
where created_on = (current_date-1) or last_change_date =  (current_date-1); """)

Incremental_Load_IBL_PRODUCTS_results = Incremental_Load_IBL_PRODUCTS.result() #
Incremental_Load_IBL_PRODUCTS_df1 = Incremental_Load_IBL_PRODUCTS_results.to_dataframe()
Incremental_Load_IBL_PRODUCTS_df = Incremental_Load_IBL_PRODUCTS_df1.to_dict(orient = 'records')

########################################### Full_Load_IBL_BUSINESS_LINE ###############################################################
Full_Load_IBL_BUSINESS_LINE = client.query("""
  select * from `data-light-house-prod.EDW.IBL_BUSINESS_LINE`; """)

Full_Load_IBL_BUSINESS_LINE_results = Full_Load_IBL_BUSINESS_LINE.result() #
Full_Load_IBL_BUSINESS_LINE_df = Full_Load_IBL_BUSINESS_LINE_results.to_dataframe()


# print(df.head())
# app = FastAPI()
@app.get("/Full_Load_IBL_Branches")
async def root():
    return branches_df

@app.get("/Full_Load_IBL_CUSTOMER")
async def root(apikeyCode: str):
    if  apikeyCode!='iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

    else:

        return Full_Load_IBL_CUSTOMER_df

@app.get("/Incremental_Load_IBL_CUSTOMER")
async def root(apikeyCode: str):
    if  apikeyCode!='iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

    else:

        return Incremental_Load_IBL_CUSTOMER_df

@app.get("/Full_Load_IBL_PRODUCTS")
async def root(apikeyCode: str):
    if  apikeyCode!='iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

    else:

        return Full_Load_IBL_PRODUCTS_df

@app.get("/Incremental_Load_IBL_PRODUCTS")
async def root(apikeyCode: str):
    if  apikeyCode!='iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

    else:

        return Incremental_Load_IBL_PRODUCTS_df

@app.get("/Full_Load_IBL_BUSINESS_LINE")
async def root():
    return Full_Load_IBL_BUSINESS_LINE_df



if __name__=="__main__":
    uvicorn.run(app,host="10.172.0.2",port=9000)


#Temp Code
# if __name__ == "__main__":
#     uvicorn.run(app, host="localhost", port=9001)


