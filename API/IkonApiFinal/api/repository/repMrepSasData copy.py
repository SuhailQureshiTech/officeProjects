import sourcedefender
from typing import List
from pydantic import fields
from pydantic.fields import Field
from sqlalchemy.orm import Session
from fastapi import Depends,HTTPException,status,Response
from sqlalchemy.sql.expression import and_, or_
from database import database
from models import oracleModels
from apikey import apiKeyList
from datetime import date, datetime
from sqlalchemy.sql.sqltypes import Date, String,Integer

def salesData(db: Session,apiKey):
    if apiKey!=apiKeyList.MREP_APIKEY:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND
                            ,detail='Invalid or Missing API-Key')
    salesData=db.query(oracleModels.MrepSasData).all()
    return salesData


def salesDataDate(fromDate, toDate,
                db: Session, apiKey, limit: int = 100
                ):

    if apiKey!=apiKeyList.MREP_APIKEY:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND
                            ,detail='Invalid or Missing API-Key')

    salesData=db.query(oracleModels.MrepSasData).filter(
        and_(oracleModels.MrepSasData.BILL_DT >= fromDate
            , oracleModels.MrepSasData.BILL_DT <= toDate)
        ).order_by(oracleModels.MrepSasData.BILL_DT,oracleModels.MrepSasData.BILL_NO
                ).all()
    return salesData

def distinctCustomer(fromDate,toDate,db: Session,apiKey
                    ,limit: int=200
                    ):

    if apiKey!=apiKeyList.MREP_APIKEY:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND
                            ,detail='Invalid or Missing API-Key')

    customer=db.query(oracleModels.MrepSasData.CUSTOMERCODE ,oracleModels.MrepSasData.CUSTOMERNAME
                    ,oracleModels.MrepSasData.ADDRESS1, oracleModels.MrepSasData.ADDRESS2
                    ,oracleModels.MrepSasData.PHONE,oracleModels.MrepSasData.LICENSE
                    ,oracleModels.MrepSasData.AREACODE,oracleModels.MrepSasData.AREANAME
                    ).filter(and_(oracleModels.MrepSasData.INVOICEDATE>=fromDate
                ,oracleModels.MrepSasData.INVOICEDATE<=toDate)).distinct().all()

    return customer
