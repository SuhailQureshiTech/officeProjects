import sourcedefender
from typing import Dict, List, Optional
from fastapi import APIRouter,Depends,HTTPException,status
from sqlalchemy.orm import Session
from datetime import date, datetime
from sqlalchemy.sql.sqltypes import DateTime

from sqlalchemy.util.compat import importlib_metadata_get
from database import database
from models   import oracleModels
from repository import repMrepSasData
from  schemas import oracleSchemas
from sqlalchemy import Integer,String,Date

router=APIRouter(
    prefix='/mrep',
    tags=['Sales Data']
)

@router.get('/sales',response_model=List[oracleSchemas.MrepSasData])
def Sales_Data(
                db: Session=Depends(database.get_Oracledb),apiKey: Optional[str]=None
            ):
    return repMrepSasData.salesData(db,apiKey)


@router.get('/salesByDate'
            ,response_model=List[oracleSchemas.MrepSasData]
            )

def Sales_Data_Date_Range(fromDate: date,toDate: date,
                db: Session=Depends(database.get_Oracledb),apiKey: Optional[str]=None
            ):
    return repMrepSasData.salesDataDate(fromDate,toDate,db,apiKey)



@router.get('/customer'
            ,response_model= List[oracleSchemas.MrepCustomer]
            )

def Customer_Data_Date_Range(
                fromDate: date,toDate: date,
                db: Session=Depends(database.get_Oracledb),apiKey: Optional[str]=None
            ):
    return repMrepSasData.distinctCustomer(fromDate,toDate,db,apiKey)
