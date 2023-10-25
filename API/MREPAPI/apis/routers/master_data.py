from fastai import APIRouter, Depends
from apis.db_schemas import master_schemas
from apis import database
from sqlalchemy.orm import Session
from apis.repository import master_data
# email start
from fastai import (
    FastAPI,
    BackgroundTasks,
    UploadFile, File,
    Form,
    Query,
    Body,
    Depends
)
from starlette.responses import JSONResponse
from starlette.requests import Request
# from fastapi_mail import FastMail, MessageSchema,ConnectionConfig
from pydantic import EmailStr, BaseModel
from typing import List
# from fastapi_mail.email_utils import DefaultChecker
# from fastapi_mail import ConnectionConfig
#end email

router = APIRouter(
    prefix="/data",
    tags=["Master Data Management"]
)


@router.get('/getAllCompanies')
def get_all_companies(db: Session=Depends(database.get_db)):
    return master_data.listOfCompanies(db)

@router.get('/getAllBusinessUnit')
def get_all_business_unit(db: Session=Depends(database.get_db)):
    return master_data.listOfBusinessUnit(db)

@router.get('/getAllTeam')
def get_all_team(db: Session=Depends(database.get_db)):
    return master_data.listOfTeam(db)

@router.get('/getAllBrand')
def get_all_brand(db: Session=Depends(database.get_db)):
    return master_data.listOfBrand(db)

@router.get('/getAllProduct')
def get_all_product(db: Session=Depends(database.get_db)):
    return master_data.listOfProduct(db)

@router.get('/getAllCity')
def get_all_city(db: Session=Depends(database.get_db)):
    return master_data.listOfCity(db)

@router.get('/getAllBranch')
def get_all_branch(db: Session=Depends(database.get_db)):
    return master_data.listOfBranch(db)

@router.get('/getAllDesignation')
def get_all_designation(db: Session=Depends(database.get_db)):
    return master_data.listOfDesignation(db)

@router.get('/getAllEmployee')
def get_all_emoloyee(db: Session=Depends(database.get_db)):
    return master_data.listOfEmployee(db)

@router.post('/createBrick')
def create_brick(request: master_schemas.Brick, db: Session=Depends(database.get_db)):
    return master_data.createBrick(request, db)

@router.get('/getAllBrick')
def get_all_brick(db: Session=Depends(database.get_db)):
    return master_data.listOfBrick(db)

@router.post('/createRegion')
def create_region(request: master_schemas.Region, db: Session=Depends(database.get_db)):
    return master_data.createRegion(request, db)

@router.get('/getAllRegion')
def get_all_region(db: Session=Depends(database.get_db)):
    return master_data.listOfRegion(db)

@router.post('/createTerritory')
def create_territory(request: master_schemas.Territory, db: Session=Depends(database.get_db)):
    return master_data.createTerritory(request, db)

@router.get('/getAllTerritory')
def get_all_territory(db: Session=Depends(database.get_db)):
    return master_data.listOfTerritory(db)

@router.post('/createZone')
def create_zone(request: master_schemas.Zone, db: Session=Depends(database.get_db)):
    return master_data.createZone(request, db)

@router.get('/getAllZone')
def get_all_zone(db: Session=Depends(database.get_db)):
    return master_data.listOfZone(db)


@router.get('/getAllDoctor')
def get_all_doctor(db: Session=Depends(database.get_db)):
    return master_data.listOfDoctor(db)

@router.get('/getAllDistributor')
def get_all_distributor(db: Session=Depends(database.get_db)):
    return master_data.listOfDistributor(db)

@router.get('/getAllProductBrand')
def get_all_product_brand(db: Session=Depends(database.get_db)):
    return master_data.listOfProductBrand(db)

@router.get('/getAllProductTeam')
def get_all_product_team(db: Session=Depends(database.get_db)):
    return master_data.listOfProductTeam(db)

@router.get('/getAllProductPrice')
def get_all_product_price(db: Session=Depends(database.get_db)):
    return master_data.listOfProductPrice(db)

@router.get('/getAllCustomerType')
def get_all_customer_type(db: Session=Depends(database.get_db)):
    return master_data.listOfCustomerType(db)

@router.get('/getAllCustomer')
def get_all_customer(db: Session=Depends(database.get_db)):
    return master_data.listOfCustomer(db)

@router.get('/getAllBrickCustomer')
def get_all_brick_customer(db: Session=Depends(database.get_db)):
    return master_data.listOfBrickCustomer(db)

@router.get('/getAllProductInvoice')
def get_all_product_invoice(db: Session=Depends(database.get_db)):
    return master_data.listOfProductInvoice(db)

@router.post('/createEmployeeSapRecord')
def create_Employee_Sap_Record(request: master_schemas.Employees_Sap_Record, db: Session=Depends(database.get_db)):
    return master_data.createEmployeeSapRecord(request, db)

@router.get('/getAllEmployeeSapRecord')
def get_all_employee_sap_record(db: Session=Depends(database.get_db)):
    return master_data.listOfBrick(db)
