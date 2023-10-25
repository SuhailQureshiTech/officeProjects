from typing import List
from fastai import APIRouter, Depends
from apis import database
from apis.db_schemas import activity_schemas
from sqlalchemy.orm import Session
from apis.repository import activity

router = APIRouter(
    prefix="/activity",
    tags=["Activity Management"]
)

@router.post('/initiateNewActivity')
async def create_activity(request: activity_schemas.Activity, db: Session=Depends(database.get_db)):
    result_activity = await activity.createActivity(request, db)
    return result_activity
    # return activity.createActivity(request, db)

@router.post('/initiateActivityDoctorExpense/{activity_id}')
def create_activity(activity_id, request: List[activity_schemas.ActivityDoctor], db: Session=Depends(database.get_db)):
    return activity.createActivityDoctorExpense(activity_id, request, db)

@router.post('/initiateActivityProductExpense/{activity_id}')
def create_activity(activity_id, request: List[activity_schemas.ActivityProduct], db: Session=Depends(database.get_db)):
    return activity.createActivityProductExpense(activity_id, request, db)

@router.get('/fetchListOfEmployeesByLM/{line_manager_sap_id}')
def get_employee_by_lm(line_manager_sap_id, db: Session=Depends(database.get_db)):
    return activity.listOfEmployeesByLM(line_manager_sap_id, db)

@router.get('/fetchListOfProductByEmpId/{emp_sap_id}')
def get_product_by_emp(emp_sap_id, db: Session=Depends(database.get_db)):
    return activity.listOfProductByEmp(emp_sap_id, db)

@router.get('/fetchListOfDoctorsByMie/{mie_sap_id}')
def get_doctors_by_mie(mie_sap_id, db: Session=Depends(database.get_db)):
    return activity.listOfDoctors(mie_sap_id, db)

@router.get('/fetchListOfActivityById/{activity_id}')
def get_activity_by_id(activity_id, db: Session=Depends(database.get_db)):
    return activity.listOfActivityById(activity_id, db)

@router.get('/fetchListOfActivity/{line_manager_sas_id}/{is_complete}')
def get_activity_by_lm(line_manager_sas_id,is_complete, db: Session=Depends(database.get_db)):
    return activity.listOfActivity(line_manager_sas_id,is_complete, db)

@router.get('/fetchListOfActivityProduct/{activity_id}')
def get_list_of_activity_doctor(activity_id, db:Session=Depends(database.get_db)):
    return activity.listOfActivityProducts(activity_id, db)

@router.get('/fetchListOfActivityDoctor/{activity_id}')
def get_list_of_activity_doctor(activity_id, db:Session=Depends(database.get_db)):
    return activity.listOfActivityDoctors(activity_id, db)

@router.post('/initiateNewActivityStatus')
async def create_activity_status(request: activity_schemas.ActivityStatus, db: Session=Depends(database.get_db)):
    result_activity_status = await activity.createActivityStatus(request, db)
    return result_activity_status

@router.post('/bulkActivityApprovals')
async def bulkApprovals(request: activity_schemas.BulkActivityStatus, db: Session=Depends(database.get_db)):
    result_bulk_approvals = await activity.bulkActivityStatus(request, db)
    return result_bulk_approvals

@router.put('/editActivityAmount/{activity_id}/{approval_amount}')
def edit_activity_amount(activity_id, approval_amount, db:Session=Depends(database.get_db)):
    return activity.editActivityAmount(activity_id, approval_amount, db)


@router.get('/fetchListOfActivityForReport/{line_manager_sas_id}/{is_complete}/{from_date}/{to_date}')
def get_activity_for_report(line_manager_sas_id,is_complete,from_date,to_date, db: Session=Depends(database.get_db)):
    return activity.listOfReportActivity(line_manager_sas_id,is_complete,from_date,to_date, db)

@router.get('/checkActivityStatus/{activity_id}')
def checkActivityById(activity_id, db: Session=Depends(database.get_db)):
    return activity.checkActivityStatus(activity_id, db)
