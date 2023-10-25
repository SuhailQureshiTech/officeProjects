from datetime import date, datetime
from multiprocessing.dummy import Array
from typing import List
from pandas import array
from pydantic import BaseModel, EmailStr

class Activity(BaseModel):
    activity_id: int
    activity_description: str
    activity_type: str
    activity_initiate_for: str
    activity_initiate_by: str
    current_date: date
    activity_date: date
    activity_amount: int
    payment_term: bool
    is_complete: str
    approved_amount: int
    expense_amount: int
    mie_id: str
    remarks: str

    
class ActivityDoctor(BaseModel):
    doctorName: str
    comments: str
    expense: str
    
    class Config:
        orm_mode = True
        
class ActivityProduct(BaseModel):
    productName: str
    productExpense: str
    
    class Config:
        orm_mode = True

class ActivityStatus(BaseModel):
    activity_id: int
    approval: bool
    approved_by: str
    approver_designation_id: int
    remarks: str

class BulkActivityStatus(BaseModel):
    activity_ids: List[int]
    approval: bool
    approved_by: str
    approver_designation_id: int
   
class EmailSchema(BaseModel):
    email: List[EmailStr]
