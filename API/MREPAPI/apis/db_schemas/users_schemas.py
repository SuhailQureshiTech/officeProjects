from datetime import date
from typing import Optional
from pydantic import BaseModel

class User(BaseModel):
    user_name: str
    email: str
    password: str
    create_date: date
    company_id: int
    emp_sap_id: str
    territory_id: str
    status: bool
    role_id: int
    created_by: int

class ShowUser(BaseModel):
    email: str
    user_name: str
    create_date: date
    status: bool
    class Config():
        orm_mode=True

class Login(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str
    
class TokenData(BaseModel):
    distributor_id: Optional[str] = None

class Users_Role(BaseModel):

    role_description: str
    create_date: date
    created_by: int
    status: bool

class Form(BaseModel):

    form_description: str
    form_url: str
    create_date: date
    created_by: int
    status: bool

class Role_permission(BaseModel):

    form_id: int
    role_id: int
    create: bool
    view: bool
    edit: bool
    delete: bool
    create_date: date
    created_by: int
    status: bool
