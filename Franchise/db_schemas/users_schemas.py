from datetime import date
from typing import Optional
from pydantic import BaseModel

class User(BaseModel):
    company_code: str
    email: str
    distributor_id: str
    username: str
    password: str
    created_at: date
    status: bool
    store_name: str
    role_id: str
    location_id: str

class ShowUser(BaseModel):
    company_code: str
    email: str
    distributor_id: str
    created_at: date
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
