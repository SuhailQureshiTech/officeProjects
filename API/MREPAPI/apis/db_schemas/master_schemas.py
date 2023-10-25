from datetime import date
from pydantic import BaseModel
from typing import List

class Company(BaseModel):
    company_description: str
    create_date: date
    status: bool
    created_by: str
    mrep_id: str
    sas_id: str

class Business_Unit(BaseModel):
    business_unit_description: str
    create_date: date
    status: bool
    created_by: str
    mrep_id: str
    sas_id: str
    company_id: int

class Teams(BaseModel):
    team_description: str
    create_date: date
    status: bool
    created_by: str
    mrep_id: str
    sas_id: str
    business_unit_id: str
    company_id: str
    
class Brand(BaseModel):
    brand_description: str
    create_date: date
    status: bool
    created_by: str
    mrep_id: str
    sas_id: str
    company_id: str
    
class Product(BaseModel):
    product_description: str
    product_sap_code: str
    product_status: str
    create_date: date
    status: bool
    created_by: str
    mrep_id: str
    sas_id: str
    company_id: str

class City(BaseModel):
    city_name: str
    city_short_name: str

class Branch(BaseModel):
    branch_description: str
    create_date: date
    status: bool
    latitude: str
    longitude: str
    created_by: str
    branch_sap_id: str
    company_id: int
    city_id: int

class Designation(BaseModel):    
    designation_description: str
    create_date: date
    status: bool
    created_by: str
    company_id: int

class Employees(BaseModel): 

    emp_sap_id: int
    emp_name: str
    hr_id: str
    joining_date: date
    gender: str
    sap_record_id: int
    territory_id: int
    company_id: int
    business_unit_id: int
    team_id: int
    branch_id: int
    designation_id: int
    zone_id: int
    contact_number: str
    first_address: str
    second_address: str
    incentive_status: bool
    status: bool
    rfi_status: bool


class Brick(BaseModel):    
    brick_description: str
    create_date: date
    status: bool
    created_by: str
    company_id: int
    branch_id: int

class Region(BaseModel):
    region_description: str
    company_id: int

class Territory(BaseModel):
    territory_description: str
    company_id: int

class Zone(BaseModel):
    zone_description: str
    company_id: int

class Doctor(BaseModel):
    doctor_name: str
    doctor_sas_id: str
    PMDC_Number: str
    speciality_id: str
    address: str
    contact_number: str
    qualification: str
    designation: str
    latitude: str
    longitude: str
    city_id: int
    company_id: int

class Distributor(BaseModel):
    distributor_description: str
    branch_id: int
    city_id: int
    company_id: int

class Product_Brand(BaseModel):
    start_date: date
    end_date: date
    brand_id: int
    product_id: int
    company_id: int

class Product_Team(BaseModel):
    start_date: date
    end_date: date
    team_id: int
    product_id: int
    company_id: int

class Product_Price(BaseModel):
    start_date: date
    end_date: date
    efp: str
    tp: str
    cp: str
    mrp: str
    product_id: int
    company_id: int

class Customer_Type(BaseModel):
    customer_type_description: str

class Customer(BaseModel):
    customer_description: str
    first_address: str
    second_address: str
    contact_number: str
    latitude: str
    longitude: str
    email: str
    Iqvia_brick_id: str
    creation_date: date
    status: bool
    customer_type_id= int
    branch_id: int
    brick_id: int
    company_id: int

class Brick_Customer_Map(BaseModel):
    start_date: date
    end_date: date
    brick_id: int
    customer_id: int
    branch_id: int
    company_id: int

class Product_Invoice_Map(BaseModel):
    invoice_product_id: str
    sas_product_id: str
    mrep_product_id: str
    distributor_id: int
    product_id: int

class Employees_Sap_Record(BaseModel): 

    name: str
    line_manager: str
    joining_date: date
    cnic: str
    email: str
    contact_number: str
    gender: str
    status: bool
    emp_sap_id: str
    company_description: str
    city_description: str
    designation_description: str
    team_name: str
