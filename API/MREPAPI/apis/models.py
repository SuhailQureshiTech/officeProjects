from datetime import datetime
from sqlite3 import Timestamp
from sqlalchemy import Column, VARCHAR, Date, ForeignKey, Integer, Boolean, DateTime
from apis.database import Base
from sqlalchemy.sql import func

class Company(Base):
    __tablename__ = 'COMPANIES'

    company_id=Column(Integer, primary_key=True, index=True)
    company_sap_code=Column(VARCHAR(50))
    company_description=Column(VARCHAR(100))
    create_date=Column(Date)
    status=Column(Boolean)
    created_by=Column(VARCHAR(50))
    mrep_id=Column(VARCHAR(50))
    sas_id=Column(VARCHAR(50))

class Business_Unit(Base):
    __tablename__ = 'BUSINESS_UNITS'

    business_unit_id=Column(Integer, primary_key=True, index=True)
    business_unit_description=Column(VARCHAR(100))
    create_date=Column(Date)
    status=Column(Boolean)
    created_by=Column(VARCHAR(50))
    mrep_id=Column(VARCHAR(50))
    sas_id=Column(VARCHAR(50))
    emp_sap_id=Column(VARCHAR(50))
    emp_name=Column(VARCHAR(50))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Teams(Base):
    __tablename__ = 'TEAMS'

    team_id=Column(Integer, primary_key=True, index=True)
    team_description=Column(VARCHAR(100))
    create_date=Column(Date)
    status=Column(Boolean)
    created_by=Column(VARCHAR(50))
    mrep_id=Column(VARCHAR(50))
    sas_id=Column(VARCHAR(50))
    emp_sap_id=Column(VARCHAR(50))
    emp_name=Column(VARCHAR(50))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))
    business_unit_id=Column(Integer, ForeignKey('BUSINESS_UNITS.business_unit_id'))

class Brand(Base):
    __tablename__ = 'BRANDS'

    brand_id=Column(Integer, primary_key=True, index=True)
    brand_description=Column(VARCHAR(100))
    create_date=Column(Date)
    status=Column(Boolean)
    created_by=Column(VARCHAR(100))
    mrep_id=Column(VARCHAR(100))
    sas_id=Column(VARCHAR(100))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Product(Base):
    __tablename__ = 'PRODUCTS'

    product_id=Column(Integer, primary_key=True, index=True)
    product_description=Column(VARCHAR(100))
    create_date=Column(Date)
    status=Column(Boolean)
    prod_status=Column(Boolean)
    created_by=Column(VARCHAR(100))
    prod_sap_code=Column(VARCHAR(100))
    mrep_prod_id=Column(VARCHAR(100))
    sas_prod_id=Column(VARCHAR(100))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class City(Base):
    __tablename__ = 'CITIES'

    city_id=Column(Integer, primary_key=True, index=True)
    city_name=Column(VARCHAR(100))
    city_short_name=Column(VARCHAR(100))

class Branch(Base):
    __tablename__ = 'BRANCHES'

    branch_id=Column(Integer, primary_key=True, index=True)
    branch_description=Column(VARCHAR(100))
    create_date=Column(Date)
    status=Column(Boolean)
    latitude=Column(VARCHAR(100))
    longitude=Column(VARCHAR(100))
    created_by=Column(VARCHAR(100))
    branch_sap_id=Column(VARCHAR(100))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))
    city_id=Column(Integer, ForeignKey('CITIES.city_id'))

class Designation(Base):
    __tablename__ = 'DESIGNATION'

    designation_id=Column(Integer, primary_key=True, index=True)
    designation_description=Column(VARCHAR(100))
    create_date=Column(Date)
    status=Column(Boolean)
    created_by=Column(VARCHAR(100))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))
    is_activity_approver=Column(Boolean)

class Employee_Sap_Record(Base):
    __tablename__= 'EMPLOYEES_SAP_RECORD'

    emp_id=Column(Integer, primary_key=True, index=True)
    name=Column(VARCHAR(50))
    line_manager=Column(VARCHAR(50))
    joining_date=Column(Date)
    cnic=Column(VARCHAR(50))
    email=Column(VARCHAR(50))
    contact_number=Column(VARCHAR(50))
    gender=Column(VARCHAR(20))
    status=Column(Boolean)
    emp_sap_id=Column(VARCHAR(25))
    company_description=Column(VARCHAR(50))
    city_description=Column(VARCHAR(50))
    designation_description=Column(VARCHAR(50))
    team_name=Column(VARCHAR(50))

class Brick(Base):
    __tablename__ = 'BRICKS'

    brick_id=Column(Integer, primary_key=True, index=True)
    brick_description=Column(VARCHAR(100))
    create_date=Column(Date)
    status=Column(Boolean)
    created_by=Column(VARCHAR(100))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))
    branch_id=Column(Integer,ForeignKey('BRANCHES.branch_id'))

class Region(Base):
    __tablename__ = 'REGIONS'

    region_id=Column(Integer, primary_key=True, index=True)
    region_description=Column(VARCHAR(100))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Territory(Base):
    __tablename__ = 'TERRITORIES'

    territory_id=Column(Integer,  index=True)
    sas_id=Column(VARCHAR(10),primary_key=True,)
    mrep_id=Column(VARCHAR(10))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))
    designation_id=Column(Integer, ForeignKey('DESIGNATION.designation_id'))
    team_id=Column(Integer, ForeignKey('TEAMS.team_id'))



class Zone(Base):
    __tablename__ = 'ZONES'

    zone_id=Column(Integer, primary_key=True, index=True)
    zone_description=Column(VARCHAR(100))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Doctor(Base):
    __tablename__ = 'DOCTORS'

    doctor_id=Column(Integer, index=True)
    doctor_name=Column(VARCHAR(100))
    doctor_sas_id=Column(VARCHAR(100), primary_key=True,)
    PMDC_Number=Column(VARCHAR(100))
    speciality_id=Column(VARCHAR(100))
    address=Column(VARCHAR(100))
    contact_number=Column(VARCHAR(100))
    qualification=Column(VARCHAR(100))
    designation=Column(VARCHAR(100))
    latitude=Column(VARCHAR(50))
    longitude=Column(VARCHAR(50))
    city_id=Column(Integer, ForeignKey('CITIES.city_id'))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Distributor(Base):
    __tablename__ = 'DISTRIBUTORS'

    distributor_id=Column(Integer, primary_key=True, index=True)
    distributor_description=Column(VARCHAR(100))
    branch_id=Column(Integer,ForeignKey('BRANCHES.branch_id'))
    city_id=Column(Integer, ForeignKey('CITIES.city_id'))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Product_Brand(Base):
    __tablename__ = 'PRODUCTS_BRANDS_MAPPING'

    product_brand_id=Column(Integer, primary_key=True, index=True)
    start_date=Column(Date)
    end_date=Column(Date)
    brand_id=Column(Integer,ForeignKey('BRANDS.brand_id'))
    product_id=Column(Integer, ForeignKey('PRODUCTS.product_id'))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Product_Team(Base):
    __tablename__ = 'PRODUCTS_TEAMS_MAPPING'

    product_team_id=Column(Integer, primary_key=True, index=True)
    start_date=Column(Date)
    end_date=Column(Date)
    team_id=Column(Integer, ForeignKey('TEAMS.team_id'))
    product_id=Column(Integer, ForeignKey('PRODUCTS.product_id'))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Product_Price(Base):
    __tablename__ = 'PRODUCTS_PRICES_MAPPING'

    product_price_id=Column(Integer, primary_key=True, index=True)
    start_date=Column(Date)
    end_date=Column(Date)
    efp=Column(VARCHAR(50))
    tp=Column(VARCHAR(50))
    cp=Column(VARCHAR(50))
    mrp=Column(VARCHAR(50))
    product_id=Column(Integer, ForeignKey('PRODUCTS.product_id'))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Customer_Type(Base):
    __tablename__ = 'CUSTOMER_TYPE'

    customer_type_id=Column(Integer, primary_key=True, index=True)
    customer_type_description=Column(VARCHAR(100))

class Customer(Base):
    __tablename__ = 'CUSTOMERS'

    customer_id=Column(Integer, primary_key=True, index=True)
    customer_description=Column(VARCHAR(100))
    first_address=Column(VARCHAR(50))
    second_address=Column(VARCHAR(50))
    contact_number=Column(VARCHAR(20))
    latitude=Column(VARCHAR(100))
    longitude=Column(VARCHAR(100))
    email=Column(VARCHAR(20))
    creation_date=Column(Date)
    status=Column(Boolean)
    customer_type_id=Column(Integer, ForeignKey('CUSTOMER_TYPE.customer_type_id'))
    branch_id=Column(Integer, ForeignKey('BRANCHES.branch_id'))
    brick_id=Column(Integer, ForeignKey('BRICKS.brick_id'))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Iqvia_brick(Base):
     __tablename__= 'IQVIA_BRICK'

     iqvia_id=Column(Integer, primary_key=True, index=True)
     iqvia_brick_id=Column(VARCHAR(50))
     iqvia_brick_name=Column(VARCHAR(50))
     iqvia_customer_id=Column(VARCHAR(50))
     iqvia_customer_name=Column(VARCHAR(50))
     address=Column(VARCHAR(50))
     area=Column(VARCHAR(50))
     city=Column(VARCHAR(50))

class Brick_Customer_Map(Base):
     __tablename__ = 'BRICK_CUSTOMER_MAPPING'

     brick_customer_id=Column(Integer, primary_key=True, index=True)
     start_date=Column(Date)
     end_date=Column(Date)
     iqvia_id=Column(Integer, ForeignKey('IQVIA_BRICK.iqvia_id'))
     brick_id=Column(Integer, ForeignKey('BRICKS.brick_id'))
     customer_id=Column(Integer, ForeignKey('CUSTOMERS.customer_id'))
     branch_id=Column(Integer, ForeignKey('BRANCHES.branch_id'))
     company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))

class Product_Invoice_Map(Base):
    __tablename__ = 'PRODUCT_INVOICE_MAPPING'

    product_invoice_id=Column(Integer, primary_key=True, index=True)
    invoice_product_id=Column(VARCHAR(10))
    sas_product_id=Column(VARCHAR(10))
    mrep_product_id=Column(VARCHAR(10))
    distributor_id=Column(Integer, ForeignKey('DISTRIBUTORS.distributor_id'))
    product_id=Column(Integer, ForeignKey('PRODUCTS.product_id'))

class Activity(Base):
    __tablename__ = 'ACTIVITY'

    activity_id=Column(Integer, primary_key=True)
    activity_description=Column(VARCHAR(50))
    activity_type=Column(VARCHAR(50))
    activity_initiate_for=Column(VARCHAR(50))
    activity_initiate_by=Column(VARCHAR(50))
    current_date=Column(Date)
    activity_date=Column(Date)
    activity_amount=Column(Integer)
    payment_term=Column(Boolean)
    is_complete=Column(VARCHAR(255))
    approved_amount=Column(Integer)
    expense_amount=Column(Integer)
    mie_id=Column(VARCHAR(50))
    remarks=Column(VARCHAR(100))
    created_at=Column(DateTime(timezone=True), default=func.now())

class Activity_Product_Group(Base):
    __tablename__ = 'ACTIVITY_PRODUCT'

    activity_product_id=Column(Integer, primary_key=True, index=True)
    activity_product_name=Column(VARCHAR(50))
    activity_product_percentage=Column(VARCHAR(50))

    activity_id=Column(Integer, ForeignKey('ACTIVITY.activity_id'))

class Activity_Doctor_Group(Base):
    __tablename__ = 'ACTIVITY_DOCTOR'

    activity_doctor_id=Column(Integer, primary_key=True, index=True)
    activity_doctor_name=Column(VARCHAR(50))
    activity_doctor_comments=Column(VARCHAR(100))
    activity_doctor_percentage=Column(VARCHAR(50))

    activity_id=Column(Integer, ForeignKey('ACTIVITY.activity_id'))

class Employees(Base):
    __tablename__= 'EMPLOYEES'

    emp_id=Column(Integer, index=True)
    emp_sap_id=Column(VARCHAR(50), primary_key=True)
    emp_sas_id=Column(VARCHAR(50))
    emp_mrep_id=Column(VARCHAR(50))
    emp_name=Column(VARCHAR(50))
    hr_id=Column(VARCHAR(50))
    line_manager_sas_id=Column(VARCHAR(50))
    joining_date=Column(Date)
    gender=Column(VARCHAR(20))
    contact_number=Column(VARCHAR(20))
    territory_id=Column(VARCHAR, ForeignKey('TERRITORIES.sas_id'))
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))
    business_unit_id=Column(Integer, ForeignKey('BUSINESS_UNITS.business_unit_id'))
    team_id=Column(Integer, ForeignKey('TEAMS.team_id'))
    branch_id=Column(Integer, ForeignKey('BRANCHES.branch_id'))
    designation_id=Column(Integer, ForeignKey('DESIGNATION.designation_id'))
    incentive_status=Column(Boolean)
    status=Column(Boolean)
    rfi_status=Column(Boolean)
    email=Column(VARCHAR(100))

class Employees_territory_mapping(Base):
    __tablename__= 'EMPLOYEES_TERRITORY_MAPPING'

    emp_territory_map_id=Column(Integer, primary_key=True, index=True)
    emp_id=Column(VARCHAR, ForeignKey('EMPLOYEES.emp_sap_id'))
    line_manager=Column(VARCHAR, ForeignKey('EMPLOYEES.emp_sap_id'))
    territory_id=Column(VARCHAR, ForeignKey('TERRITORIES.sas_id'))
    start_date=Column(Date)
    end_date=Column(Date)
    status=Column(Boolean)



class Users_Role(Base):
    __tablename__= 'USERS_ROLE'

    role_id=Column(Integer, primary_key=True, index=True)
    role_description=Column(VARCHAR(50))
    create_date=Column(Date)
    created_by=Column(Integer)
    status=Column(Boolean)

class Role_permission(Base):
    __tablename__= 'ROLE_PERMISSION'

    role_permission_id=Column(Integer, primary_key=True, index=True)
    form_id=Column(Integer, ForeignKey('FORM.form_id'))
    role_id=Column(Integer, ForeignKey('USERS_ROLE.role_id'))
    create=Column(Boolean)
    view=Column(Boolean)
    edit=Column(Boolean)
    delete=Column(Boolean)
    create_date=Column(Date)
    created_by=Column(Integer)
    status=Column(Boolean)

class Form(Base):
    __tablename__= 'FORM'

    form_id=Column(Integer, primary_key=True, index=True)
    form_description=Column(VARCHAR(50))
    form_url=Column(VARCHAR(50))
    create_date=Column(Date)
    created_by=Column(Integer)
    status=Column(Boolean)

class Users(Base):
    __tablename__= 'USERS'

    user_id=Column(Integer, primary_key=True, index=True)
    user_name=Column(VARCHAR(100))
    email=Column(VARCHAR(100))
    password=Column(VARCHAR(100))
    create_date=Column(Date)
    company_id=Column(Integer, ForeignKey('COMPANIES.company_id'))
    emp_sap_id=Column(VARCHAR, ForeignKey('EMPLOYEES.emp_sap_id'))
    territory_id=Column(VARCHAR, ForeignKey('TERRITORIES.sas_id'))
    status=Column(Boolean)
    role_id=Column(Integer, ForeignKey('USERS_ROLE.role_id'))
    created_by=Column(Integer)


class Doctor_Mie_Allocation(Base):
    __tablename__= 'DOCTOR_MIE_ALLOCATION'

    doctor_mie_id=Column(Integer, primary_key=True, index=True)
    mie_sap_id=Column(VARCHAR, ForeignKey('EMPLOYEES.emp_sap_id'))
    mie_sas_id=Column(VARCHAR(100))
    doctor_sas_id=Column(VARCHAR, ForeignKey('DOCTORS.doctor_sas_id'))
    customer_id	=Column(Integer)
    brick_id=Column(Integer)
    status=Column(Boolean)
    latitude=Column(VARCHAR(100))
    longitude=Column(VARCHAR(100))
    doctor_class=Column(VARCHAR(100))

class Activity_Sms_Log(Base):
    __tablename__= 'ACTIVITY_SMS_LOG'

    sms_log_id=Column(Integer, primary_key=True, index=True)
    contact_number=Column(VARCHAR(20))
    emp_sap_id=Column(VARCHAR(20))
    activity_no=Column(Integer)
    sms_date=Column(Date)
    latitude=Column(VARCHAR(100))
    longitude=Column(VARCHAR(100))
    amount=Column(VARCHAR(20))

class Activity_Status(Base):
    __tablename__= 'ACTIVITY_STATUS'

    activity_status_id=Column(Integer, primary_key=True, index=True)
    activity_id=Column(Integer, ForeignKey('ACTIVITY.activity_id'))
    approval=Column(Boolean)
    approved_by=Column(VARCHAR, ForeignKey('EMPLOYEES.emp_sap_id'))
    approver_designation_id=Column(Integer, ForeignKey('DESIGNATION.designation_id'))






