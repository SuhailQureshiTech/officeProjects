# from io import BytesIO
from fastai import HTTPException, status
from sqlalchemy.orm import Session
from apis.db_schemas import master_schemas
from apis import models
# import pandas as pd

def createCompany(request: master_schemas.Company, db:Session):
    new_company = db.query(models.Company).filter(models.Company.company_description == request.company_description).first()
    if new_company != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Company name {request.company_description} is already exist")
    add_new_company = models.Company(
        company_description=request.company_description,
        create_date=request.create_date,
        status=request.status,
        created_by=request.created_by,
        mrep_id=request.mrep_id,
        sas_id=request.sas_id
    )
    db.add(add_new_company)
    db.commit()
    db.refresh(add_new_company)
    return add_new_company

def listOfCompanies(db:Session):
    all_companies = db.query(models.Company).all()
    return all_companies

def createBusinessUnit(request: master_schemas.Business_Unit, db:Session):
    new_business_unit = db.query(models.Business_Unit).filter(models.Business_Unit.business_unit_description == request.business_unit_description).first()
    if new_business_unit != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Business Unit name {request.business_unit_description} is already exist")
    add_new_business_unit = models.Business_Unit(
        business_unit_description=request.business_unit_description,
        create_date=request.create_date,
        status=request.status,
        created_by=request.created_by,
        mrep_id=request.mrep_id,
        sas_id=request.sas_id,
        company_id=request.company_id
    )
    db.add(add_new_business_unit)
    db.commit()
    db.refresh(add_new_business_unit)
    return add_new_business_unit

def listOfBusinessUnit(db:Session):
    all_business_unit = db.query(models.Business_Unit).all()
    return all_business_unit

def createTeam(request: master_schemas.Teams, db:Session):
    new_team = db.query(models.Teams).filter(models.Teams.team_description == request.team_description).first()
    if new_team != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Team {request.team_description} is already exist")
    add_new_team = models.Teams(
        team_description=request.team_description,
        create_date=request.create_date,
        status=request.status,
        created_by=request.created_by,
        mrep_id=request.mrep_id,
        sas_id=request.sas_id,
        business_unit_id=request.business_unit_id,
        company_id=request.company_id
    )
    db.add(add_new_team)
    db.commit()
    db.refresh(add_new_team)
    return add_new_team

def listOfTeam(db:Session):
    all_team = db.query(models.Teams).all()
    return all_team


def createBrand(request: master_schemas.Brand, db:Session):
    new_brand = db.query(models.Brand).filter(models.Brand.brand_description == request.brand_description).first()
    if new_brand != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Brand {request.brand_description} is already exist")
    add_new_brand = models.Brand(
        brand_description=request.brand_description,
        create_date=request.create_date,
        status=request.status,
        created_by=request.created_by,
        mrep_id=request.mrep_id,
        sas_id=request.sas_id,
        company_id=request.company_id
    )
    db.add(add_new_brand)
    db.commit()
    db.refresh(add_new_brand)
    return add_new_brand

def listOfBrand(db:Session):
    all_brand = db.query(models.Brand).all()
    return all_brand

def createProduct(request: master_schemas.Product, db:Session):
    new_product = db.query(models.Product).filter(models.Product.product_description == request.product_description).first()
    if new_product != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Product {request.product_description} is already exist")
    add_new_product = models.Product(
        product_description=request.product_description,
        product_sap_code=request.product_sap_code,
        product_status=request.product_status,
        create_date=request.create_date,
        status=request.status,
        created_by=request.created_by,
        mrep_id=request.mrep_id,
        sas_id=request.sas_id,
        company_id=request.company_id
    )
    db.add(add_new_product)
    db.commit()
    db.refresh(add_new_product)
    return add_new_product

def listOfProduct(db:Session):
    all_product = db.query(models.Product).all()
    return all_product

def createCity(request: master_schemas.City, db:Session):
    new_city = db.query(models.City).filter(models.City.city_name == request.city_name).first()
    if new_city != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"City {request.city_name} is already exist")
    add_new_city = models.City(
        city_name=request.city_name,
        city_short_name=request.city_short_name
    )
    db.add(add_new_city)
    db.commit()
    db.refresh(add_new_city)
    return add_new_city

def listOfCity(db:Session):
    all_city = db.query(models.City).all()
    return all_city


def createBranch(request: master_schemas.Branch, db:Session):
    new_branch = db.query(models.Branch).filter(models.Branch.branch_description == request.branch_description).first()
    if new_branch != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Branch {request.branch_description} is already exist")
    add_new_branch = models.Branch(
        branch_description=request.branch_description,
        branch_sap_id=request.branch_sap_id,
        create_date=request.create_date,
        status=request.status,
        latitude=request.latitude,
        longitude=request.longitude,
        created_by=request.created_by,
        company_id=request.company_id,
        city_id=request.city_id
    )
    db.add(add_new_branch)
    db.commit()
    db.refresh(add_new_branch)
    return add_new_branch

def listOfBranch(db:Session):
    all_branch = db.query(models.Branch).all()
    return all_branch

def createDesignation(request: master_schemas.Designation, db:Session):
    new_designation = db.query(models.Designation).filter(models.Designation.designation_description == request.designation_description).first()
    if new_designation != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Designation {request.designation_description} is already exist")
    add_new_designation = models.Designation(
        designation_description=request.designation_description,
        create_date=request.create_date,
        status=request.status,
        created_by=request.created_by,
        company_id=request.company_id
    )
    db.add(add_new_designation)
    db.commit()
    db.refresh(add_new_designation)
    return add_new_designation

def listOfDesignation(db:Session):
    all_designation = db.query(models.Designation).all()
    return all_designation

def createEmployee(request: master_schemas.Employees, db:Session):
    new_employee = db.query(models.Employees).filter(models.Employees.emp_name == request.emp_name).first()
    if new_employee != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Employee {request.emp_name} is already exist")
    add_new_employee = models.Employees(
        emp_sap_id=request.emp_sap_id,
        emp_name=request.emp_name,
        hr_id=request.hr_id,
        joining_date=request.joining_date,
        gender=request.gender,
        sap_record_id=request.sap_record_id,
        territory_id=request.territory_id,
        company_id=request.company_id,
        business_unit_id=request.business_unit_id,
        team_id=request.team_id,
        branch_id=request.branch_id,
        designation_id=request.designation_id,
        zone_id=request.zone_id,
        contact_number=request.contact_number,
        first_address=request.first_address,
        second_address=request.second_address,
        incentive_status=request.incentive_status,
        status=request.status,
        rfi_status=request.rfi_status

    )
    db.add(add_new_employee)
    db.commit()
    db.refresh(add_new_employee)
    return add_new_employee

def listOfEmployee(db:Session):
    all_employee = db.query(models.Employees).all()
    return all_employee

def createBrick(request: master_schemas.Brick, db:Session):
    new_brick = db.query(models.Brick).filter(models.Brick.brick_description == request.brick_description).first()
    if new_brick != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Brick {request.brick_description} is already exist")
    add_new_brick = models.Brick(

        brick_description=request.brick_description,
        create_date=request.create_date,
        status=request.status,
        created_by=request.created_by,
        company_id=request.company_id,
        branch_id=request.branch_id
    )
    db.add(add_new_brick)
    db.commit()
    db.refresh(add_new_brick)
    return add_new_brick

def listOfBrick(db:Session):
    all_brick = db.query(models.Brick).all()
    return all_brick

def createRegion(request: master_schemas.Region, db:Session):
    new_region = db.query(models.Region).filter(models.Region.region_description == request.region_description).first()
    if new_region != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Region {request.region_description} is already exist")
    add_new_region = models.Region(
         region_description=request.region_description,
        company_id=request.company_id
    )
    db.add(add_new_region)
    db.commit()
    db.refresh(add_new_region)
    return add_new_region

def listOfRegion(db:Session):
    all_region = db.query(models.Region).all()
    return all_region

def createTerritory(request: master_schemas.Territory, db:Session):
    new_territory = db.query(models.Territory).filter(models.Territory.territory_description == request.territory_description).first()
    if new_territory != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Territory {request.territory_description} is already exist")
    add_new_territory = models.Territory(
        territory_description=request.territory_description,
        company_id=request.company_id
    )
    db.add(add_new_territory)
    db.commit()
    db.refresh(add_new_territory)
    return add_new_territory

def listOfTerritory(db:Session):
    all_territory = db.query(models.Territory).all()
    return all_territory

def createZone(request: master_schemas.Zone, db:Session):
    new_zone = db.query(models.Zone).filter(models.Zone.zone_description == request.zone_description).first()
    if new_zone != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Zone {request.zone_description} is already exist")
    add_new_zone = models.Zone(
        zone_description=request.zone_description,
        company_id=request.company_id
    )
    db.add(add_new_zone)
    db.commit()
    db.refresh(add_new_zone)
    return add_new_zone

def listOfZone(db:Session):
    all_zone = db.query(models.Zone).all()
    return all_zone

def createDoctor(request: master_schemas.Doctor, db:Session):
    new_doctor = db.query(models.Doctor).filter(models.Doctor.doctor_name == request.doctor_name).first()
    if new_doctor != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Doctor {request.doctor_name} is already exist")
    add_new_doctor = models.Doctor(
        doctor_name=request.doctor_name,
        doctor_sas_id=request.doctor_sas_id,
        PMDC_Number=request.PMDC_Number,
        speciality_id=request.speciality_id,
        address=request.address,
        contact_number=request.contact_number,
        qualification=request.qualification,
        designation=request.designation,
        latitude=request.latitude,
        longitude=request.longitude,
        city_id=request.city_id,
        company_id=request.company_id
    )
    db.add(add_new_doctor)
    db.commit()
    db.refresh(add_new_doctor)
    return add_new_doctor

def listOfDoctor(db:Session):
    all_doctor = db.query(models.Doctor).all()
    return all_doctor

def createDistributor(request: master_schemas.Distributor, db:Session):
    new_distributor = db.query(models.Distributor).filter(models.Distributor.distributor_description == request.distributor_description).first()
    if new_distributor != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Distributor {request.distributor_description} is already exist")
    add_new_distributor = models.Distributor(
         distributor_description=request.distributor_description,
        branch_id=request.branch_id,
        city_id=request.city_id,
        company_id=request.company_id
    )
    db.add(add_new_distributor)
    db.commit()
    db.refresh(add_new_distributor)
    return add_new_distributor

def listOfDistributor(db:Session):
    all_disrtbutor = db.query(models.Distributor).all()
    return all_disrtbutor


def createProductBrand(request: master_schemas.Product_Brand, db:Session):
    new_product_brand = db.query(models.Product_Brand).filter(models.Product_Brand.product_id == request.product_id).first()
    if new_product_brand != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Product  {request.product_id} is already mapped with another brand")
    add_new_product_brand = models.Product_Brand(
        start_date=request.start_date,
        end_date=request.end_date,
        brand_id=request.brand_id,
        product_id=request.product_id,
        company_id=request.company_id
    )
    db.add(add_new_product_brand)
    db.commit()
    db.refresh(add_new_product_brand)
    return add_new_product_brand

def listOfProductBrand(db:Session):
    all_product_brand = db.query(models.Product_Brand).all()
    return all_product_brand

def createProductTeam(request: master_schemas.Product_Team, db:Session):
    new_product_team = db.query(models.Product_Team).filter(models.Product_Team.product_id == request.product_id).first()
    if new_product_team != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Product  {request.product_id} is already mapped with another team")
    add_new_product_team = models.Product_Team(
        start_date=request.start_date,
        end_date=request.end_date,
        team_id=request.team_id,
        product_id=request.product_id,
        company_id=request.company_id
    )
    db.add(add_new_product_team)
    db.commit()
    db.refresh(add_new_product_team)
    return add_new_product_team

def listOfProductTeam(db:Session):
    all_product_team = db.query(models.Product_Team).all()
    return all_product_team

def createProductPrice(request: master_schemas.Product_Price, db:Session):
    new_product_price = db.query(models.Product_Price).filter(models.Product_Price.product_id == request.product_id).first()
    if new_product_price != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Product  {request.product_id} is already mapped with another team")
    add_new_product_price = models.Product_Price(
        start_date=request.start_date,
        end_date=request.end_date,
        team_id=request.team_id,
        product_id=request.product_id,
        company_id=request.company_id
    )
    db.add(add_new_product_price)
    db.commit()
    db.refresh(add_new_product_price)
    return add_new_product_price

def listOfProductPrice(db:Session):
    all_product_price = db.query(models.Product_Price).all()
    return all_product_price

def createCustomerType(request: master_schemas.Customer_Type, db:Session):
    new_customer_type = db.query(models.Customer_Type).filter(models.Customer_Type.customer_type_description == request.customer_type_description).first()
    if new_customer_type != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Customer Type  {request.customer_type_description} is already created")
    add_new_customer_type = models.Customer_Type(
        customer_type_description=request.customer_type_description
    )
    db.add(add_new_customer_type)
    db.commit()
    db.refresh(add_new_customer_type)
    return add_new_customer_type

def listOfCustomerType(db:Session):
    all_customer_type= db.query(models.Customer_Type).all()
    return all_customer_type

def createCustomer(request: master_schemas.Customer, db:Session):
    new_customer = db.query(models.Customer).filter(models.Customer.customer_description == request.customer_description).first()
    if new_customer != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Customer  {request.customer_description} is already Created")
    add_new_customer = models.Customer(
        customer_description=request.customer_description,
        first_address=request.first_address,
        second_address=request.second_address,
        contact_number=request.contact_number,
        latitude=request.latitude,
        longitude=request.longitude,
        email=request.email,
        Iqvia_brick_id=request.Iqvia_brick_id,
        creation_date=request.creation_date,
        status=request.status,
        customer_type_id=request.customer_type_id,
        branch_id=request.branch_id,
        brick_id=request.brick_id,
        company_id=request.company_id
    )
    db.add(add_new_customer)
    db.commit()
    db.refresh(add_new_customer)
    return add_new_customer

def listOfCustomer(db:Session):
    all_customer = db.query(models.Customer).all()
    return all_customer

def createBrickCustomer(request: master_schemas.Brick_Customer_Map, db:Session):
    new_brick_customer_map = db.query(models.Brick_Customer_Map).filter(models.Brick_Customer_Map.customer_id == request.customer_id).first()
    if new_brick_customer_map != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Customer  {request.customer_id} is already mapped with another Brick")
    add_new_brick_customer_map = models.Brick_Customer_Map(
        start_date=request.start_date,
        end_date=request.end_date,
        brick_id=request.brick_id,
        customer_id=request.customer_id,
        branch_id=request.branch_id,
        company_id=request.company_id
    )
    db.add(add_new_brick_customer_map)
    db.commit()
    db.refresh(add_new_brick_customer_map)
    return add_new_brick_customer_map

def listOfBrickCustomer(db:Session):
    all_brick_customer = db.query(models.Brick_Customer_Map).all()
    return all_brick_customer

def createProductInvoice(request: master_schemas.Product_Invoice_Map, db:Session):
    new_product_invoice_map = db.query(models.Product_Invoice_Map).filter(models.Product_Invoice_Map.product_id == request.product_id).first()
    if new_product_invoice_map != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Product  {request.product_id} is already mapped with another team")
    add_new_product_invoice_map = models.Product_Invoice_Map(
        invoice_product_id=request.invoice_product_id,
        sas_product_id=request.sas_product_id,
        mrep_product_id=request.mrep_product_id,
        distributor_id=request.distributor_id,
        product_id=request.product_id
    )
    db.add(add_new_product_invoice_map)
    db.commit()
    db.refresh(add_new_product_invoice_map)
    return add_new_product_invoice_map

def listOfProductInvoice(db:Session):
    all_product_invoice = db.query(models.Product_Invoice_Map).all()
    return all_product_invoice

def createEmployeeSapRecord(request: master_schemas.Employees_Sap_Record, db:Session):
    new_employee_sap_record = db.query(models.Employee_Sap_Record).filter(models.Employee_Sap_Record.name == request.name).first()
    if new_employee_sap_record != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Employee {request.name} is already exist")
    add_new_employee_sap_record = models.Employee_Sap_Record(
        name=request.name,
        line_manager=request.line_manager,
        joining_date=request.joining_date,
        cnic=request.cnic,
        email=request.email,
        contact_number=request.contact_number,
        gender=request.gender,
        status=request.status,
        emp_sap_id=request.emp_sap_id,
        company_description=request.company_description,
        city_description=request.city_description,
        designation_description=request.designation_description,
        team_name=request.team_name
    )
    db.add(add_new_employee_sap_record)
    db.commit()
    db.refresh(add_new_employee_sap_record)
    return add_new_employee_sap_record

def listOfEmployeeSapRecord(db:Session):
    all_employee_sap_record = db.query(models.Employee_Sap_Record).all()
    return all_employee_sap_record


