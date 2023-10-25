from datetime import datetime, date
import time
from statistics import mode
from typing import List
from fastai import HTTPException, status
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from sqlalchemy.orm import Session
from apis.db_schemas import activity_schemas
from apis import models
from starlette.responses import JSONResponse
import requests

url = "http://221.132.117.58:7700/sendsms_xml.html"

conf = ConnectionConfig(
    MAIL_USERNAME="dna@email.iblops.com",
    MAIL_PASSWORD="iblDna1!",
    MAIL_FROM="dna@email.iblops.com",
    MAIL_PORT=25,
    MAIL_SERVER="mail.iblgroup.biz",
    MAIL_FROM_NAME="Focal Activity Portal",
    MAIL_TLS=False,
    MAIL_SSL=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)

# html = """
#     http://localhost:3000/ViewActivityDetails
# <p>View Activity</p>
# """
html = """<pre> 
Go to the page to check your activity: <a href="http://focall.iblgrp.com/">click here</a>
Thanks,
Focall Activity.
</pre>"""


async def simple_send(email: activity_schemas.EmailSchema) -> JSONResponse:
    print('EMAIL', email)

    message = MessageSchema(
        subject="Focall Activity Portal",
        recipients=[email],
        body=html

    )

    fm = FastMail(conf)
    await fm.send_message(message)
    return JSONResponse(status_code=200, content={"message": "email has been sent"})


async def sms_send(mie_number, activity_id):
    print('mie_number-sms', mie_number)
    activity_id = str(activity_id)

    payload = "<SMSRequest>\r\n    <Username>03028501421</Username>\r\n    <Password>searle123</Password>\r\n    <From>7005101</From>\r\n    <To>" +mie_number+"</To>\r\n    <Message>"'Your Activity '+activity_id +' is approved in focall system'"</Message>\r\n</SMSRequest>"
    headers = {
        'Content-Type': 'application/xml'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)

async def createActivity(request: activity_schemas.Activity, db: Session):
    check_activity = db.query(models.Activity).filter(
        models.Activity.activity_id == request.activity_id).first()
    if check_activity != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Activity id {request.activity_id} is already exist")
    new_activity = models.Activity(
        activity_id=request.activity_id,
        activity_description=request.activity_description,
        activity_type=request.activity_type,
        activity_initiate_for=request.activity_initiate_for,
        activity_initiate_by=request.activity_initiate_by,
        current_date=datetime.now(),
        activity_date=request.activity_date,
        activity_amount=request.activity_amount,
        payment_term=request.payment_term,
        is_complete=request.is_complete,
        approved_amount=request.approved_amount,
        expense_amount=request.expense_amount,
        mie_id=request.mie_id,
        remarks=request.remarks

    )
    db.add(new_activity)
    db.commit()
    db.refresh(new_activity)
    get_lm = db.query(models.Employees.line_manager_sas_id).filter(
        models.Employees.emp_sap_id == request.activity_initiate_by).first()
    get_email = db.query(models.Employees.email).filter(
        models.Employees.emp_sap_id == get_lm[0]).first()
    await simple_send(get_email[0])
    return new_activity


def createActivityDoctorExpense(activity_id, request: List[activity_schemas.ActivityDoctor], db: Session):
    for element in request:
        new_doctor_expense = models.Activity_Doctor_Group(
            activity_doctor_name=element.doctorName,
            activity_doctor_comments=element.comments,
            activity_doctor_percentage=element.expense,
            activity_id=activity_id
        )
        db.add(new_doctor_expense)
        db.commit()
        db.refresh(new_doctor_expense)
    return "Success"


def createActivityProductExpense(activity_id, request: List[activity_schemas.ActivityProduct], db: Session):
    for element in request:
        new_product_expense = models.Activity_Product_Group(
            activity_product_name=element.productName,
            activity_product_percentage=element.productExpense,
            activity_id=activity_id
        )
        db.add(new_product_expense)
        db.commit()
        db.refresh(new_product_expense)
    return "Success"


def listOfEmployeesByLM(line_manager_sap_id, db: Session):
    abc = db.query(
        models.Employees.emp_name,
        models.Employees.emp_sap_id,
        models.Designation.designation_description
    ).join(models.Designation
           ).where(models.Employees.line_manager_sas_id == line_manager_sap_id).all()
    return abc


def listOfProductByEmp(emp_sap_id, db: Session):
    listOfProducts = db.query(
        models.Product.product_description
    ).join(models.Product_Team, models.Product.product_id == models.Product_Team.product_id
           ).join(models.Employees, models.Employees.team_id == models.Product_Team.team_id
                  ).where(models.Employees.emp_sap_id == emp_sap_id).all()
    return listOfProducts


def listOfDoctors(mie_sap_id, db: Session):
    listOfDoctors = db.query(
        models.Doctor.doctor_name
    ).join(models.Doctor_Mie_Allocation, models.Doctor.doctor_sas_id == models.Doctor_Mie_Allocation.doctor_sas_id
           ).where(models.Doctor_Mie_Allocation.mie_sap_id == mie_sap_id).all()
    return listOfDoctors


def listOfActivityById(activity_id, db: Session):
    activityListById = db.query(models.Activity).where(
        models.Activity.activity_id == activity_id).all()
    return activityListById


def listOfActivity(line_manager_sas_id, is_complete, db: Session):
    get_role = db.query(models.Users.role_id).where(
        models.Users.emp_sap_id == line_manager_sas_id).all()
    print('Role', get_role)
    if get_role == []:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Line manager id {line_manager_sas_id} is invalid")

    if get_role[0].role_id == 2 and is_complete == "ALL":
        listOfActivity = db.query(
            models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete
        ).order_by(models.Activity.created_at.desc()
        ).where(models.Activity.activity_initiate_by == line_manager_sas_id).all()
        return listOfActivity

    elif get_role[0].role_id == 2 and is_complete == "PENDING":
        listOfActivity = db.query(models.Activity
        ).order_by(models.Activity.created_at.desc()).where(
            models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity
    elif get_role[0].role_id == 2 and is_complete == "REJECT":
        listOfActivity = db.query(models.Activity).order_by(models.Activity.created_at.desc()).where(
            models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity
    elif get_role[0].role_id == 2 and is_complete == "APPROVED":
        listOfActivity = db.query(models.Activity).order_by(models.Activity.created_at.desc()).where(
            models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity
    elif get_role[0].role_id == 2 and is_complete == "COMPLETED":
        listOfActivity = db.query(models.Activity).order_by(models.Activity.created_at.desc()).where(
            models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity

    if get_role[0].role_id == 3 and is_complete == "ALL":
        listOfActivity = db.query(
            models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete
        ).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
               ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
                      ).order_by(models.Activity.created_at.desc()).where(models.Employees.line_manager_sas_id == line_manager_sas_id).all()
        return listOfActivity
    elif get_role[0].role_id == 3 and is_complete == "PENDING":
        listOfActivity = db.query(models.Activity
        ).outerjoin(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
        ).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
        ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
        ).order_by(models.Activity.created_at.desc()
        ).where(models.Activity_Status.activity_id.is_(None), models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity

    elif get_role[0].role_id == 3 and is_complete == "REJECT":
        listOfActivity = db.query(models.Activity).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
                                                        ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
                                                               ).order_by(models.Activity.created_at.desc()).where(models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity
    elif get_role[0].role_id == 3 and is_complete == "APPROVED":
        listOfActivity = db.query(models.Activity).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
                                                        ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
                                                               ).order_by(models.Activity.created_at.desc()).where(models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity
    elif get_role[0].role_id == 3 and is_complete == "COMPLETED":
        listOfActivity = db.query(models.Activity).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
                                                        ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
                                                               ).order_by(models.Activity.created_at.desc()).where(models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity

    if get_role[0].role_id == 5 and is_complete == "ALL":
        listOfActivity = db.query(
            models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete
        ).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
               ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
                      ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
                             ).order_by(models.Activity.created_at.desc()).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id).all()
        return listOfActivity
    elif get_role[0].role_id == 5 and is_complete == "PENDING":
        listOfActivity = db.query(models.Activity).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
                                                        ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
                                                               ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
                                                                      ).order_by(models.Activity.created_at.desc()).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity
    elif get_role[0].role_id == 5 and is_complete == "REJECT":
        listOfActivity = db.query(models.Activity).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
                                                        ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
                                                               ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
                                                                      ).order_by(models.Activity.created_at.desc()).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity
    elif get_role[0].role_id == 5 and is_complete == "APPROVED":
        listOfActivity = db.query(models.Activity).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
                                                        ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
                                                               ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
                                                                      ).order_by(models.Activity.created_at.desc()).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity
    elif get_role[0].role_id == 5 and is_complete == "COMPLETED":
        listOfActivity = db.query(models.Activity).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
                                                        ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
                                                               ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
                                                                      ).order_by(models.Activity.created_at.desc()).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.is_complete == is_complete).all()
        return listOfActivity


def listOfActivityProducts(activityId, db: Session):
    activityProductList = db.query(models.Activity_Product_Group).filter(
        models.Activity_Product_Group.activity_id == activityId).all()
    return activityProductList


def listOfActivityDoctors(activityId, db: Session):
    activityDoctorList = db.query(models.Activity_Doctor_Group).filter(
        models.Activity_Doctor_Group.activity_id == activityId).all()
    return activityDoctorList


async def createActivityStatus(request: activity_schemas.ActivityStatus, db: Session):
    new_activity_status = models.Activity_Status(
        activity_id=request.activity_id,
        approval=request.approval,
        approved_by=request.approved_by,
        approver_designation_id=request.approver_designation_id
    )
    db.add(new_activity_status)
    db.commit()
    db.refresh(new_activity_status)

    if request.approval == True and request.approver_designation_id == 5:
        update_activity = db.query(models.Activity).filter(
            models.Activity.activity_id == request.activity_id)
        update_activity.update({
            'is_complete': 'APPROVED'
        })
        db.commit()
        get_mi = db.query(models.Activity.activity_initiate_for).filter(
            models.Activity.activity_id == request.activity_id).first()
        mie_number = db.query(models.Employees.contact_number).filter(
            models.Employees.emp_name == get_mi[0]).first()
        await sms_send(mie_number[0], request.activity_id)
        return
    if request.approval == True and request.approver_designation_id == 3:
        update_activity = db.query(models.Activity).filter(
            models.Activity.activity_id == request.activity_id)
        update_activity.update({
            'is_complete': 'PENDING'
        })
        db.commit()
        get_nsm = db.query(models.Employees.line_manager_sas_id).filter(
            models.Employees.emp_sap_id == request.approved_by).first()
        get_bm = db.query(models.Employees.line_manager_sas_id).filter(
            models.Employees.emp_sap_id == get_nsm[0]).first()
        get_email = db.query(models.Employees.email).filter(
            models.Employees.emp_sap_id == get_bm[0]).first()
        await simple_send(get_email[0])
        return

    if request.approval == False and request.approver_designation_id == 3:
        update_activity = db.query(models.Activity).filter(
            models.Activity.activity_id == request.activity_id)
        update_activity.update({
            'is_complete': 'REJECT',
            'remarks': request.remarks
        })
        db.commit()
        return

    if request.approval == False and request.approver_designation_id == 5:
        update_activity = db.query(models.Activity).filter(
            models.Activity.activity_id == request.activity_id)
        update_activity.update({
            'is_complete': 'REJECT',
            'remarks': request.remarks
        })
        db.commit()
        return


async def bulkActivityStatus(request: activity_schemas.BulkActivityStatus, db: Session):
    for index in range(len(request.activity_ids)):
        check_activity_status = db.query(models.Activity_Status).filter(
            models.Activity_Status.activity_id == request.activity_ids[index])
        print("Check_Activity", check_activity_status.first())
        print("Activity--", request.activity_ids[index])
        new_activity_status = models.Activity_Status(
            activity_id=request.activity_ids[index],
            approval=request.approval,
            approved_by=request.approved_by,
            approver_designation_id=request.approver_designation_id
        )
        db.add(new_activity_status)
        db.commit()
        db.refresh(new_activity_status)
        if request.approval == True and request.approver_designation_id == 5:
            update_activity = db.query(models.Activity).filter(
                models.Activity.activity_id == request.activity_ids[index])
            update_activity.update({
                'is_complete': 'APPROVED'
            })
            db.commit()
            get_mie = db.query(models.Activity.activity_initiate_for).filter(
                models.Activity.activity_id == request.activity_ids[index]).first()
            mie_number = db.query(models.Employees.contact_number).filter(
                models.Employees.emp_name == get_mie[0]).first()
            await sms_send(mie_number[0], request.activity_ids[index])
            # return
        if request.approval == True and request.approver_designation_id == 3:
            update_activity = db.query(models.Activity).filter(
                models.Activity.activity_id == request.activity_ids[index])
            update_activity.update({
                'is_complete': 'PENDING'
            })
            db.commit()
            get_nsm = db.query(models.Employees.line_manager_sas_id).filter(
                models.Employees.emp_sap_id == request.approved_by).first()
            get_bm = db.query(models.Employees.line_manager_sas_id).filter(
                models.Employees.emp_sap_id == get_nsm[0]).first()
            get_email = db.query(models.Employees.email).filter(
                models.Employees.emp_sap_id == get_bm[0]).first()
            await simple_send(get_email[0])
            # return

        if request.approval == False and request.approver_designation_id == 3:
            update_activity = db.query(models.Activity).filter(
                models.Activity.activity_id == request.activity_ids[index])
            update_activity.update({
                'is_complete': 'REJECT'
            })
            db.commit()
            return

        if request.approval == False and request.approver_designation_id == 5:
            update_activity = db.query(models.Activity).filter(
                models.Activity.activity_id == request.activity_ids[index])
            update_activity.update({
                'is_complete': 'REJECT'
            })
            db.commit()
            return


def editActivityAmount(activity_id, approval_amount, db: Session):
    update_activity_amount = db.query(models.Activity).filter(
        models.Activity.activity_id == activity_id)
    if not update_activity_amount.first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Activity with id {activity_id} not found")
    update_activity_amount.update({
        'approved_amount': approval_amount
    })
    db.commit()
    return "Update Activity Successfully"


def listOfReportActivity(line_manager_sas_id, is_complete, from_date, to_date, db: Session):
    get_role = db.query(models.Users.role_id).where(
        models.Users.emp_sap_id == line_manager_sas_id).all()
    print('Role', get_role)
    if get_role == []:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Line manager id {line_manager_sas_id} is invalid")

    if get_role[0].role_id == 2 and is_complete == 'ALL':
        listOfReportActivity = db.query(
            models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.current_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
        ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
        ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
        ).filter(models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date)).all()
        # , models.Activity.is_complete == is_complete).all()
        return listOfReportActivity
    elif get_role[0].role_id == 2 and is_complete == "PENDING":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.current_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).where(models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 2 and is_complete == "REJECT":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.current_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).where(models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 2 and is_complete == "APPROVED":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.current_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).where(models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 2 and is_complete == "COMPLETED":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.current_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).where(models.Activity.activity_initiate_by == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    if get_role[0].role_id == 3 and is_complete == 'ALL':
        listOfReportActivity = db.query(
            models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
        ).distinct().join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
        ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
        ).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
        ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
        ).where(models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date)).all()
        # , models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 3 and is_complete == "PENDING":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
            ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
            ).where(models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 3 and is_complete == "REJECT":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
            ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
            ).where(models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 3 and is_complete == "APPROVED":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
            ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
            ).where(models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 3 and is_complete == "COMPLETED":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).join(models.Employees, models.Activity.activity_initiate_by == models.Employees.emp_sap_id
            ).join(models.Designation, models.Employees.designation_id == models.Designation.designation_id
            ).where(models.Employees.line_manager_sas_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    if get_role[0].role_id == 5 and is_complete == 'ALL':
        listOfReportActivity = db.query(
            models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
        ).distinct().join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
        ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
        ).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
        ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
        ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
        ).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date)).all()
        # , models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 5 and is_complete == "PENDING":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
            ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
            ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
            ).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 5 and is_complete == "REJECT":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
            ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
            ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
            ).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

    elif get_role[0].role_id == 5 and is_complete == "APPROVED":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
            ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
            ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
            ).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity
    elif get_role[0].role_id == 5 and is_complete == "COMPLETED":
        listOfReportActivity = db.query(models.Activity.activity_id,
            models.Activity.activity_initiate_by,
            models.Activity.activity_initiate_for,
            models.Activity.activity_description,
            models.Activity.activity_amount,
            models.Activity.activity_date,
            models.Activity.activity_type,
            models.Activity.is_complete,
            models.Activity_Doctor_Group.activity_doctor_name,
            models.Activity_Doctor_Group.activity_doctor_percentage,
            models.Activity_Product_Group.activity_product_name,
            models.Activity_Product_Group.activity_product_percentage
            ).join(models.Activity_Doctor_Group, models.Activity.activity_id == models.Activity_Doctor_Group.activity_id
            ).join(models.Activity_Product_Group, models.Activity.activity_id == models.Activity_Product_Group.activity_id
            ).join(models.Activity_Status, models.Activity.activity_id == models.Activity_Status.activity_id
            ).join(models.Employees, models.Activity_Status.approved_by == models.Employees.emp_sap_id
            ).join(models.Teams, models.Employees.team_id == models.Teams.team_id
            ).where(models.Employees.designation_id == '8', models.Activity_Status.approval == True, models.Teams.emp_sap_id == line_manager_sas_id, models.Activity.current_date.between(from_date, to_date), models.Activity.is_complete == is_complete).all()
        return listOfReportActivity

def checkActivityStatus(activity_id, db: Session):
    check = db.query(models.Activity_Status).filter(models.Activity_Status.activity_id == activity_id)
    if check.first() != None:
        return True
    return False
