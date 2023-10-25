from io import BytesIO
from fastai import HTTPException, status
from sqlalchemy.orm import Session
from apis.db_schemas import users_schemas
from apis import models
from apis.hashing import Hash
import pandas as pd

def createUser(request: users_schemas.User, db: Session):
    userEmail = db.query(models.Users).filter(models.Users.emp_sap_id == request.emp_sap_id)
    print(userEmail)
    if userEmail.first() != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"User email {request.emp_sap_id} is already exist")
    new_user = models.Users(

        user_name=request.user_name,
        email=request.email,
        password= Hash.passwordHash(request.password),
        create_date=request.create_date,
        company_id=request.company_id,
        emp_sap_id=request.emp_sap_id,
        territory_id=request.territory_id,
        status=request.status,
        role_id=request.role_id,
        created_by=request.created_by
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user

def listOfUsers(db:Session):
    all_users = db.query(models.User).all()
    return all_users

def fetchUserInfoById(id, db:Session):
    user_info = db.query(models.User).filter(models.User.id == id).all()
    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with id {id} is not found"
        )
    return user_info

def updateUserInfo(id, request: users_schemas.User ,db:Session):
    updateUser = db.query(models.User).filter(models.User.id == id)
    if not updateUser.first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"User details with id {id} not found")
    updateUser.update({
        'company_code': request.company_code,
        'distributor_id': request.distributor_id,
        'email': request.email,
        'username': request.username,
        'status': request.status
    })
    db.commit()
    return 'User Information Updated'

def updateUserPassword(email, request: users_schemas.User ,db:Session):
    updateUserPassword = db.query(models.User).filter(models.User.email == email)
    if not updateUserPassword.first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"User details with email {email} not found")
    updateUserPassword.update({
        'password': Hash.passwordHash(request.password)
    })
    db.commit()
    return 'User Password Updated'


def createUserRole(request: users_schemas.Users_Role, db:Session):
    new_users_role = db.query(models.Users_Role).filter(models.Users_Role.role_description == request.role_description).first()
    if new_users_role!= None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Users Role {request.role_description} is already exist")
    add_new_users_role = models.Users_Role(
        role_description=request.role_description,
        create_date=request.create_date,
        created_by=request.created_by,
        status=request.status
    )
    db.add(add_new_users_role)
    db.commit()
    db.refresh(add_new_users_role)
    return add_new_users_role

def listOfUserRole(db:Session):
    all_users_role = db.query(models.Users_Role).all()
    return all_users_role

def createForm(request: users_schemas.Form, db:Session):
    new_form = db.query(models.Form).filter(models.Form.form_description == request.form_description).first()
    if new_form!= None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Form {request.form_description} is already exist")
    add_new_form = models.Form(
        form_description=request.form_description,
        form_url=request.form_url,
        create_date=request.create_date,
        created_by=request.created_by,
        status=request.status
       
    )
    db.add(add_new_form)
    db.commit()
    db.refresh(add_new_form)
    return add_new_form

def listOfForm(db:Session):
    all_form = db.query(models.Form).all()
    return all_form

def createRolePermission(request: users_schemas.Role_permission, db:Session):
    new_role_permission= db.query(models.Role_permission).filter(models.Role_permission.form_id == request.form_id,models.Role_permission.role_id == request.role_id).first()
    if new_role_permission!= None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Permission for this form {request.form_id} is already granted to this Role")
    add_new_role_permission = models.Role_permission(

        form_id=request.form_id,
        role_id=request.role_id,
        create=request.create,
        view=request.view,
        edit=request.edit,
        delete=request.delete,
        create_date=request.create_date,
        created_by=request.created_by,
        status=request.status
       
    )
    db.add(add_new_role_permission)
    db.commit()
    db.refresh(add_new_role_permission)
    return add_new_role_permission










