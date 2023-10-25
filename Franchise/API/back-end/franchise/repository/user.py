from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from franchise.db_schemas import sales_schemas, users_schemas
from franchise import models
from franchise.hashing import Hash

def createUser(request: users_schemas.User, db: Session):
    distributor_id = db.query(models.User).filter(models.User.distributor_id == request.distributor_id)
    print(distributor_id)
    if distributor_id.first() != None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Distributor Id {request.distributor_id} is already exist")
    new_user = models.User(
        company_code=request.company_code, 
        email=request.email,
        distributor_id=request.distributor_id, 
        username=request.username,
        password= Hash.passwordHash(request.password), 
        created_at=request.created_at, 
        status=request.status,
        store_name=request.store_name,
        role_id=request.role_id,
        location_id=request.location_id
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

def fetchUserLocation(db: Session):
    get_user_location = db.query(models.Locations).all()
    return get_user_location

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