from fastapi import APIRouter, Depends, status, HTTPException, Response
from fastapi.security import OAuth2PasswordRequestForm
from franchise.db_schemas import users_schemas
from franchise import database, models, TokenJWT
from sqlalchemy.orm import Session
from franchise.repository import user
from franchise.hashing import Hash

router = APIRouter(
    prefix="/auth",
    tags=["User Management"]
)

@router.post('/user', response_model=users_schemas.ShowUser)
def create_user(request: users_schemas.User, db: Session=Depends(database.get_db)):
    return user.createUser(request, db)

@router.post('/login')
def login(request:OAuth2PasswordRequestForm=Depends(), db: Session = Depends(database.get_db)):
    userId = db.query(models.User).filter(models.User.distributor_id == request.username).first()
    if not userId:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Invalid Credentials")
    if not Hash.verifyPassword(userId.password, request.password):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Invalid password")
    # Generate Token and return
    access_token = TokenJWT.create_access_token(data={"sub": userId.distributor_id, "company_id": userId.company_code, "status": userId.status, "role": userId.role_id})
    return {"access_token": access_token, "token_type": "bearer"}

@router.get('/listOfUsers')
def list_users(db: Session=Depends(database.get_db)):
    return user.listOfUsers(db)

@router.get('/abcd')
def abcd():
    return 1234

@router.get('/fetchUserInfo/{id}', status_code=status.HTTP_200_OK)
def user_info(id, db: Session=Depends(database.get_db)):
    return user.fetchUserInfoById(id, db)

@router.get('/getLocation', status_code=status.HTTP_200_OK)
def user_locations(db: Session=Depends(database.get_db)):
    return user.fetchUserLocation(db)

@router.put('/updateUserInfo/{id}', status_code=status.HTTP_202_ACCEPTED)
def update_user_info(id, request: users_schemas.User ,db: Session=Depends(database.get_db)):
    return user.updateUserInfo(id, request, db)

@router.put('/updateUserPassword/{email}', status_code=status.HTTP_202_ACCEPTED)
def update_user_password(email, request: users_schemas.User ,db: Session=Depends(database.get_db)):
    return user.updateUserPassword(email, request, db)

# @router.get('/listOfDistributor/{userRoleId}', status_code=status.HTTP_200_OK)
# def fetch_distributor(userRoleId, db: Session=Depends(database.get_db)):
#     return "asd"