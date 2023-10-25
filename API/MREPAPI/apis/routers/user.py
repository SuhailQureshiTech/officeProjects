from fastai import APIRouter, Depends, File, UploadFile, status, HTTPException
from fastai.security import OAuth2PasswordRequestForm
from apis.db_schemas import users_schemas
from apis import database, models, TokenJWT
from sqlalchemy.orm import Session
from apis.repository import user
from apis.hashing import Hash

router = APIRouter(
    prefix="/auth",
    tags=["User Management"]
)

@router.post('/user', response_model=users_schemas.ShowUser)
def create_user(request: users_schemas.User, db: Session=Depends(database.get_db)):
    return user.createUser(request, db)

@router.post('/login')
def login(request:OAuth2PasswordRequestForm=Depends(), db: Session = Depends(database.get_db)):
    userId = db.query(models.Users).filter(models.Users.emp_sap_id == request.username).first()
    if not userId:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Invalid Credentials")
    if not Hash.verifyPassword(userId.password, request.password):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Invalid password")
    # Generate Token and return
    access_token = TokenJWT.create_access_token(data={"emp_sap_id": userId.emp_sap_id, "status": userId.status, "role": userId.role_id, "user_name": userId.user_name})
    return {"access_token": access_token, "token_type": "bearer"}

@router.get('/listOfUsers')
def list_users(db: Session=Depends(database.get_db)):
    return user.listOfUsers(db)

@router.get('/fetchUserInfo/{id}', status_code=200)
def user_info(id, db: Session=Depends(database.get_db)):
    return user.fetchUserInfoById(id, db)

@router.put('/updateUserInfo/{id}', status_code=status.HTTP_202_ACCEPTED)
def update_user_info(id, request: users_schemas.User ,db: Session=Depends(database.get_db)):
    return user.updateUserInfo(id, request, db)

@router.put('/updateUserPassword/{email}', status_code=status.HTTP_202_ACCEPTED)
def update_user_password(email, request: users_schemas.User ,db: Session=Depends(database.get_db)):
    return user.updateUserPassword(email, request, db)

@router.post('/createUsersRole')
def create_Users_Role(request: users_schemas.Users_Role, db: Session=Depends(database.get_db)):
    return user.createUserRole(request, db)

@router.get('/getAllUsersRole')
def get_all_users_role(db: Session=Depends(database.get_db)):
    return user.listOfUserRole(db)

@router.post('/createForm')
def create_Form(request: users_schemas.Form, db: Session=Depends(database.get_db)):
    return user.createForm(request, db)

@router.get('/getAllForm')
def get_all_form(db: Session=Depends(database.get_db)):
    return user.listOfForm(db)

@router.post('/createRolePermission')
def create_Role_Permission(request: users_schemas.Role_permission, db: Session=Depends(database.get_db)):
    return user.createRolePermission(request, db)