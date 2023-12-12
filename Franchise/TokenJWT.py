from datetime import timedelta, datetime
from jose import JWTError, jwt
from franchise.db_schemas import users_schemas

SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verifyToken(token: str, credentials_exception):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        distributor_id: str = payload.get("sub")
        if distributor_id is None:
            raise credentials_exception
        token_data = users_schemas.TokenData(distributor_id=distributor_id)
    except JWTError:
        raise credentials_exception

def getUserId(token: str, credentials_exception):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        userId: str = payload.get("sub")
        if userId is None:
            raise credentials_exception
        return userId
    except JWTError:
        raise credentials_exception
