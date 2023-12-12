from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from franchise import TokenJWT

oauth2Scheme = OAuth2PasswordBearer(tokenUrl="login")

def getCurrentUser(token: str = Depends(oauth2Scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authentication": "Bearer"},
    )
    
    return TokenJWT.verifyToken(token, credentials_exception)

def getUserId(token: str = Depends(oauth2Scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authentication": "Bearer"},
    )

    return TokenJWT.getUserId(token, credentials_exception)