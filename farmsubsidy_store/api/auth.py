# https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/

from datetime import datetime, timedelta
from typing import Optional, Union

import pandas as pd
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials, OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from ..settings import API_HTPASSWD, API_TOKEN_LIFETIME, API_TOKEN_SECRET

ALGORITHM = "HS256"
USERS = pd.read_csv(API_HTPASSWD, names=("username", "password"), delimiter=":")
USERS.index = USERS["username"]
USERS = USERS.T.to_dict()


class Token(BaseModel):
    access_token: str
    token_type: str


class User(BaseModel):
    username: str
    password: str


class Authenticated(BaseModel):
    status: Optional[bool] = False


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
basic_auth = HTTPBasic(auto_error=True)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_user(username: str) -> User:
    if username in USERS:
        user_dict = USERS[username]
        return User(**user_dict)


def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        return False
    if not verify_password(password, user.password):
        return False
    return user


def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, API_TOKEN_SECRET, algorithm=ALGORITHM)
    return encoded_jwt


def get_authenticated(token: str = Depends(oauth2_scheme)) -> bool:
    """
    return auth status (for optional authenticated endpoints)
    """
    if not token:
        return False
    try:
        payload = jwt.decode(token, API_TOKEN_SECRET, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            return False
    except JWTError:
        return False
    user = get_user(username=username)
    return bool(user)


def require_authenticated(token: str = Depends(oauth2_scheme)) -> bool:
    """
    return auth status but raise for unauthenticated (for endpoints that require authentication)
    """
    authenticated = get_authenticated(token)
    if not authenticated.status:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return authenticated


def login_for_access_token(credentials: HTTPBasicCredentials = Depends(basic_auth)):
    user = authenticate_user(credentials.username, credentials.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=API_TOKEN_LIFETIME)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}
