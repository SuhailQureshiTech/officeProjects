from datetime import date, datetime, timedelta, timezone
import time
from io import BytesIO
from typing import Optional
from fastapi import APIRouter, Depends, status, UploadFile, File, HTTPException,Request
from fastapi.responses import FileResponse
from numpy import datetime64, int64
import pandas as pd
import psycopg2
from requests import Session
from sqlalchemy import create_engine, text
from franchise import database
import numpy as np
import requests
from . import dateLib
import tkinter as tk
# from tkinter import filedialog
from franchise.hashing import Hash
distMappingDf=pd.DataFrame()

spec_chars = ["!", '"', "#", "%", "&", "'" ,"\(","\)"
    ,"\*" ,"\+"  ,","   ,"-" , "/"  ,":",";", "<","=", ">"
    ,"\?","@","\[","\]","^","_","`", "{"
    , "}", "~", "–"
        ]

router = APIRouter(
    prefix="/api",
    tags=["Distributor Apis"]
)

conn_string =  'postgresql://franchise:franchisePassword!123!456@35.216.155.219:5432/franchise_db'
today = date.today()

db = create_engine(conn_string)
conn = db.connect()

getSapCustomer=f''' 
            SELECT k.KUNNR sap_cust,k2.VKORG sap_plant  FROM SAPABAP1.KNA1 k
            LEFT OUTER JOIN SAPABAP1.KNVV k2 ON (k2.MANDT=k.MANDT AND k2.KUNNR=k.KUNNR AND k2.VKORG=6300)
            WHERE 1=1 AND k.mandt=300 AND k2.VKORG =6300
        '''
dfSapCustomers=pd.read_sql(getSapCustomer,con=conn)

print(dfSapCustomers)
