from pickle import TRUE
import platform
from hdbcli import dbapi
import pandas as pd
from google.cloud import storage
import os
from google.oauth2 import service_account
from urllib.parse import quote
import urllib
from sqlalchemy import create_engine
import sqlalchemy
import pyodbc
import cx_Oracle as xo


#verify the architecture of Python
print ("Platform architecture: " + platform.architecture()[0])

#Initialize credentials variables:
address = '10.210.134.204',
port = '33015',
user = 'Etl',
password = 'EtlIbl12345'

def getSpecChars():
    spec_chars = ["!", '"', "#", "%", "&", "'" ,"\(","\)"
            ,"\*" ,"\+"  ,","   ,"-" , "/"  ,":",";", "<","=", ">"
            ,"\?","@","\[","\]","^","_","`", "{"
            , "}", "~", "–"
                ]
    return spec_chars

#Initialize your connection
def sapConn():
    conn = dbapi.connect(
        address='10.210.134.43',
        port='33015',
        user='Etl',
        password='EtlIbl12345' )
    return conn

def s4HanaConnection():
    conn = dbapi.connect(
        address='10.210.134.205',
        port='33015',
        user='Etl',
        password='Etl@2024' )
    return conn

def sapConnAlchemy():
    hdb_user = 'ETL'
    hdb_password = 'EtlIbl12345'
    hdb_host = '10.210.134.204'
    hdb_port = '33015'
    connection_string ='hana://%s:%s@%s:%s/?encrypt=true&sslvalidatecertificate=false' % (
        hdb_user, hdb_password, hdb_host, hdb_port)
    return connection_string

# 
def sapSandBox2ConnAlchemy():
    hdb_user = 'ETL'
    hdb_password = 'Sand2etl'
    hdb_host = ' 10.210.135.228'
    hdb_port = '33015'
    connection_string ='hana://%s:%s@%s:%s/?encrypt=true&sslvalidatecertificate=false' % (
        hdb_user, hdb_password, hdb_host, hdb_port)
    engine=sqlalchemy.create_engine(connection_string)
    return engine

def markittSqlServer():
    server = '172.20.7.71\SQLSERVER2017'
    db = 'Markitt2021-2022'
    user = 'syed.shujaat'
    password = 'new$5201'
    schema = 'dbo'
    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server +
                    ';DATABASE='+db+';UID='+user + ';PWD='+password+';TrustServerCertificate=Yes')
    return conn

def markittSqlServerAlchmy():
    server = '172.20.7.71\SQLSERVER2017'
    db = 'Markitt2021-2022'
    user = 'syed.shujaat'
    password = 'new$5201'
    schema = 'dbo'
    connect_string = 'mssql+psycopg2://syed.shujaat:new$5201@172.20.7.71:1433/Markitt2021-2022'
    engine = sqlalchemy.create_engine(connect_string)
    return engine

def oracleIblGrpHcmAlchmy(): 
    DIALECT = 'oracle'
    SQL_DRIVER = 'cx_oracle'
    USERNAME = 'IBLGRPHCM' #enter your username
    PASSWORD = 'iblgrp106hcm' #enter your password
    HOST = 'Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com' #enter the oracle db host url
    PORT = 6464 # enter the oracle port number
    SERVICE = 'cdb1' # enter the oracle db service name
    ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    return engine

def oracleIlgrpHcm():
    oracleConnectionDB = xo.connect('IBLGRPHCM', 'iblgrp106hcm',
                        xo.makedsn('Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com', 6464, 'cdb1'))
    return oracleConnectionDB

def FranchiseAlchmy():

    # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')

    host = '35.216.155.219'
    db = 'franchise_db'
    user = 'franchise'
    password = 'franchisePassword!123!456'
    schema = 'franchise'
    connect_string =f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
    engine = sqlalchemy.create_engine(connect_string)
    return engine

def pioneerSqlAlchmy():
    host = '35.216.155.219'
    db = 'poineer_db'
    user = 'pioneer'
    password = 'poineer_synch!123!456'
    schema = 'poineer_schema'
    connect_string =f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
    engine = sqlalchemy.create_engine(connect_string)
    return engine


def lorealConnectionAlchemy():
    DIALECT = 'oracle'
    SQL_DRIVER = 'cx_oracle'
    USERNAME = 'loreal1' #enter your username
    PASSWORD = 'Loreal1106' #enter your password
    HOST = 'Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com' #enter the oracle db host url
    PORT = 6464 # enter the oracle port number
    SERVICE = 'cdb1' # enter the oracle db service name
    ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

    engine =sqlalchemy.create_engine(ENGINE_PATH_WIN_AUTH)
    return engine

def attendanceMachine66():
    server = '192.168.130.66'
    db = 'iblgrp'
    user = 'sa'
    password =urllib.parse.quote_plus ('abc@123')
    schema = 'dbo'
    connect_string = f'''mssql+psycopg2://{user}:{password}@{server}:1433/{db}'''
    engine = sqlalchemy.create_engine(connect_string)
    return engine
