from pickle import TRUE
import platform
from hdbcli import dbapi
import pandas as pd
from google.cloud import storage
import os
from google.oauth2 import service_account
from sqlalchemy import create_engine
import sqlalchemy
import pyodbc

#verify the architecture of Python
print ("Platform architecture: " + platform.architecture()[0])

#Initialize credentials variables:
address = '10.210.134.204',
port = '33015',
user = 'Etl',
password = 'EtlIbl12345'


#Initialize your connection
def sapConn():
        conn = dbapi.connect(

            # Option 1, retrieve the connection parameters from the hdbuserstore
            # key='USER1UserKey', # address, port, user and password are retrieved from the hdbuserstore

            # Option2, specify the connection parameters
            address='10.210.134.204',
            port='33015',
            user='Etl',
            password='EtlIbl12345'

            # Additional parameters
            # encrypt=True, # must be set to True when connecting to HANA as a Service
            # As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud)
            # sslValidateCertificate=False #Must be set to false when connecting
            # to an SAP HANA, express edition instance that uses a self-signed certificate.
        )
        return conn


def s4HanaConnection():
    conn = dbapi.connect(

        address='10.210.134.205',
        port='33015',
        user='Etl',
        password='Etl@2024'

    )
    return conn

def sapConnAlchemy():
    hdb_user = 'ETL'
    hdb_password = 'EtlIbl12345'
    hdb_host = '10.210.134.204'
    hdb_port = '33015'
    connection_string ='hana://%s:%s@%s:%s/?encrypt=true&sslvalidatecertificate=false' % (
        hdb_user, hdb_password, hdb_host, hdb_port)

    # engine1 = sqlalchemy.create_engine(connection_string)
    return connection_string

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
    # conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server +
    #                 ';DATABASE='+db+';UID='+user + ';PWD='+password+';TrustServerCertificate=Yes')

    connect_string = 'mssql+psycopg2://syed.shujaat:new$5201@172.20.7.71:1433/Markitt2021-2022'
    engine = sqlalchemy.create_engine(connect_string)

    return engine
