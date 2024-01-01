from datetime import date
# import imp
from tkinter.tix import Tree
from typing import Optional
from pydantic import Field
from pydantic.main import BaseModel
from sqlalchemy.sql.expression import _True, column, true
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Float
import sqlalchemy

# from database.database import Base, oracleSessionLocal
# from databaserepo.databaseFactory import Base
# from databaserepo.databaseFactory import Base

from sqlalchemy import Integer, String, Date

metadata = sqlalchemy.MetaData()

sales = sqlalchemy.Table(
    "sas_mrep_data"
    , metadata
    , sqlalchemy.Column('ind', sqlalchemy.String, primary_key=True)
    , sqlalchemy.Column('DistributorCode', sqlalchemy.String)
    , sqlalchemy.Column('InvoiceNumber',sqlalchemy.String)
    , sqlalchemy.Column('InvoiceDate', sqlalchemy.Date,index=True)
    , sqlalchemy.Column('CustomerCode', sqlalchemy.String)
    , sqlalchemy.Column('CustomerName', sqlalchemy.String)
    , sqlalchemy.Column('CustomerAddress', sqlalchemy.String)
    , sqlalchemy.Column('CustomerType', sqlalchemy.String)
    , sqlalchemy.Column('ProductCode', sqlalchemy.String)
    , sqlalchemy.Column('ProductName', sqlalchemy.String)
    , sqlalchemy.Column('BatchNumber', sqlalchemy.String)
    , sqlalchemy.Column('TradePrice', sqlalchemy.Float)
    , sqlalchemy.Column('Units', sqlalchemy.Integer)
    , sqlalchemy.Column('Bonus', sqlalchemy.Integer)
    , sqlalchemy.Column('Discount', sqlalchemy.Float)    
    , sqlalchemy.Column('NetAmount', sqlalchemy.Float)
    , sqlalchemy.Column('TransactionType', sqlalchemy.String)
    , sqlalchemy.Column('BrickCode', sqlalchemy.String)
    , sqlalchemy.Column('BrickName', sqlalchemy.String)
    , schema='DW'
)


