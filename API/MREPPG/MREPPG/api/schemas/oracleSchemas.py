import string
import sourcedefender
from datetime import date, datetime
from typing import  Optional
from pydantic import BaseModel
from pydantic.fields import Field
from sqlalchemy.sql.expression import column
from sqlalchemy.sql.sqltypes import INT, Date, String,Integer

class MrepSasData(BaseModel):
    BR_CD: str = Field(alias="DistributorCode")
    BILL_NO: int = Field(alias="InvoiceNumber")
    BILL_DT: date = Field(alias="InvoiceDate")
    EBS_CUST:int=Field(alias="CustomerCode")
    CUSTOMER_NAME:str=Field(alias="CustomerName")
    ADDRESS: Optional[str] = Field(alias="CustomerAddress")
    CH_CD: str = Field(alias="CustomerType")
    ITEM_CODE: str = Field(alias="ProductCode")
    description: str = Field(alias="ProductName")
    BATCH_NO: str = Field(alias="BatchNumber")
    price: float = Field(alias="TradePrice")
    SOLD_QTY: int = Field(alias="Units")
    BON_QTY: int = Field(alias="Bonus")
    disc_amt: float = Field(alias="Discount")
    NET_amt: float = Field(alias="NetAmount")
    reason: str = Field(alias="TransactionType")
    BrickCode:str = Field(alias='BrickCode')
    BrickName: str = Field(alias='BrickName')


    class Config():
        orm_mode = True
        allow_population_by_field_name = True

# class MrepSasData(BaseModel):
#     INVOICEDATE:date=Field(alias="InvoiceDate")
#     INVOICE:int=Field(alias="Invoice")
#     PRODCODE:str=Field(alias="ProductCode")
#     BATCH:str=Field(alias="Batch")
#     TRADEPRICE:float=Field(alias="TradePrice")
#     UNITS:int=Field(alias="Unit")
#     BONUS:int=Field(alias="Bonus")
#     DISCOUNT:float=Field(alias="Discount")
#     NETVALUE:float=Field(alias="NetValue")
#     FLAG:str=Field(alias="Flag")
#     CUSTOMERCODE:str=Field(alias="CustomerCode")
#     CUSTOMERNAME:str=Field(alias="CustomerName")
#     ADDRESS1:str=Field(alias="Address1")
#     ADDRESS2:Optional[str]=Field(alias='Address2')
#     PHONE:str=Field(alias="Phone")
#     LICENSE:str=Field(alias="License")
#     AREACODE:str=Field(alias="AreaCode")
#     AREANAME:str=Field(alias="AreaName")
#     class Config():
#         orm_mode=True
#         allow_population_by_field_name = True

class MrepCustomer(BaseModel):
    CUSTOMERCODE:str=Field(alias="CustomerCode")
    CUSTOMERNAME:str=Field(alias='CustomerName')
    ADDRESS1:str=Field(alias='Address1')
    ADDRESS2:Optional[str]=Field(alias='Address2')
    PHONE:str=Field(alias='Phone')
    LICENSE:Optional[str]=Field(alias='License')
    AREACODE:Optional[str]=Field(alias='AreaCode')
    AREANAME:Optional[str]=Field(alias='AreaName')
    class Config():
        orm_mode=True
        allow_population_by_field_name = True





