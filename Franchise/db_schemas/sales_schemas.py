from datetime import date
from typing import Optional
from pydantic import BaseModel

class FRANCHISE_SALES(BaseModel):
    company_code: str
    ibl_distributor_code: str
    order_no: str
    invoice_no: str
    invoice_date: date
    channel: str
    distributor_customer_no: str
    ibl_customer_no: str
    customer_name:str
    distributor_item_code: str
    ibl_item_code: str
    qty_sold: str
    gross_amount: str
    reason: str
    bonus_qty: str
    discount: str
    item_description: str

class FRANCHISE_STOCK(BaseModel):
    company_code: str
    ibl_distributor_code: str
    dated: date
    distributor_item_code: str
    ibl_item_code: str
    distributor_item_description: str
    lot_number: str
    expiry_date: date
    stock_qty: int
    stock_value: int