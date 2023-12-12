from fastapi import status, HTTPException
from sqlalchemy.orm import Session
from franchise import models
from franchise.db_schemas.sales_schemas import Distributor_Sales_Data


# def distributor_sales(request: Distributor_Sales_Data, db: Session):
#     new_distributors_sales = models.DistributorSalesData(
#         company_code=request.company_code,
#         ibl_distributor_code=request.ibl_distributor_code,
#         order_no=request.order_no,
#         invoice_no=request.invoice_no,
#         invoice_date=request.invoice_date,
#         channel=request.channel,
#         distributor_customer_no=request.distributor_customer_no,
#         ibl_customer_no=request.ibl_customer_no,
#         customer_name=request.customer_name,
#         distributor_item_code=request.distributor_item_code,
#         ibl_item_code=request.ibl_item_code,
#         SOLD_QTY=request.SOLD_QTY,
#         GROSS_AMOUNT=request.GROSS_AMOUNT,
#         REASON_CODE=request.REASON_CODE,
#         BONUS_QTY=request.BONUS_QTY,
#         CLAIMABLE_DISCOUNT=request.CLAIMABLE_DISCOUNT
#     )
#     return "Distributor sales"

# def distributors_data(current_date, db: Session):
#     data = db.query(models.)