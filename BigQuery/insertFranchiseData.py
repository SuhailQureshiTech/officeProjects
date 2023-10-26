# import 

import os
from sqlalchemy import create_engine
import sqlalchemy
import pyodbc
import pandas as pd

# import -- > End

def FranchiseAlchmy():
    # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')

    host = '34.65.6.130'
    db = 'franchise_db'
    user = 'franchise'
    password = 'franchisePassword!123!456'
    schema = 'franchise'
    connect_string =f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
    engine = sqlalchemy.create_engine(connect_string)
    return engine

franchiseEngine=FranchiseAlchmy()
# os.chdir('D:\\TEMP\\FranchiseDataExcel\\')
# print('current loc ',os.getcwd())
franchiseDf = pd.read_csv('D:\TEMP\FranchiseDataExcel\\AugustFranchise.csv')
# print(franchiseDf)
# print(franchiseDf.info())
colDict = {'Franchise Customer OrderNo': 'franchise_customer_order_no'
           , 'Franchise Customer Invoice Date': 'franchise_customer_invoice_date'
           , 'Franchise Customer Invoice Number': 'franchise_customer_invoice_number', 'Channel': 'channel'
           , 'Franchise Code': 'franchise_code', 'Franchise Customer Number': 'franchise_customer_number'
           , 'IBL Customer Number': 'ibl_customer_number', 'RD Customer Name': 'rd_customer_name'
           , 'IBL Customer Name': 'ibl_customer_name', 'Customer Address': 'customer_address'
           , 'Franchise Item Code': 'franchise_item_code', 'IBL Item Code': 'ibl_item_code'
           , 'Franchise Item Description': 'franchise_item_description'
           , 'IBL Item Description': 'ibl_item_description', 'Quantity Sold': 'quantity_sold'
           , 'Gross Amount': 'gross_amount', 'Reason': 'reason', 'FOC': 'foc', 'BATCH_NO': 'batch_no'
           , 'PRICE': 'price', 'BON_QTY': 'bon_qty', 'DISC_AMT': 'disc_amt', 'NET_AMT': 'net_amt'
           , 'DISCOUNTED_RATE': 'discounted_rate', 'Brick code': 'brick_code', 'Brick Name': 'brick_name'
           }
franchiseDf.rename(columns=colDict,inplace=True)
print(franchiseDf.info())
franchiseDf.to_sql('franchise_sales', schema='franchise',if_exists='append',con=franchiseEngine,index=False)
