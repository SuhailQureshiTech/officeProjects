#import sections--All imports
from datetime import date, datetime, timedelta, timezone
import time
from io import BytesIO
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

@router.post('/uploadSalesData/{company_code}/{user_id}', status_code=status.HTTP_201_CREATED)
async def upload_sales_file(company_code, user_id, request: Request, files: UploadFile = File(...)):

    df = pd.read_excel(BytesIO(files.file.read()), sheet_name="Sales")  
    df.columns = df.columns.str.strip()
    def checkColumnsinFile():

        if '.xlsx' not in  files.filename:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="File should be xlsx format, other format will be rejected by system")

        if len(df.columns) > 26:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="File columns shout not less or more than 26 columns")
        
        if 'Franchise Customer OrderNo' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="'Franchise Customer OrderNo column not found in Sales sheet")

        if 'Franchise Customer Invoice Date' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Franchise Customer Invoice Date column not found in Sales sheet")

        if 'Franchise Customer Invoice Number' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Franchise Customer Invoice Number column not found in Sales sheet")
        
        if 'Channel' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Channel column not found in Sales sheet")
        
        if 'Franchise Code' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Franchise Code column not found in Sales sheet")
        
        if 'Franchise Customer Number' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Franchise Customer Number column not found in Sales sheet")
        
        if 'IBL Customer Number' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="IBL Customer Number column not found in Sales sheet")

        if 'RD Customer Name' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="RD Customer Name column not found in Sales sheet")
        
        if 'IBL Customer Name' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="IBL Customer Name column not found in Sales sheet")

        if 'Customer Address' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Customer Address column not found in Sales sheet")

        if 'Franchise Item Code' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Franchise Item Code column not found in Sales sheet")
            
        if 'IBL Item Code' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="IBL Item Code column not found in Sales sheet")
        
        if 'Franchise Item Description' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Franchise Item Description column not found in Sales sheet")

        if 'IBL Item Description' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="IBL Item Description column not found in Sales sheet")

        if 'Quantity Sold' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Quantity Sold column not found in Sales sheet")
            
        if 'Gross Amount' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Gross Amount column not found in Sales sheet")
            
        if 'Reason' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Reason column not found in Sales sheet")
        
        if 'FOC' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="FOC column not found in Sales sheet")

        if 'BATCH_NO' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="BATCH_NO column not found in Sales sheet")

        if 'PRICE' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="PRICE column not found in Sales sheet")

        if 'BON_QTY' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Franchise Item Description column not found in Sales sheet")


        if 'DISC_AMT' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="DISC_AMT column not found in Sales sheet")
        
        if 'NET_AMT' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="NET_AMT column not found in Sales sheet")

        if 'DISCOUNTED_RATE' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="DISCOUNTED_RATE column not found in Sales sheet")

        if 'Brick Code' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Brick Code column not found in Sales sheet")

        if 'Brick Name' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="Brick Name column not found in Sales sheet")
        
    def validateData():
        try:    
            df['Franchise Customer Invoice Date'] = pd.to_datetime(df['Franchise Customer Invoice Date'])            
        except Exception as  ex:            
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Customer Invoice Date contains other than Date value")

        # date_validation = df['Franchise Customer Invoice Date'].dt.date < datetime64(
        #     today)
        
        # if date_validation.all() != True:
        #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
        #             detail="Invalid Date in sales file, Invoice Date should be less than current data")

        if df['Franchise Customer Invoice Number'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Customer Invoice Number cannot be null")

        if df['Channel'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Channel cannot be null")

        if df['Franchise Code'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Code cannot be null")

        if df['Franchise Code'].apply(lambda x: len(str(x))>10 or len(str(x))<10).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Code data length must not greater or less than 10 character")

        if df['Franchise Code'].dtypes == object:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Code contains alphanumeric data")
        
        dfFranchCode=df.query(f" `Franchise Code` not in [{user_id}] " )

        if len(dfFranchCode):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="file contains multiple Franchise Code")

        if df['Franchise Customer Number'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Customer Number cannot be null")        
            
        if df['IBL Customer Number'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="IBL Customer Number cannot be null and must not contains alphanumeric")    

        if df['IBL Customer Number'].apply(lambda x: len(str(x))>10).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="IBL Customer Number length must not greater than 10 character")                 

        dfCheckFranIblCus=df.query(" `IBL Customer Number`==`Franchise Code`  ")
        if len(dfCheckFranIblCus)>0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Code is not allowed to be in IBL Customer Number")                 



        if df['RD Customer Name'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="RD Customer Name cannot be null")        


        if df['IBL Customer Name'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="IBL Customer Name cannot be null")        

        if df['Franchise Item Code'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Item Code column contain null values")        


        if df['Franchise Item Code'].astype('str').str.count('\s+').any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="whitespaces found")      

        if df['IBL Item Code'].apply(lambda x: len(str(x))>11 ).any() or df['IBL Item Code'].apply(lambda x: len(str(x))<10).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="IBL Item Code length must not greater than 11 character")        

        if df['Franchise Item Description'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Item Description cannot be null")        

        if df['IBL Item Description'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="IBL Item Description cannot be null")        

        if df['Quantity Sold'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Quantity Sold should contain integer values and must not null")        

        if df['FOC'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="FOC should contain integer values")        

        if df['BON_QTY'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="BON_QTY should contain integer values and must not null")        

        if df['Gross Amount'].dtypes != float:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Gross Amount should contain integer values and must not null")        
            
        dfGrossSales=df.query("( `Gross Amount`<0 or `Quantity Sold`<0 or `FOC`<0  )and  `Reason`=='Sales' ")
        dfGross=df.query("(`Gross Amount`>0 or `Quantity Sold`>0 or `FOC`>0 ) and  `Reason`=='Return' ")

        if len(dfGrossSales)>0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="found sales with negative value in (Gross Amount or Quantity Sold or FOC)")        

        if len(dfGross)>0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="found return with postive value in (Gross Amount or Quantity Sold or FOC)")        


        if (~df['Reason'].isin(['Sales', 'Return'])).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Reason column can contains only (Sales/Return)")       

        if df['PRICE'].dtypes != float:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="PRICE should contain integer values and must not null")                 

        if df['BON_QTY'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                     detail="BON_QTY should contain integer values and must not null")   

        if (df['DISC_AMT'].dtypes != float and df['DISC_AMT'].dtypes !=int64):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                     detail="DISC_AMT should contain integer values and must not null")               

        if (df['NET_AMT'].dtypes != float and df['NET_AMT'].dtypes != int64):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                     detail="NET_AMT should contain integer values and must not null")   

        if df['DISCOUNTED_RATE'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="DISCOUNTED_RATE cannot be null")        



        if (df['DISCOUNTED_RATE'].dtypes != int64 and  df['DISCOUNTED_RATE'].dtypes != float):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                     detail="DISCOUNTED_RATE should contain integer values and must not null")                

    checkColumnsinFile()
    validateData()

    db = create_engine(conn_string)
    conn = db.connect()

    print('dates', df['Franchise Customer Invoice Date'].unique())

    df['Franchise Customer Invoice Date'] = pd.to_datetime(
        df['Franchise Customer Invoice Date']).dt.date

    #deleting existing records based on date 
    print('deleting existing records...')
    dateLIst=[]
    for x in df['Franchise Customer Invoice Date'].unique():
       dateFound=str(x)[0:10]
       dateLIst.append(dateFound)
    
    print(dateLIst)

    d_list_string ="'"+"','".join(dateLIst)+"'"
    delete_query = "delete from franchise_sales where franchise_customer_invoice_date in (" +d_list_string+")"+" and franchise_code="+"'"+str(user_id)+"'"
    # print(d_list_string)
    print(delete_query)

    result = conn.execute(delete_query)
    rowCounts=result.rowcount
    print(f''' delte row count {rowCounts}   ''')

    df.columns = ['franchise_customer_order_no', 'franchise_customer_invoice_date', 'franchise_customer_invoice_number'
                  ,'channel', 'franchise_code', 'franchise_customer_number', 'ibl_customer_number'
                  ,'rd_customer_name','ibl_customer_name', 'customer_address', 'franchise_item_code'
                  ,'ibl_item_code','franchise_item_description','ibl_item_description'
                  ,'quantity_sold', 'gross_amount', 'reason','foc','batch_no','price','bon_qty'
                  ,'disc_amt', 'net_amt','discounted_rate','brick_code','brick_name']
    
    df['company_code'] = company_code
    df['created_date'] = today

    start_time = time.time()
    print('df records')
    print(df)
    success = df.to_sql('franchise_sales', schema="franchise",
                        if_exists='append', con=conn, index=False)
    # conn.commit()
    if success:
        print("Data insertion was successful.")
    else:
        print("Data insertion failed.")

    total_sales_quantity = df['quantity_sold'].sum()
    total_sales_gross_amount = df['gross_amount'].sum()
    total_sales_discount = df['disc_amt'].sum()
    total_sales_bonus_quantity = df['bon_qty'].sum()
    total_sales_SKU = df['ibl_item_code'].nunique(dropna=True)
    total_sales_rows = df.shape[0]
    fileName=files.filename

    dataDetails={
        "Distributor_code":user_id,
        "File_Name":fileName,
        "Sales_Quantity": int(total_sales_quantity),
        "Sales_Gross_Amount": int(total_sales_gross_amount),
        "Sales_Discounts": int(total_sales_discount),
        "Sales_Bonus_Quantity": int(total_sales_bonus_quantity),
        "Total_Sales_SKUs": int(total_sales_SKU),
        "Total_Sales_Rows": int(total_sales_rows),
    }

    print('data details dataframe :')

    dataDetailsdf=pd.DataFrame.from_dict([dataDetails])
    dataDetailsdf.to_sql('users_activity_log', schema="franchise",
                        if_exists='append', con=conn, index=False)
    
    # dataDetailsdf.to_csv("d:\\temp\\dataDetails.csv",index=False)

    # dataDetailsdf.to_csv(f''' dataDetails.csv''',index=False)
    # print(dataDetailsdf.to_string(index=False))
    # return FileResponse(path="dataDetails.csv", filename="d:\\temp\\dataDetails.csv"
    #                     , media_type="multipart/form-data")
    
    return {
        "Sales_Quantity": int(total_sales_quantity),
        "Sales_Gross_Amount": int(total_sales_gross_amount),
        "Sales_Discounts": int(total_sales_discount),
        "Sales_Bonus_Quantity": int(total_sales_bonus_quantity),
        "Total_Sales_SKUs": int(total_sales_SKU),
        "Total_Sales_Rows": int(total_sales_rows),
    }

@router.post('/uploadStockData/{company_code}/{user_id}', status_code=status.HTTP_201_CREATED)
async def upload_stock_file(company_code, user_id, stockFiles: UploadFile = File(...)):
    
    df = pd.read_excel(BytesIO(stockFiles.file.read()), sheet_name='Stock')

    def checkColumnsinFile():    
        if len(df.columns)>13 or len(df.columns)<13:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail="File columns shout not less or more than 13 columns")
        
        if 'RD Code' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="RD Code column not found in Sales sheet")

        if 'IBL Branch code' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="IBL Branch code column not found in Sales sheet")

        if 'RD Item Code' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="RD Item Code column not found in Sales sheet")

        if 'IBL Item Code' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="IBL Item Code column not found in Sales sheet")

        if 'RD Item Description' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="RD Item Description column not found in Sales sheet")

        if 'LOT NUMBER' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="LOT NUMBER column not found in Sales sheet")

        if 'Expiry Date' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="Expiry Date column not found in Sales sheet")
        
        if 'Closing Quantity' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="Closing Quantity column not found in Sales sheet")

        if 'Value' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="Value column not found in Sales sheet")

        if 'Value' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="Value column not found in Sales sheet")
        
        if 'Dated' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="Dated column not found in Sales sheet")

        if 'Price' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="Price column not found in Sales sheet")
        
        if 'In-Transit stock' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="In-Transit stock column not found in Sales sheet")

        if 'Purchase Unit' not in df.columns:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail="Purchase Unit column not found in Sales sheet")

    def validateData():
    #   start
        if df['RD Code'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="RD Code cannot be null")

        if df['RD Code'].apply(lambda x: len(str(x))>10 or len(str(x))<10).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="RD Code data length must not greater or less than 10 character")

        if df['RD Code'].dtypes == object:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="RD Code contains alphanumeric data")
        
        dfFranchCode=df.query(f" `RD Code` not in [{user_id}] " )

        if len(dfFranchCode):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="file contains multiple RD Code")

        if df['IBL Branch code'].apply(lambda x: len(str(x))>4 or len(str(x))<4).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="IBL Branch code data length must not greater or less than 4 character")        

        if df['IBL Branch code'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="IBL Branch code should contain integer values and must not null")  
        
        if df['RD Item Code'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="RD Item Code cannot be null")
        
        if df['IBL Item Code'].apply(lambda x: len(str(x))>11 ).any() or df['IBL Item Code'].apply(lambda x: len(str(x))<10).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="IBL Item Code length must not greater than 11 character")       

        if df['RD Item Description'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="RD Item Description cannot be null")

        if df['LOT NUMBER'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="LOT NUMBER cannot be null")
        try:    
            df['Expiry Date'] = pd.to_datetime(df['Expiry Date'])            
        except Exception as  ex:            
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Expiry Date contains other than Date value")   
             
        dfGrossSales=df.query("( `Closing Quantity`<0 or `Value`<0 or `In-Transit stock`<0  or `Purchase Unit`<0  ) ")

        if len(dfGrossSales)>0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Closing Quantity or Value or In-Transit stock or Purchase Unit can not be negative")   

        if df['Price'].dtypes==object:
        # (df['Price'].dtypes!=int64 or  df['Price'].dtypes!=float):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Price can not be null and must be integer or float")  

    checkColumnsinFile()
    validateData()

    db = create_engine(conn_string)
    conn = db.connect()

    df['Dated'] = pd.to_datetime(df['Dated']).dt.date

    #deleting existing records based on date 
    print('deleting existing records...')
    dateLIst=[]
    for x in df['Dated'].unique():
       dateFound=str(x)[0:10]
       dateLIst.append(dateFound)
    
    print(dateLIst)

    d_list_string ="'"+"','".join(dateLIst)+"'"
    delete_query = "delete from franchise_stock where 1=1 and dated in (" +d_list_string+")"+" and ibl_distributor_code="+"'"+str(user_id)+"'"
    print(delete_query)

    df.columns = ['ibl_distributor_code','ibl_branch_code', 'distributor_item_code', 'ibl_item_code',
                  'distributor_item_description'
                  , 'lot_number', 'expiry_date', 'stock_qty', 'stock_value', 'dated','price','in_transit_stock'
                  ,'purchase_unit']

    df['company_code'] = company_code

    df.to_sql('franchise_stock', schema="franchise",
              if_exists='append', con=conn, index=False)
    
    # # print("to_sql duration: {} seconds".format(time.time() - start_time))

    # total_quantity = df['stock_qty'].sum()
    # total_stock = df['stock_value'].sum()
    # total_sku = df['ibl_item_code'].nunique(dropna=True)
    # total_rows = df.shape[0]
    
    # return {
    #     "Quantity": int(total_quantity),
    #     "Stock": int(total_stock),
    #     "Total_SKUs": int(total_sku),
    #     "Total_rows": int(total_rows)
    # }


# test Case new format sale
@router.post('/uploadSalesDataFormat/{company_code})', status_code=status.HTTP_201_CREATED)
# async def upload_sales_file_new(company_code, user_id, files: UploadFile = File(...)):
async def upload_sales_file_new(company_code,  files: UploadFile = File(...)):
    # df = pd.read_csv(BytesIO(files.file.read()), sheet_name="Sales")

    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv(BytesIO(files.file.read())
                    ,low_memory=False,encoding='ISO-8859-1'
                    #  , error_bad_lines=False, lineterminator='\n'
                     )
    
    df.columns = df.columns.str.strip()

    print(df.info())

    # df['Franchise Customer Invoice Date'] = pd.to_datetime(
    #     df['Franchise Customer Invoice Date']).dt.date

    # df.columns = ('franchise_customer_order_no',
    #               'franchise_customer_invoice_date', 'franchise_customer_invoice_number', 'channel', 'franchise_code', 'franchise_customer_number', 'ibl_customer_number', 'rd_customer_name', 'ibl_customer_name', 'customer_address', 'franchise_item_code', 'ibl_item_code', 'franchise_item_description', 'ibl_item_description', 'quantity_sold', 'gross_amount', 'reason', 'foc', 'batch_no', 'price', 'bon_qty', 'disc_amt', 'net_amt', 'discounted_rate', 'brick_code', 'brick_name'
    #               )

    df.columns = ('franchise_customer_order_no', 'franchise_customer_invoice_date'
                , 'franchise_customer_invoice_number', 'channel', 'franchise_code'
                , 'franchise_customer_number', 'ibl_customer_number'
                , 'rd_customer_name', 'ibl_customer_name', 'customer_address'
                , 'franchise_item_code', 'ibl_item_code', 'franchise_item_description', 'ibl_item_description'
                  , 'quantity_sold', 'gross_amount', 'reason', 'foc', 'batch_no', 'price', 'bon_qty'
                , 'disc_amt', 'net_amt', 'discounted_rate', 'brick_code', 'brick_name'
                  )

# checking Bad Data....
    df['foc'] = df['foc'].fillna(0)
    df['bon_qty'] = df['bon_qty'].fillna(0)
    df['disc_amt'] = df['disc_amt'].fillna(0)
    df['net_amt'] = df['net_amt'].fillna(0)
    df['discounted_rate'] = df['discounted_rate'].fillna(0)

    df = df.set_index('franchise_customer_invoice_number')
    focNonNumericData = pd.to_numeric(df.foc, errors='coerce')
    idx = focNonNumericData.isna()
    print(df[idx])

# Data Conversion

    df['foc'] = df['foc'].astype('float64')
    df['bon_qty'] = df['bon_qty'].astype('float64')

    df['price'] = df['price'].astype('float64')

    df['gross_amount'] = df['gross_amount'].astype('float64')
    df['gross_amount'] = df['gross_amount'].fillna(0)

    df['quantity_sold'] = df['quantity_sold'].astype('int64')
    df['quantity_sold'] = df['quantity_sold'].fillna(0)

    df['franchise_item_code'] = df['franchise_item_code'].astype('str')
    df['ibl_item_code'] = df['ibl_item_code'].astype('str')
    df['franchise_item_description'] = df['franchise_item_description'].astype(
        'str')
    df['ibl_item_description'] = df['ibl_item_description'].astype(
        'str')

    df['franchise_customer_invoice_date'] = pd.to_datetime(
        df['franchise_customer_invoice_date'])
    
    df['franchise_code'] = df['franchise_code'].astype('str')
    df['franchise_customer_number'] = df['franchise_customer_number'].astype(
        'str')
    df['ibl_customer_number'] = df['ibl_customer_number'].astype('str')
    
    df['rd_customer_name'] = df['rd_customer_name'].astype('str')
    df['ibl_customer_name'] = df['ibl_customer_name'].astype('str')
    df['customer_address'] = df['customer_address'].astype('str')


# Data Conversion -- End

    # df['batch_no'] = '-'
    
    # df['batch_no'] = df['batch_no'].astype('|S')

    
    # for char in spec_chars:
    #     df['batch_no'] = df['batch_no'].str.replace(
    #         char, ' ', regex=True)
    #     df['batch_no'] = df['batch_no'].str.split().str.join(" ")

    if len(df.columns) > 26:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="File contains one or more extra columns")
    df['company_code'] = company_code
    print(df.info())
    
    # start_time = time.time()
    df.to_sql('franchise_sales'
              , schema="franchise",
              if_exists='append'
              , con=conn
              , index=False
              )

# Reports...

@router.get('/getSalesDistributorStatus/{current_date}', status_code=status.HTTP_200_OK)
async def get_distributor_status(current_date, db: Session = Depends(database.get_db)):
    b = "'"+str(current_date)+"'"
    db = create_engine(conn_string)
    conn = db.connect()
    df_sales = pd.read_sql_query(
        f"""
            select
                distinct u.distributor_id,
                u.username  as distributor_name,
                l.location_name,
                fs2.franchise_code  ibl_distributor_code,
                fs2.created_date  current_dates,
                case
                    when (u.distributor_id is not null
                    and fs2.franchise_code  is not null) then true
                    else false
                end as file_Status
            from
                franchise.users u
            left join franchise."franchise_sales" fs2 on
                u.distributor_id = cast(fs2.franchise_code  as varchar)
            and fs2.franchise_customer_invoice_date = {b}
            inner join franchise.locations l on
                l.location_id = u.location_id            """, con=conn)
    df_sales['ibl_distributor_code'] = df_sales['ibl_distributor_code'].replace(
        np.nan, 0)
    # return df_sales.to_json(orient='records')
    return df_sales.to_dict(orient='records')

@router.get('/getStatusSales/{userId}/{from_date}/{to_date}', status_code=status.HTTP_200_OK)
def get_status(userId, from_date, to_date, db: Session = Depends(database.get_db)):
    db = create_engine(conn_string)
    conn = db.connect()
    current_date = date.today()
    read_sales = pd.read_sql_query(
        f"""select * from franchise."franchise_sales" fs2 where franchise_code = '{userId}' 
            and date(fs2.franchise_customer_invoice_date) between '{from_date}' and '{to_date}'""", con=conn
    )
    print(read_sales)
    return read_sales.to_dict(orient='records')

@router.get('/getStatusStock/{userId}/{from_date}/{to_date}', status_code=status.HTTP_200_OK)
def get_status(userId, from_date, to_date, db: Session = Depends(database.get_db)):
    db = create_engine(conn_string)
    conn = db.connect()
    current_date = date.today()
    print('date', userId)
    read_stock = pd.read_sql_query(
        f"""select * from franchise."franchise_stock" fs2 
        where ibl_distributor_code = '{userId}' and dated between '{from_date}' and '{to_date}';""", con=conn
    )
    print(read_stock)
    return read_stock.to_dict(orient='records')

@router.get('/getStatusSalesByInvoiceDate/{userId}/{from_date}/{to_date}', status_code=status.HTTP_200_OK)
def get_status(userId, from_date, to_date, db: Session = Depends(database.get_db)):
    # and fs2.franchise_code  = '{userId}' and date(fs2.franchise_customer_invoice_date) between '{from_date}' and '{to_date}' 

    db = create_engine(conn_string)
    conn = db.connect()
    read_status_by_invoice_id = pd.read_sql_query(
        f"""
            select  date, ibl_distributor_code,  total_gross_amount,l.location_name  from (
            select date(franchise_customer_invoice_date),fs2.franchise_code  ibl_distributor_code
            , sum(gross_amount) as total_gross_amount from franchise."franchise_sales" fs2
            where 1=1
             and fs2.franchise_code  = '{userId}' 
             and date(fs2.franchise_customer_invoice_date) between '{from_date}' and '{to_date}' 
            group by  fs2.franchise_customer_invoice_date ,fs2.franchise_code             
            ) fss
            inner join franchise.users u  on fss.ibl_distributor_code::varchar = u.distributor_id
            inner join franchise.locations l  on u.location_id =l.location_id
        """, con=conn)
    read_status_by_invoice_id['total_gross_amount'] = read_status_by_invoice_id['total_gross_amount'].replace(
        np.nan, 'null')
    
    print(read_status_by_invoice_id.info())
    read_status_by_invoice_id.to_csv('getstatusSales.csv')
    return read_status_by_invoice_id.to_dict(orient="records")

@router.get('/fetchDistributorIdAndLocation', status_code=status.HTTP_200_OK)
def get_status(db: Session = Depends(database.get_db)):
    db = create_engine(conn_string)
    conn = db.connect()
    fetchDistributorIdAndLocation = pd.read_sql_query(
        f"""select distributor_id, l.location_name from franchise.users u inner join 
            franchise.locations l on u.location_id = l.location_id order by 1""", con=conn)
    # read_status_by_invoice_id['total_gross_amount'] = read_status_by_invoice_id['total_gross_amount'].replace(
    # np.nan, 'null')
    return fetchDistributorIdAndLocation.to_dict(orient="records")

@router.get('/getStatusSalesForAllDistributors/{from_date}/{to_date}', status_code=status.HTTP_200_OK)
def get_status_sales_all_distributors(from_date, to_date, db: Session = Depends(database.get_db)):
    print('suhail')
    print(from_date)
    print(to_date)
    db = create_engine(conn_string)
    conn = db.connect()
    fetchDistributorAndSales = pd.read_sql_query(
        f"""
                    select
                date(fs2.franchise_customer_invoice_date),
                fs2.franchise_code  ibl_distributor_code,
                sum(gross_amount) as total_gross_amount,
                l.location_name
            from
                franchise."franchise_sales" fs2
            inner join franchise.users u on
                fs2.franchise_code ::varchar = u.distributor_id
            inner join franchise.locations l on
                u.location_id = l.location_id
            where	date(invoice_date) between '{from_date}' and '{to_date}'
            group by
                fs2.franchise_customer_invoice_date ,
                fs2.franchise_code ,
                l.location_name
            """, con=conn)
    return fetchDistributorAndSales.to_dict(orient="records")

#
@router.get('/changeFirstTimePassword/{distributor_id}/{old_password}/{new_password}'
            , status_code=status.HTTP_200_OK)
def changeFirstTimePassword(distributor_id,old_password,new_password, db: Session = Depends(database.get_db)):
    db = create_engine(conn_string)
    conn = db.connect()
    
    newPassword=new_password
    hashNewPassword="'"+str(Hash.passwordHash(newPassword))+"'"
    distributorId="'"+str(distributor_id)+"'"

    print('new password :',newPassword)
    print('hash password : ',hashNewPassword)

    franchiseEngine = create_engine(conn_string)

    qryGetUser=f'''select * from users
                where 1=1 and distributor_id={distributorId}
    '''
    
    df=pd.DataFrame()
    df= pd.read_sql_query(qryGetUser,con=franchiseEngine)
    print('data frame...........')
    userPassword=str(df['password'].values[0])
    print('user password........',userPassword)


    if Hash.verifyPassword(userPassword,old_password)==True:
        qryUpdateUser=f'''
                    update users set password={hashNewPassword}
                    where 1=1 and distributor_id={distributorId}                    
        '''
    else:
        return('current password is not correct')
    
    franchiseEngine.execute(qryUpdateUser)
    return('Password updated successfully')