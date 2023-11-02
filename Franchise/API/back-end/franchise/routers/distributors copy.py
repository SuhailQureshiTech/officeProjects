from datetime import date, datetime
import time
from io import BytesIO
from fastapi import APIRouter, Depends, status, UploadFile, File, HTTPException
from numpy import datetime64, int64
import pandas as pd
import psycopg2
from requests import Session
from sqlalchemy import create_engine, text
from franchise import database
import numpy as np
import requests
from . import dateLib

spec_chars = ["!", '"', "#", "%", "&", "'" ,"\(","\)"
    ,"\*" ,"\+"  ,","   ,"-" , "/"  ,":",";", "<","=", ">"
    ,"\?","@","\[","\]","^","_","`", "{"
    , "}", "~", "–"
        ]

router = APIRouter(
    prefix="/api",
    tags=["Distributor Apis"]
)

conn_string = 'postgresql://franchise:franchisePassword!123!456@34.65.6.130:5432/franchise_db'

# url = f"http://34.65.6.130:9000/Full_Load_IBL_PRODUCTS?apikeyCode=iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup"
# product_response = requests.get(url)
# product_df = pd.json_normalize(product_response.json())
# product_id = product_df['item_code']

today = date.today()

@router.post('/uploadSalesData/{company_code}/{user_id}', status_code=status.HTTP_201_CREATED)
async def upload_sales_file(company_code, user_id, files: UploadFile = File(...)):
    df = pd.read_excel(BytesIO(files.file.read()), sheet_name="Sales")
    
    # is_exist = df['IBL Item Code'].astype(str).isin(product_id.str.lstrip('0'))

    # if is_exist.isin([False]).any():
    #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
    #                         detail="Invalid IBL item codes used in Sales Sheet")

    df.columns = df.columns.str.strip()

    def checkColumnsinFile():

        print(df.info())
        if len(df.columns) > 26:
            print('File contains one or more extra columns')
        
        if 'Franchise Customer OrderNo' not in df.columns:
            print('Franchise Customer OrderNo) column not found')
        
        if 'Franchise Customer Invoice Date' not in df.columns:
            print('Invoice Date column not found')
        
        if 'Franchise Customer Invoice Number' not in df.columns:
            print('Invoice number column not found')
        
        if 'Channel' not in df.columns:
        print('Channel column not found')
        
        if 'Franchise Code' not in df.columns:
            print(  
                                detail="Franchise Code column not found")
        
        if 'Franchise Customer Number' not in df.columns:
            print('Franchise Customer Number column not found')
        
        if 'IBL Customer Number' not in df.columns:
            print(  
                                detail="IBL Customer Number column not found")
        if 'RD Customer Name' not in df.columns:
            print('RD Customer Name) column not found')
        
        if 'IBL Customer Name' not in df.columns:
            print('IBL Customer Name column not found')

        if 'Customer Address' not in df.columns:
            print('Customer Address column not found')

        if 'Franchise Item Code' not in df.columns:
            print("Franchise Item Code column not found")
            
        if 'IBL Item Code' not in df.columns:
            print("IBL Item Code column not found")
        
        if 'Franchise Item Description' not in df.columns:
            print("Franchise Item Description not found")

        if 'IBL Item Description' not in df.columns:
            print("IBL Item Description not found")

        if 'Quantity Sold' not in df.columns:
            print("Quantity Sold not found")
            
        if 'Gross Amount' not in df.columns:
            print("Gross Amount not found")
            
        if 'Reason' not in df.columns:
            print("Reason not found")
        
        if 'FOC' not in df.columns:
            print("FOC column not found")

        if 'BATCH_NO' not in df.columns:
            print("BATCH_NO column not found")

        if 'PRICE' not in df.columns:
            print("PRICE column not found")

        if 'BON_QTY' not in df.columns:
            print("BON_QTY column not found")


        if 'DISC_AMT' not in df.columns:
            print("DISC_AMT not found")
        
        if 'NET_AMT' not in df.columns:
            print("NET_AMT not found")

        if 'DISCOUNTED_RATE' not in df.columns:
            print("DISCOUNTED_RATE not found")

        if 'Brick Code' not in df.columns:
            print("Brick Code not found")

        if 'Brick Name' not in df.columns:
            print("Brick Name not found")


            print(df.info())
            if len(df.columns) > 26:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                    detail="File contains one or more extra columns")
            
            if 'Franchise Customer OrderNo' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="(Franchise Customer OrderNo) column not found")
            
            if 'Franchise Customer Invoice Date' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Invoice Date column not found")
            
            if 'Franchise Customer Invoice Number' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Invoice number column not found")
            
            if 'Channel' not in df.columns:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Channel column not found")
            
            if 'Franchise Code' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Franchise Code column not found")
            
            if 'Franchise Customer Number' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Franchise Customer Number column not found")
            
            if 'IBL Customer Number' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="IBL Customer Number column not found")
            if 'RD Customer Name' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="(RD Customer Name) column not found")
            
            if 'IBL Customer Name' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="IBL Customer Name column not found")

            if 'Customer Address' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Customer Address column not found")

            if 'Franchise Item Code' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Franchise Item Code column not found")
            if 'IBL Item Code' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="IBL Item Code column not found")
            
            if 'Franchise Item Description' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Franchise Item Description not found")

            if 'IBL Item Description' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="IBL Item Description not found")

            if 'Quantity Sold' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Quantity Sold not found")
            if 'Gross Amount' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="Gross Amount not found")
            if 'Reason' not in df.columns:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Reason not found")
            
            if 'FOC' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="FOC column not found")

            if 'BATCH_NO' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="BATCH_NO column not found")

            if 'PRICE' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="PRICE column not found")

            if 'BON_QTY' not in df.columns:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail="BON_QTY column not found")


            if 'DISC_AMT' not in df.columns:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="DISC_AMT not found")
            
            if 'NET_AMT' not in df.columns:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="NET_AMT not found")

            if 'DISCOUNTED_RATE' not in df.columns:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="DISCOUNTED_RATE not found")

            if 'Brick Code' not in df.columns:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Brick Code not found")

            if 'Brick Name' not in df.columns:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Brick Name not found")



    def validateData():
        try:    
            df['Franchise Customer Invoice Date'] = pd.to_datetime(df['Franchise Customer Invoice Date'])
        except Exception as  ex:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                  detail="Franchise Customer Invoice Date contains other than Date value")
        

 
        # df['Franchise Customer Invoice Date'] = pd.to_datetime(df['Franchise Customer Invoice Date'])

        # if df['Franchise Customer Invoice Date'].dtypes != date:
        #         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
        #                             detail="Franchise Customer Invoice Date contains other than Date value")

        if df['Franchise Customer Invoice Number'].isnull().values.any():
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                    detail="Franchise Customer Invoice Number cannot be null")

        if df['Channel'].isnull().values.any():
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                    detail="Channel cannot be null")

        # compare_user_id = np.where(
        #     df["Franchise Code"] == int(user_id), True, False)[0]

        # if compare_user_id == False:
        #     raise HTTPException(
        #         status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Distributor code in sales file"
        #     )        


        if df['Franchise Code'].apply(lambda x: len(str(x))>10 or len(str(x))<10).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Franchise Code data length must not greater or less than 10 character")
        


        if df['Franchise Code'].dtypes == object:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Franchise Code contains alphanumeric data")
        

        date_validation = df['Franchise Customer Invoice Date'].dt.date < datetime64(
            today)
        if date_validation.all() != True:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Date in sales file")






        if df['IBL Customer Number'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="IBL Customer Number column contain null values.")



        if df['IBL Customer Number'].dtypes==object:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="IBL Customer Number must not contains alphanumeric")
        

        if df['Franchise Item Code'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="Franchise Item Code column contain null values.")
        

        if df['Franchise Item Code'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Franchise Item Code should contain integer values")

        if df['IBL Item Code'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="IBL item code should contain integer values")

        if df['IBL Item Code'].apply(lambda x: len(str(x))>11).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="IBL Item Code length must not greater or less than 11 character")
        
        if (~df['Reason'].isin(['Sales', 'Return'])).any():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Reason column can contains only (Sales/Return)")



# ----------------------

        if df['Quantity Sold'].isnull().values.any():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="Quantity Sold column contain null values.")

        if df['Quantity Sold'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Quantity Sold should contain integer values ")


        if df['FOC'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="FOC should contain integer values")

        # if df['Gross Amount'].isnull().values.any():
        #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
        #                         detail="Gross Amount column contain null values.")
        
        if df['Gross Amount'].dtypes != float:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Gross Amount should contain integer values")



        # ===Type check start==
        

        if df['Franchise Customer Invoice Date'].dtypes == datetime:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="(Franchise Customer Invoice Date) column have invalid values.")

        if df['Franchise Item Description'].dtypes != object:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Franchise Item Description should contain integer values")
        
        if df['Quantity Sold'].dtypes != int64:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Quantity Sold should contain integer values")

        # ===Type check end==

    checkColumnsinFile()
    validateData()

    db = create_engine(conn_string)
    conn = db.connect()

    get_list_invoice_date = pd.read_sql_query(
        f'''
        select distinct franchise_customer_invoice_date invoice_date 
        from franchise."franchise_sales_1" 
        where franchise_code='{user_id}' ''', con=conn)

    print('dates', df['Franchise Customer Invoice Date'].unique())

    df['Franchise Customer Invoice Date'] = pd.to_datetime(
        df['Franchise Customer Invoice Date']).dt.date

    for x in range(len(df['Franchise Customer Invoice Date'].unique())):
        get_list_invoice_date = pd.read_sql_query(
            f"""
            select distinct franchise_customer_invoice_date invoice_date 
            from franchise."franchise_sales_1" 
            where franchise_customer_invoice_date = '{df['Franchise Customer Invoice Date'].unique()[x]}' 
            and franchise_code='{user_id}' """, con=conn)
        get_list_invoice_date['invoice_date'] = pd.to_datetime(
            get_list_invoice_date['invoice_date']).dt.date
        data = get_list_invoice_date['invoice_date'].to_numpy().all()
        print('data')
        print(data)

        if data is not True:
            print(
                f'''DELETE FROM franchise."franchise_sales_1" 
                WHERE franchise_customer_invoice_date = '{data}' 
                AND franchise_code = '{user_id}' ''')
                
            sql_statement = text('DELETE FROM franchise_sales_1 WHERE franchise_customer_invoice_date = :invoice_date AND franchise_code = :ibl_distributor_code')

    # Establish a database connection and execute the SQL statement
            with db.connect() as conn:
                try:
                    result = conn.execute(
                        sql_statement, invoice_date=data, ibl_distributor_code=user_id)
                    return {"message": f"{result.rowcount} record(s) deleted successfully."}
                except Exception as e:
                    return HTTPException(status_code=500, detail=str(e))

    df.columns = ['franchise_customer_order_no', 'franchise_customer_invoice_date', 'franchise_customer_invoice_number'
                  , 'channel', 'franchise_code'
                  , 'franchise_customer_number', 'ibl_customer_number','rd_customer_name',
                  'ibl_customer_name', 'customer_address', 'franchise_item_code', 'ibl_item_code'
                  , 'franchise_item_description','ibl_item_description'
                  ,'quantity_sold', 'gross_amount', 'reason','foc','batch_no','price','bon_qty'
                  ,'disc_amt', 'net_amt','discounted_rate','brick_code','brick_name']
    df['company_code'] = company_code
    df['created_date'] = today

    # df['discount'] = df['discount'].replace(NaN, 0)
    # df['bonus_qty'] = df['bonus_qty'].replace(NaN, 0)

    start_time = time.time()
    success = df.to_sql('franchise_sales_1', schema="franchise",
                        if_exists='append', con=conn, index=False)
    # conn.commit()
    if success:
        print("Data insertion was successful.")
    else:
        print("Data insertion failed.")

    # pg_conn_sales = psycopg2.connect(conn_string)
    # pg_conn_sales.commit()

    total_sales_quantity = df['quantity_sold'].sum()
    total_sales_gross_amount = df['gross_amount'].sum()
    total_sales_discount = df['disc_amt'].sum()
    total_sales_bonus_quantity = df['bon_qty'].sum()
    total_sales_SKU = df['ibl_item_code'].nunique(dropna=True)
    total_sales_rows = df.shape[0]

    return {
        "Sales_Quantity": int(total_sales_quantity),
        "Sales_Gross_Amount": int(total_sales_gross_amount),
        "Sales_Discounts": int(total_sales_discount),
        "Sales_Bonus_Quantity": int(total_sales_bonus_quantity),
        "Total_Sales_SKUs": int(total_sales_SKU),
        "Total_Sales_Rows": int(total_sales_rows),
    }

# test Case new format sale
# @router.post('/uploadSalesDataFormat/{company_code}/{user_id})', status_code=status.HTTP_201_CREATED)

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
    
# test Case new format sale -- End


@router.post('/uploadStockData/{company_code}/{user_id}', status_code=status.HTTP_201_CREATED)
async def upload_stock_file(company_code, user_id, stockFiles: UploadFile = File(...)):
    df = pd.read_excel(BytesIO(stockFiles.file.read()), sheet_name='Stock')
    print('stock')
    print(df.info())
    # is_exist = df['IBL ItemCode'].astype(str).isin(product_id.str.lstrip('0'))

    # if 'AZ Distributor Code' not in df.columns:
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Distributor code not found")
    # if 'AZ Distributor ItemCode' not in df.columns:
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Distributor item code not found")

    if 'IBL Item Code' not in df.columns:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="(IBL Item Code) not found in Stock")
    
    # if 'AZ Distributor Item Description' not in df.columns:
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Item description not found")

    if 'LOT NUMBER' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(LOT NUMBER) not found in Stock")

    if 'Expiry Date' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(Expiry Date) not found in Stock")

    if 'Closing Quantity' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(Closing Quantity) not found in Stock")

    if 'Value' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(Value) not found in Stock")

    if 'Dated' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dated not found in Stock")

    if 'Price' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(Price) not found in Stock")

    if 'In-Transit stock' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(In-Transit stock) not found in Stock")

    if 'Purchase Unit' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(Purchase Unit) not found in Stock")

    if 'RD Item Code' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(RD Item Code) not found in Stock")

    if 'RD Code' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(RD Code) not found in Stock")

    if 'IBL Branch code' not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="(IBL Branch code) not found in Stock")

    # # ====Null Check start====

    if df['IBL Item Code'].isnull().values.any():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="(IBL Item Code) column contain null values in Stock")

    # if df['AZ Distributor Item Description'].isnull().values.any():
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Item description column contain null values.")
    # if df['LOT Number'].isnull().values.any():
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Stock Lot Number column contain null values.")
    # if df['Expiry Date'].isnull().values.any():
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Stock Expiry Date column contain null values.")
    # if df['Quantity'].isnull().values.any():
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Stock Quantity column contain null values.")
    # if df['Value'].isnull().values.any():
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Stock Value column contain null values.")
    # if df['Dated'].isnull().values.any():
    #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
    #                         detail="Stock Dated column contain null values.")
    # # ====NUll Check end====
    # # ====Type check start====
    # if df['AZ Distributor Item Description'].dtypes != object:
    #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
    #                         detail="Item Description should not contain integer values")
    # if df['LOT Number'].dtypes != object:
    #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
    #                         detail="Stock Lot Number contain invalid values")
    # if df['Quantity'].dtypes != int64:
    #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
    #                         detail="Stock Quantity should contain integer values")
    # if df['Dated'].dtypes == datetime:
    #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
    #                         detail="Stock Dated column contain Invalid values.")

    # compare_user_id = np.where(
    #     df["AZ Distributor Code"] == int(user_id), True, False)[0]

    # if compare_user_id == False:
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Distributor code"
    #     )

    # today = date.today()
    # date_validation = df['Dated'].dt.date < datetime64(
    #     today)
    # if date_validation.all() != True:
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Date")

    df.columns = ['ibl_distributor_code','ibl_branch_code', 'distributor_item_code', 'ibl_item_code',
                  'distributor_item_description'
                  , 'lot_number', 'expiry_date', 'stock_qty', 'stock_value', 'dated','price','in_transit_stock'
                  ,'purchase_unit']

    df['company_code'] = company_code
    # # df['current_dates'] = today

    db = create_engine(conn_string)
    conn = db.connect()

    # get_list_invoice_date = pd.read_sql_query(
    #     f'''select distinct dated from test_schema."test_franchise_stock" where ibl_distributor_code='{user_id}' ''', con=conn)

    # df['dated'] = pd.to_datetime(df['dated']).dt.date

    # for x in range(len(get_list_invoice_date['dated'].values)):
    #     compare_user_id = np.where(df['dated'].isin(
    #         get_list_invoice_date['dated']), True, False)
    #     if compare_user_id.any() == True:
    #         print(get_list_invoice_date['dated'][x])
    #         sql_statement = f'''
    #         DELETE FROM test_schema."test_franchise_stock" 
    #         WHERE dated = :dated 
    #         AND ibl_distributor_code = :ibl_distributor_code
    #         '''

    # # Establish a database connection and execute the SQL statement
    #         with db.connect() as conn:
    #             try:
    #                 result = conn.execute(
    #                     sql_statement, dated=get_list_invoice_date['user_id'][x], ibl_distributor_code=ibl_distributor_code)
    #                 return {"message": f"{result.rowcount} record(s) deleted successfully."}
    #             except Exception as e:
    #                 return HTTPException(status_code=500, detail=str(e))

    # start_time = time.time()
    df.to_sql('franchise_stock', schema="franchise",
              if_exists='append', con=conn, index=False)
    
    # # conn.commit()
    # pg_conn = psycopg2.connect(conn_string)
    # pg_conn.commit()
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
            and date(fs2.created_date) = {b}
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
        f"""select * from franchise."franchise_sales" fs2 where ibl_distributor_code = '{userId}' 
            and date(fs2."current_dates") between '{from_date}' and '{to_date}'""", con=conn
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
        f"""select * from franchise."franchise_stock" fs2 where ibl_distributor_code = '{userId}' and "current_dates" between '{from_date}' and '{to_date}';""", con=conn
    )
    print(read_stock)
    return read_stock.to_dict(orient='records')


@router.get('/getStatusSalesByInvoiceDate/{userId}/{from_date}/{to_date}', status_code=status.HTTP_200_OK)
def get_status(userId, from_date, to_date, db: Session = Depends(database.get_db)):
    db = create_engine(conn_string)
    conn = db.connect()
    read_status_by_invoice_id = pd.read_sql_query(
        f"""
            select  date, ibl_distributor_code,  total_gross_amount,l.location_name  from (
            select date(franchise_customer_invoice_date),fs2.franchise_code  ibl_distributor_code
            , sum(gross_amount) as total_gross_amount from franchise."franchise_sales" fs2
            where 1=1 and fs2.franchise_code  = '{userId}' and date(fs2.franchise_customer_invoice_date) between '{from_date}' and '{to_date}' 
            group by  fs2.franchise_customer_invoice_date ,fs2.franchise_code 
            union
            select date(dates), '{userId}' as ibl_distributor_code 
            , null as total_gross_amount from generate_series('{from_date}','{to_date}', interval '1 day') as dates
            where dates not in ( select date(franchise_customer_invoice_date) 
            from franchise."franchise_sales" fs2
            where fs2.franchise_code = '{userId}' and date(franchise_customer_invoice_date) between '{from_date}' and '{to_date}' group by  franchise_customer_invoice_date )
            order by 1
            ) fss
            inner join franchise.users u  on fss.ibl_distributor_code::varchar = u.distributor_id
            inner join franchise.locations l  on u.location_id =l.location_id
        """, con=conn)
    read_status_by_invoice_id['total_gross_amount'] = read_status_by_invoice_id['total_gross_amount'].replace(
        np.nan, 'null')
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
