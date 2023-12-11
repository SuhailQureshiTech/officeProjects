
from datetime import date,datetime
# import datetime
from numpy import datetime64, int64
import pandas as pd
import connectionClass
connections=connectionClass
franchiseEngine=connections.FranchiseAlchmy()
today = date.today()
print('starting')

df = pd.read_excel('Sales and Stock.xlsx', sheet_name="Stock")

df.columns = df.columns.str.strip()
print(df.info())
date1=str(date.today())
print(type(date1))
print(date1)

chkPd=pd.DataFrame()
chkPd=df[df['Dated']!=today] 
if len(chkPd)>=1:
    print('Found entry')

# if df['IBL Item Code'].apply(lambda x: len(str(x))>11 ).any() or df['IBL Item Code'].apply(lambda x: len(str(x))<10).any():
#     print('IBL Item Code length must not greater than 11 character')        


# df.columns = ['franchise_customer_order_no', 'franchise_customer_invoice_date', 'franchise_customer_invoice_number'
#                   ,'channel', 'franchise_code', 'franchise_customer_number', 'ibl_customer_number'
#                   ,'rd_customer_name','ibl_customer_name', 'customer_address', 'franchise_item_code'
#                   ,'ibl_item_code','franchise_item_description','ibl_item_description'
#                   ,'quantity_sold', 'gross_amount', 'reason','foc','batch_no','price','bon_qty'
#                   ,'disc_amt', 'net_amt','discounted_rate','brick_code','brick_name']
    

# print(df)
# success = df.to_sql('franchise_sales_2', schema="franchise",
#                     if_exists='replace', con=franchiseEngine, index=False)


# if success:
#     print("Data insertion was successful.")
# else:
#     print("Data insertion failed.")

# print(df.info())
# print(df.shape[0])
# df2 = df.groupby('franchise_customer_invoice_date')['quantity_sold'].sum().reset_index()

# print(df2)

print('Done..............')