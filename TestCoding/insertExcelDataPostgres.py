import pandas as pd
from connectionClass import FranchiseAlchmy

franchiseEngine=FranchiseAlchmy()

df=pd.DataFrame()

# df=pd.read_excel('CustomersFranchise.xlsx',index_col=False)
# df.to_sql('missing_customers',con=franchiseEngine
#           ,schema='franchise'
#             ,if_exists='replace')

# df=pd.read_csv('AugustFranchise.csv',index_col=None,low_memory=False)
df=pd.read_excel(
          'Oct_Sales.xlsx',sheet_name='sales01',index_col=None
                 )
df.columns = df.columns.str.strip()
print(df.info())

df.columns = (
    'franchise_customer_order_no', 'franchise_customer_invoice_date','franchise_customer_invoice_number'
    , 'channel', 'franchise_code','franchise_customer_number', 'ibl_customer_number','rd_customer_name'
    , 'ibl_customer_name', 'customer_address' , 'franchise_item_code', 'ibl_item_code'
    , 'franchise_item_description', 'ibl_item_description' , 'quantity_sold', 'gross_amount', 'reason'
    , 'foc', 'batch_no', 'price', 'bon_qty' , 'disc_amt', 'net_amt', 'discounted_rate', 'brick_code'
    , 'brick_name'
                  )

df['brick_code']=df['brick_code'].astype('str')
df['brick_code'] = df['brick_code'].fillna(0)

print(df.info())

df.to_sql('franchise_sales_oct',con=franchiseEngine
          ,schema='franchise',index=False
            ,if_exists='append')

print('done....')