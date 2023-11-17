import pandas as pd
from connectionClass import FranchiseAlchmy
import numpy as np

franchiseEngine=FranchiseAlchmy()

df=pd.DataFrame()

read_status_by_invoice_id = pd.read_sql_query(
        f"""
            select  date, ibl_distributor_code,  total_gross_amount,l.location_name  from (
            select date(franchise_customer_invoice_date),fs2.franchise_code  ibl_distributor_code
            , sum(gross_amount) as total_gross_amount from franchise."franchise_sales" fs2
            where 1=1
              and fs2.franchise_code  = '2000217691' and date(fs2.franchise_customer_invoice_date) between '2023-11-01' and '2023-11-14' 
            group by  fs2.franchise_customer_invoice_date ,fs2.franchise_code             
            ) fss
            inner join franchise.users u  on fss.ibl_distributor_code::varchar = u.distributor_id
            inner join franchise.locations l  on u.location_id =l.location_id
        """, con=franchiseEngine)

read_status_by_invoice_id['total_gross_amount'] = read_status_by_invoice_id['total_gross_amount'].replace(
        np.nan, 'null')

read_status_by_invoice_id.to_csv('getstatusSales.csv')   
print(read_status_by_invoice_id.info())
print(
    read_status_by_invoice_id.to_dict(orient="records")
)

print('done....')