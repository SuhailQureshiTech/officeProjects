
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
from datetime import date, datetime, timedelta
import psycopg2 as pg
import pandas as pds
import numpy as np

def insertBigQuery():
    credentials = service_account.Credentials.from_service_account_file(
        'd://data-light-house-prod-0baa98f57152 (1).json')


    vEndDate = datetime.date(
        datetime.today()-timedelta(days=0)).strftime("%Y%m%d")
    vEndDate = "'"+vEndDate+"'"
    print('end date : ', vEndDate)

    conn = pg.connect(host="35.216.168.189", port='5433',
                    database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    # Read data from PostgreSQL database table and load into a DataFrame instance
    # df = pds.read_sql(f'''
    #                 select
    #                 cast("PLANT" as text) as   company_code,cast(current_date  as timestamp),"VAL_TYPE" ,"TRADE_PRICE" ,trim("MANUFACTURING_DATE")"MANUFACTURING_DATE"
    #                 ,pcos ."MATERIAL"  as material_code,"PLANT" as plant_code,"STORAGE_LOCATION"  as storage_location,null as s,null as valuation,null as stock_indicator,null as sl
    #                 ,pcos ."BATCH" as batch,"BUn" as bun, "UNRESTRICTED"  as unrestricated,"CRCY"  as crcy, "TOTAL_VALUE_1" as total_value1,pcos ."TRANSIT" as transit_stock
    #                 ,pcos ."TOTAL_VALUE_2"  as total_value_2,"IN_QUALITY_INSPECTION"  as in_quality_inspection,"TOTAL_VALUE_3"  as total_value_3
    #                 ,"RESTRICTED_USE"  as restricted_use_stock, "TOTAL_VALUE_4"  as total_value_4,"BLOCKED"  as blocked_stock,"TOTAL_VALUE_5"  as total_value_5,"RETURNS"  as returned_stock
    #                 ,"EXPIRY_DATE"  as expiry_date           from "DW".phnx_consumer_ops_stock pcos
    #                 ''', conn)

    df = pds.read_sql(f'''select  cast("PLANT" as text) as   company_code,CURRENT_DATE dated,"VAL_TYPE" val_type,"TRADE_PRICE"  trade_price
                            ,"MANUFACTURING_DATE" manufacturing_date,"MATERIAL" material_code
                            ,"PLANT" plant_code,"STORAGE_LOCATION" storage_location
                    from "DW".phnx_consumer_ops_stock pcos
                    ''', conn)

    # df['storage_location'] = df['storage_location'].fillna(NaN, inplace=True)
    # df = df.replace(to_replace=" NULL", value=0)

    df = df.replace(r'^\s+$', np.nan, regex=True)

    # print(df)
    # df['company_code'].astype('|S')
    df['plant_code'].astype(int)
    # df['storage_location'].astype(str).astype(int)
    df['manufacturing_date'] = pd.to_datetime(df['manufacturing_date'])
    # print(df.info())

    df_stock = pds.DataFrame(data=df)
    df_stock.to_csv('d:\\TEMP\\mango.csv')

    project_id = 'data-light-house-prod'
    # table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    # pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')

    # dml_statement = (f'''
    #             DELETE FROM  `data-light-house-prod.EDW.TEST`
    #             '''
    #     )

    # query_job = client.query(dml_statement)  # API request
    # query_job.result()

    table_id = 'data-light-house-prod.EDW.TEST'

    table_schema = [
                {'name': 'company_code', 'type': 'string'}
                ,{'name': 'dated', 'type': 'DATE', 'mode': 'NULLABLE'}
                , {'name': 'val_type', 'type': 'string', 'mode': 'NULLABLE'}
                , {'name': 'trade_price', 'type': 'numeric', 'mode': 'NULLABLE'}
                , {'name': 'manufacturing_date', 'type': 'DATE', 'mode': 'NULLABLE'}
                , {'name': 'material_code', 'type': 'string', 'mode': 'NULLABLE'}
                , {'name': 'plant_code', 'type': 'numeric', 'mode': 'NULLABLE'}
                , {'name': 'storage_location', 'type': 'numeric', 'mode': 'NULLABLE'}

                ]

    print('inserting stock')
    pandas_gbq.to_gbq(df_stock, table_id, project_id=project_id, if_exists='append',table_schema=table_schema)


    # df = results.to_dataframe()
    # print(df)

insertBigQuery()
