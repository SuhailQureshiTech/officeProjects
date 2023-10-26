import time
import requests
import regex as re
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as pg
import psycopg2.extras
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from dateutil import parser
from datetime import datetime,date
from datalab.context import Context
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
from airflow.utils.email import send_email_smtp
# from sqlalchemy import create_engine

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Searle Stock Exchange DAG has executed successfully."
    subject = f"Searle Stock Exchange DAG has completed"
    send_email_smtp(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)


default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 5, 9),
        # 'end_date': datetime(),
        'depends_on_past': False,
        'email_on_failure': True,
        'email': ['shehzad.lalani@iblgrp.com'],
        'on_success_callback':success_function,        
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        
        }

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_time = datetime.today()
curr_date = today.strftime("%d-%b-%Y")
### Postgres Connection
#conn = pg.connect( host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
cursor = conn.cursor()

def extract_psx_searle_data_scrap_to_postgres():

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
    url = 'https://dps.psx.com.pk/company/SEARL'

    psx_company = []
    psx_tabs = []
    stat_lbl = []
    stat_value = []
    odl_stat_lbl = []
    odl_stat_value = []
    fut_stat_lbl = []
    fut_stat_value = []
    reg_Stat = {}
    fut_Stat = {}
    odl_Stat = {}
    company_info = {}
    k = requests.get(url).text
    soup=BeautifulSoup(k,'html.parser')

    c_today = date.today()    

    for company_details in soup.find_all('div', {"class":"quote__details"}):
        for name in company_details.find('div', {"class":"quote__name"}):
            psx_company.append(name.text)
        for sector in company_details.find('div', {"class":"quote__sector"}):
            psx_company.append(sector.text)    
        for quote_close in company_details.find('div', {"class":"quote__close"}):
            psx_company.append(quote_close.text.replace('Rs.', ''))
        for  change_value in company_details.find('div', {"class":"change__value"}):
            psx_company.append(change_value.text)
        for  change_perc in company_details.find('div', {"class":"change__percent"}):
            psx_company.append(change_perc.text.strip().replace('(','').replace(')','').replace('%',''))
        for  quote_date in company_details.find('div', {"class":"quote__date"}):
            q_date = quote_date.text
            q_date =re.sub(r'^.*?, ', '', q_date)
            psx_company.append(q_date)
           




    for tab_elems in soup.find_all('div', {"class":"tabs__list__item"},limit=3):
        psx_tabs.append(tab_elems.text)
    # REG Stats
    for stats in soup.find_all(attrs={"data-name" : "REG"}):
        for stat in stats.find_all('div', {"class":"stats--noborder"}):
            for regstat in stat.find_all('div', {"class":"stats_item"}):
                for st_lbl in regstat.find_all('div', {"class":"stats_label"}):
                    stat_lbl.append('reg_' + st_lbl.text)
                for st_val in regstat.find_all('div', {"class":"stats_value"}):
                    stat_value.append(st_val.text)




    # FUT Stats
    for fut_stats in soup.find_all(attrs={"data-name" : "FUT"}):
            for fut_stat in fut_stats.find_all('div', {"class":"stats"}):
                for futstat in fut_stat.find_all('div', {"class":"stats_item"}):
                    for fut_st_lbl in futstat.find_all('div', {"class":"stats_label"}):
                        fut_stat_lbl.append('fut_' + fut_st_lbl.text)
                    for fut_st_val in futstat.find_all('div', {"class":"stats_value"}):
                        fut_stat_value.append(fut_st_val.text)

    # ODL Stats
    for odl_stats in soup.find_all(attrs={"data-name" : "ODL"}):
        for odl_stat in odl_stats.find_all('div', {"class":"stats"}):
            for odlstat in odl_stat.find_all('div', {"class":"stats_item"}):
                for odl_st_lbl in odlstat.find_all('div', {"class":"stats_label"}):
                    odl_stat_lbl.append('odl_' + odl_st_lbl.text)
                for odl_st_val in odlstat.find_all('div', {"class":"stats_value"}):
                    odl_stat_value.append(odl_st_val.text)



    del fut_stat_lbl[8:]
    del fut_stat_value[8:]
    del odl_stat_lbl[8:]
    del odl_stat_value[8:]
    reg_Stat =  dict(zip(stat_lbl, stat_value))
    reg_Stat['reg_Volume'] = re.sub("[^\d\.]", "", reg_Stat['reg_Volume'])
    odl_Stat = dict(zip(odl_stat_lbl, odl_stat_value))
    odl_Stat['odl_Volume'] = re.sub("[^\d\.]", "", odl_Stat['odl_Volume'])
    fut_Stat = dict(zip(fut_stat_lbl, fut_stat_value))
    fut_Stat['fut_Volume'] = re.sub("[^\d\.]", "", fut_Stat['fut_Volume'])


    company_df = pd.DataFrame(psx_company).T
    company_df.columns =['company_name', 'sector_name', 'rate', 'change_value', 'change_percentage', 'quote_date']
    company_df['quote_date'] = pd.to_datetime(company_df["quote_date"])
    company_df.insert(6,"compare_date",date.today())    
    company_df.insert(0,"company_code",1000)    
    compare = pd.to_datetime(company_df['quote_date']).dt.normalize() < company_df['compare_date']
    company_df = company_df.drop('compare_date', 1)    
        
        


    reg_df = pd.DataFrame(reg_Stat, index=[0])
    reg_df.columns=['reg_open', 'reg_high', 'reg_low', 'reg_volume']
    



    fut_df = pd.DataFrame(fut_Stat, index=[0])
    #fut_df.drop(['fut_CIRCUIT BREAKER', 'fut_DAY RANGE', 'fut_Ask Price', 'fut_Ask Volume', 'fut_Bid Price', 'fut_Bid Volume'], inplace=True, axis=1)
    fut_df.columns=['fut_open', 'fut_high', 'fut_low', 'fut_close', 'fut_change', 'fut_ldcp', 'fut_volume', 'fut_total_trades']

    odl_df = pd.DataFrame(odl_Stat, index=[0])
    #odl_df.drop(['odl_CIRCUIT BREAKER', 'odl_DAY RANGE', 'odl_Ask Price', 'odl_Ask Volume', 'odl_Bid Price', 'odl_Bid Volume'], inplace=True, axis=1)
    odl_df.columns=['odl_open', 'odl_high', 'odl_low', 'odl_close', 'odl_change', 'odl_ldcp', 'odl_volume', 'odl_total_trades']
    #print(odl_df)
    if(compare[0]==True):
        reg_df['reg_volume'] = 0
        fut_df['fut_volume'] = 0
        odl_df['odl_volume'] = 0    
        
    dfs = [company_df, reg_df, fut_df, odl_df]
    df_final = pd.concat(dfs, join='outer', axis=1)

    df_final['date'] = pd.to_datetime(df_final['quote_date']).dt.normalize()

    
    if len(df_final) > 0:
            df_columns = list(df_final)    
            columns = ",".join(df_columns)

    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {}".format('"DW"."IBL_PSX"',columns,values)
    pg.extras.execute_batch(cursor, insert_stmt, df_final.values)
    conn.commit()
    update_sql_fut = """ UPDATE "DW"."IBL_PSX"  set fut_volume_difference  = v_table_name.rn  FROM  
    (
        SELECT fut_volume - LAG(fut_volume)    OVER ( partition by company_code,date(created_at) ORDER BY created_at ) AS rn, company_Code,created_at
        FROM "DW"."IBL_PSX" where company_code = 1000
    ) AS v_table_name
    WHERE "DW"."IBL_PSX".company_Code = v_table_name.company_Code and "DW"."IBL_PSX".created_at = v_table_name.created_at """
    update_sql_reg = """ UPDATE "DW"."IBL_PSX"  set reg_volume_difference  = v_table_name.rn  FROM  
    (
        SELECT reg_volume - LAG(reg_volume)    OVER ( partition by company_code,date(created_at) ORDER BY created_at ) AS rn, company_Code,created_at
        FROM "DW"."IBL_PSX" where company_code = 1000
    ) AS v_table_name
    WHERE "DW"."IBL_PSX".company_Code = v_table_name.company_Code and "DW"."IBL_PSX".created_at = v_table_name.created_at """
    update_sql_odl = """ UPDATE "DW"."IBL_PSX"  set odl_volume_difference  = v_table_name.rn  FROM  
    (
        SELECT odl_volume - LAG(odl_volume)    OVER ( partition by company_code,date(created_at) ORDER BY created_at ) AS rn, company_Code,created_at
        FROM "DW"."IBL_PSX" where company_code = 1000
    ) AS v_table_name
    WHERE "DW"."IBL_PSX".company_Code = v_table_name.company_Code and "DW"."IBL_PSX".created_at = v_table_name.created_at """
    cursor.execute(update_sql_fut)
    cursor.execute(update_sql_reg)
    cursor.execute(update_sql_odl)
    conn.commit()    
    cursor.close()         

def insert_PSX_Searle_data_To_GCS():
    df = pd.read_sql('select *  from "DW"."IBL_PSX" where company_code =\'1000\' order by company_code,created_at desc limit 1', conn);
    df_searle = pd.DataFrame(data=df)
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    filename =f'ibl_psx_searle_{curr_date}'

    bucket.blob(f'staging/psx/searle/{filename}.csv').upload_from_string(df_searle.to_csv(index=False), 'text/csv')
    conn.close()

    
ibl_psx_searle = DAG(
    dag_id='ibl_psx_searle',
    default_args=default_args,
    schedule_interval='0 04-11 * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='ibl_psx_searle',
)


psx_searle_data_scrap_to_postgres = PythonOperator(
        task_id="psx_searle_data_scrap_to_postgres",
        python_callable=extract_psx_searle_data_scrap_to_postgres,
        dag=ibl_psx_searle
)

psx_searle_postgres_to_GCS =  PythonOperator(
    task_id="psx_searle_data_to_GCS",
    python_callable=insert_PSX_Searle_data_To_GCS,
    dag=ibl_psx_searle
)

psx_searle_data_gcs_to_bq = GCSToBigQueryOperator(    
    task_id='psx_searle_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/psx/searle/ibl_psx_searle_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_PSX',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'company_code', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'company_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sector_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'rate', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'change_value', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'change_percentage', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'quote_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}, 
    {'name': 'reg_open', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'reg_high', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'reg_low', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'reg_volume', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'fut_open', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'fut_high', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'fut_low', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'fut_close', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'fut_change', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'fut_ldcp', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'fut_volume', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'fut_total_trades', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'odl_open', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'odl_high', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'odl_low', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'odl_close', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'odl_change', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'odl_ldcp', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'odl_volume', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'odl_total_trades', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'reg_volume_difference', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'fut_volume_difference', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'odl_volume_difference', 'type': 'Numeric', 'mode': 'NULLABLE'},       
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=ibl_psx_searle
)

psx_searle_data_scrap_to_postgres >> psx_searle_postgres_to_GCS >> psx_searle_data_gcs_to_bq







           





