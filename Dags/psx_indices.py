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
from airflow.utils.email import send_email
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
# from sqlalchemy import create_engine

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Stock Exchange Indices DAG has executed successfully."
    subject = f"Stock Exchange Indices DAG has completed"
    send_email(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)


default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 5, 19),
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
#print(today)
curr_date = today.strftime("%d-%b-%Y")
#conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
cursor = conn.cursor()

def extract_psx_indices_data_scrap_to_postgres():

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
    url = 'https://dps.psx.com.pk/'

    psx_company1 = []
    psx_tabs = []
    stat_lbl = []
    stat_value = []
    odl_stat_lbl = []
    odl_stat_value = []
    allshr_stat_lbl = []
    allshr_stat_value = []
    kse30_stat_lbl = []
    kse30_stat_value = []
    kse300_stat_lbl= []
    kmi30_stat_lbl = []
    kmi30_stat_value = []
    allshr_Stat = {}
    kse30_Stat = {}
    kmi30_Stat = {}
    company_info = {}
    demo=[]
    demo1=[]
    demo2=[]
    demo3=[]
    demo_pd=[]
    kse_100_marketIndices__price=[]
    k = requests.get(url).text
    soup=BeautifulSoup(k,'html.parser')


    # KSE100 MarketIndices
    for stats in soup.find_all(attrs={"data-name" : "KSE100"}):
        marketIndices__price = stats.find('h1', {"class":"marketIndices__price"})
        if marketIndices__price is not None:
            marketIndices__price1=marketIndices__price.text.strip().replace('(','').replace(')','').replace('%','')
            demo=marketIndices__price1.split(' ')
            demo[0]=demo[0].replace(',','')
            #demo[2].text
            #print(type(demo[2]))
            #print(demo[2])
            
            quote_date = soup.find('div', {"class":"marketIndices__date"})
            q_date = quote_date.text
            q_date = q_date.replace("As of ","")
            #q_date =re.sub(r'^.*?, ', '', q_date)
            demo.append(q_date)
            demo_pd=pd.DataFrame(demo).T
            demo_pd.columns =['kse100_rate', 'kse100_change_value', 'kse100_change_percentage','kse100_quote_date']
            demo_pd.insert(0,'kse100_company_name','KSE100')
                    
        else:
            marketIndices__price1=None

    for stats in soup.find_all(attrs={"data-name" : "ALLSHR"}):
        marketIndices__price = stats.find('h1', {"class":"marketIndices__price"})
        if marketIndices__price is not None:
            marketIndices__price1=marketIndices__price.text.strip().replace('(','').replace(')','').replace('%','')
            demo1=marketIndices__price1.split(' ')
            demo1[0]=demo1[0].replace(',','')
            quote_date1 = soup.find('div', {"class":"marketIndices__date"})
            q_date1 = quote_date1.text
            q_date1 = q_date1.replace("As of ","")
            #q_date1 =re.sub(r'^.*?, ', '', q_date1)
            demo1.append(q_date1)
            demo_pd1=pd.DataFrame(demo1).T
            demo_pd1.columns =['allshr_rate', 'allshr_change_value', 'allshr_change_percentage','allshr_quote_date']
            demo_pd1.insert(0,'allshr_company_name','ALLSHR')
            #print(q_date1)
            
        else:
            marketIndices__price1=None
        
    for stats in soup.find_all(attrs={"data-name" : "KSE30"}):
        marketIndices__price = stats.find('h1', {"class":"marketIndices__price"})
        if marketIndices__price is not None:
            marketIndices__price1=marketIndices__price.text.strip().replace('(','').replace(')','').replace('%','')
            demo2=marketIndices__price1.split(' ')
            demo2[0]=demo2[0].replace(',','')
            quote_date2 = soup.find('div', {"class":"marketIndices__date"})
            q_date2 = quote_date2.text
            q_date2 = q_date2.replace("As of ","")
            #q_date2 =re.sub(r'^.*?, ', '', q_date2)
            demo2.append(q_date2)
            demo_pd2=pd.DataFrame(demo2).T
            demo_pd2.columns =['kse30_rate', 'kse30_change_value', 'kse30_change_percentage','kse30_quote_date']
            demo_pd2.insert(0,'kse30_company_name','KSE30')
            
            
        else:
            marketIndices__price1=None
        
    for stats in soup.find_all(attrs={"data-name" : "KMI30"}):
        marketIndices__price = stats.find('h1', {"class":"marketIndices__price"})
        if marketIndices__price is not None:
            marketIndices__price1=marketIndices__price.text.strip().replace('(','').replace(')','').replace('%','')
            demo3=marketIndices__price1.split(' ')
            demo3[0]=demo3[0].replace(',','')
            quote_date3 = soup.find('div', {"class":"marketIndices__date"})
            q_date3 = quote_date3.text
            q_date3 = q_date3.replace("As of ","")
            #q_date3 =re.sub(r'^.*?, ', '', q_date3)
            demo3.append(q_date3)
            demo_pd3=pd.DataFrame(demo3).T
            demo_pd3.columns =['kmi30_rate', 'kmi30_change_value', 'kmi30_change_percentage','kmi30_quote_date']
            demo_pd3.insert(0,'kmi30_company_name','KMI30')
            demo_pd3['kmi30_change_value'] = demo_pd3['kmi30_change_value'].str.replace(',','')
            
            
        else:
            marketIndices__price1=None

                


    # KSE100 Stats
    for stats in soup.find_all(attrs={"data-name" : "KSE100"}):
        for stat in stats.find_all('div', {"class":"stats"}):
            for kse100stat in stat.find_all('div', {"class":"stats_item"}):
                for st_lbl in kse100stat.find_all('div', {"class":"stats_label"}):
                    stat_lbl.append('kse100_' + st_lbl.text)
                    for  st_val in kse100stat.find_all('div', {"class":"stats_value"}):
                        stat_value.append(st_val.text)
                        

    # ALLSHR Stats
    for allshr_stats in soup.find_all(attrs={"data-name" : "ALLSHR"}):
        for allshr_stat in allshr_stats.find_all('div', {"class":"stats"}):
            for allshrstat in allshr_stat.find_all('div', {"class":"stats_item"}):
                for allshr_st_lbl in allshrstat.find_all('div', {"class":"stats_label"}):
                    allshr_stat_lbl.append('allshr_' + allshr_st_lbl.text)
                    for  allshr_st_val in allshrstat.find_all('div', {"class":"stats_value"}):
                        allshr_stat_value.append(allshr_st_val.text)
    

    # KSE30 Stats
    for kse30_stats in soup.find_all(attrs={"data-name" : "KSE30"}):
        for kse30_stat in kse30_stats.find_all('div', {"class":"stats"}):
            for kse30shrstat in kse30_stat.find_all('div', {"class":"stats_item"}):
                for kse30_st_lbl in kse30shrstat.find_all('div', {"class":"stats_label"}):
                    kse30_stat_lbl.append('kse30_' + kse30_st_lbl.text)
                    for  kse30_st_val in kse30shrstat.find_all('div', {"class":"stats_value"}):
                        kse30_stat_value.append(kse30_st_val.text)


    # KMI30 Stats
    for kmi30_stats in soup.find_all(attrs={"data-name" : "KMI30"}):
        for kmi30_stat in kmi30_stats.find_all('div', {"class":"stats"}):
            for kmi30shrstat in kmi30_stat.find_all('div', {"class":"stats_item"}):
                for kmi30_st_lbl in kmi30shrstat.find_all('div', {"class":"stats_label"}):
                    kmi30_stat_lbl.append('kmi30_' + kmi30_st_lbl.text)
                    for  kmi30_st_val in kmi30shrstat.find_all('div', {"class":"stats_value"}):
                        kmi30_stat_value.append(kmi30_st_val.text)

    # for odl_stats in soup.find_all(attrs={"data-name" : "ODL"}):
    #         for odl_stat in odl_stats.find_all('div', {"class":"stats"}):
    #             for odlstat in odl_stat.find_all('div', {"class":"stats_item"}):
    #                 for odl_st_lbl in odlstat.find_all('div', {"class":"stats_label"}):
    #                     odl_stat_lbl.append('odl_' + odl_st_lbl.text)
    #                 for odl_st_val in odlstat.find_all('div', {"class":"stats_value"}):
    #                     odl_stat_value.append(odl_st_val.text)                 

    del stat_lbl[-2:]
    del kse30_stat_lbl[-2:]
    del allshr_stat_lbl[-2:]
    del kmi30_stat_lbl[-2:]

    kse100_Stat = {stat_lbl[i]: stat_value[i] for i in range(len(stat_lbl))}
    kse100_Stat['kse100_Volume'] = re.sub("[^\d\.]", "", kse100_Stat['kse100_Volume'])
    allshr_Stat = {allshr_stat_lbl[i]: allshr_stat_value[i] for i in range(len(allshr_stat_lbl))}
    allshr_Stat['allshr_Volume'] = re.sub("[^\d\.]", "", allshr_Stat['allshr_Volume'])
    kse30_Stat = {kse30_stat_lbl[i]: kse30_stat_value[i] for i in range(len(kse30_stat_lbl))}
    kse30_Stat['kse30_Volume'] = re.sub("[^\d\.]", "", kse30_Stat['kse30_Volume'])
    kmi30_Stat = {kmi30_stat_lbl[i]: kmi30_stat_value[i] for i in range(len(kmi30_stat_lbl))}
    kmi30_Stat['kmi30_Volume'] = re.sub("[^\d\.]", "", kmi30_Stat['kmi30_Volume'])


    #odl_Stat = {odl_stat_lbl[i]: odl_stat_value[i] for i in range(len(odl_stat_lbl))}
    #odl_Stat['odl_Volume'] = re.sub("[^\d\.]", "", odl_Stat['odl_Volume'])
    kse100_df = pd.DataFrame(kse100_Stat, index=[0])
    allshr_df = pd.DataFrame(allshr_Stat, index=[0])
    kse30_df = pd.DataFrame(kse30_Stat, index=[0])

    kmi30_df = pd.DataFrame(kmi30_Stat, index=[0])
    #kse30_df.columns=['kse30_df_Open', 'kse30_High', 'kse30_Low', 'kse30_Volume']
    #kse100_df.columns=['kse100_open', 'kse100_high', 'kse100_low', 'kse100_volume']
    #kse100_df1 = pd.merge(kse100_df,demo_pd)
    #f_final = pd.concat(kse100_df1, join='outer', axis=1)

    dfs=[demo_pd,kse100_df,demo_pd1,allshr_df,demo_pd2,kse30_df,demo_pd3,kmi30_df]
    df_final=pd.concat(dfs,join='outer',axis=1)
    df_final['date']=date.today()
    df_final['kse100_quote_date'] = pd.to_datetime(df_final["kse100_quote_date"])
    df_final['allshr_quote_date'] = pd.to_datetime(df_final["allshr_quote_date"])
    df_final['kse30_quote_date'] = pd.to_datetime(df_final["kse30_quote_date"])
    df_final['kmi30_quote_date'] = pd.to_datetime(df_final["kmi30_quote_date"])

    df_final['kse100_High'] = df_final['kse100_High'].str.replace(',','')
    df_final['allshr_High'] = df_final['allshr_High'].str.replace(',','')
    df_final['kse30_High'] = df_final['kse30_High'].str.replace(',','')
    df_final['kmi30_High'] = df_final['kmi30_High'].str.replace(',','')
    
    df_final['kse100_Low'] = df_final['kse100_Low'].str.replace(',','')
    df_final['allshr_Low'] = df_final['allshr_Low'].str.replace(',','')
    df_final['kse30_Low'] = df_final['kse30_Low'].str.replace(',','')
    df_final['kmi30_Low'] = df_final['kmi30_Low'].str.replace(',','')
  
    df_final['kse100_Volume'] = df_final['kse100_Volume'].str.replace(',','')
    df_final['allshr_Volume'] = df_final['allshr_Volume'].str.replace(',','')
    df_final['kse30_Volume'] = df_final['kse30_Volume'].str.replace(',','')
    df_final['kmi30_Volume'] = df_final['kmi30_Volume'].str.replace(',','')

    df_final['kse100_Previous Close'] = df_final['kse100_Previous Close'].str.replace(',','')
    df_final['allshr_Previous Close'] = df_final['allshr_Previous Close'].str.replace(',','')
    df_final['kse30_Previous Close'] = df_final['kse30_Previous Close'].str.replace(',','')
    df_final['kmi30_Previous Close'] = df_final['kmi30_Previous Close'].str.replace(',','')


    
    df_final['kse100_1-Year Change']=df_final['kse100_1-Year Change'].str.replace('%','')
    df_final['kse100_YTD Change']=df_final['kse100_YTD Change'].str.replace('%','')
    df_final['allshr_1-Year Change']=df_final['allshr_1-Year Change'].str.replace('%','')
    df_final['allshr_YTD Change']=df_final['allshr_YTD Change'].str.replace('%','')
    df_final['kse30_1-Year Change']=df_final['kse30_1-Year Change'].str.replace('%','')
    df_final['kse30_YTD Change']=df_final['kse30_YTD Change'].str.replace('%','')
    df_final['kmi30_1-Year Change']=df_final['kmi30_1-Year Change'].str.replace('%','')
    df_final['kmi30_YTD Change']=df_final['kmi30_YTD Change'].str.replace('%','')
    df_final.rename(columns = {'kse100_1-Year Change':'kse100_1_Year_change', 'allshr_1-Year Change':'allshr_1_Year_change','kse30_1-Year Change':'kse30_1_Year_change', 'kmi30_1-Year Change':'kmi30_1_Year_change'}, inplace = True)
    df_final.rename(columns = {'kse100_YTD Change':'kse100_YTD_change', 'allshr_YTD Change':'allshr_YTD_change','kse30_YTD Change':'kse30_YTD_change', 'kmi30_YTD Change':'kmi30_YTD_change'}, inplace = True)
    df_final.rename(columns = {'kse100_Previous Close':'kse100_previous_close', 'allshr_Previous Close':'allshr_previous_close','kse30_Previous Close':'kse30_previous_close', 'kmi30_Previous Close':'kmi30_previous_close'}, inplace = True)
    
    
    if len(df_final) > 0:
        df_columns = list(df_final)    
        columns = ",".join(df_columns)

    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {}".format('"DW"."IBL_PSX_INDICES"',columns,values)
    pg.extras.execute_batch(cursor, insert_stmt, df_final.values)
    conn.commit()
    
    update_sql_kse100 = """ UPDATE "DW"."IBL_PSX_INDICES"  set kse100_volume_difference  = v_table_name.rn  FROM  
    (
        SELECT kse100_volume - LAG(kse100_volume)    OVER ( partition by  kse100_company_name,date(created_at) ORDER BY created_at ) AS rn, kse100_company_name,created_at
        FROM "DW"."IBL_PSX_INDICES" where kse100_company_name = 'KSE100'
    ) AS v_table_name
    WHERE "DW"."IBL_PSX_INDICES".kse100_company_name = v_table_name.kse100_company_name and "DW"."IBL_PSX_INDICES".created_at = v_table_name.created_at """
   
    update_sql_allshr = """ UPDATE "DW"."IBL_PSX_INDICES"  set allshr_volume_difference  = v_table_name.rn  FROM  
    (
        SELECT allshr_volume - LAG(allshr_volume)    OVER ( partition by  allshr_company_name,date(created_at) ORDER BY created_at ) AS rn, allshr_company_name,created_at
        FROM "DW"."IBL_PSX_INDICES" where allshr_company_name = 'ALLSHR'
    ) AS v_table_name
    WHERE "DW"."IBL_PSX_INDICES".allshr_company_name = v_table_name.allshr_company_name and "DW"."IBL_PSX_INDICES".created_at = v_table_name.created_at """

    update_sql_kse30 = """ UPDATE "DW"."IBL_PSX_INDICES"  set kse30_volume_difference  = v_table_name.rn  FROM  
    (
        SELECT kse30_volume - LAG(kse30_volume)    OVER ( partition by  kse30_company_name,date(created_at) ORDER BY created_at ) AS rn, kse30_company_name,created_at
        FROM "DW"."IBL_PSX_INDICES" where kse30_company_name = 'KSE30'
    ) AS v_table_name
    WHERE "DW"."IBL_PSX_INDICES".kse30_company_name = v_table_name.kse30_company_name and "DW"."IBL_PSX_INDICES".created_at = v_table_name.created_at """

    update_sql_kmi30 = """ UPDATE "DW"."IBL_PSX_INDICES"  set kmi30_volume_difference  = v_table_name.rn  FROM  
    (
        SELECT kmi30_volume - LAG(kmi30_volume)    OVER ( partition by  kmi30_company_name,date(created_at) ORDER BY created_at ) AS rn, kmi30_company_name,created_at
        FROM "DW"."IBL_PSX_INDICES" where kmi30_company_name = 'KMI30'
    ) AS v_table_name
    WHERE "DW"."IBL_PSX_INDICES".kmi30_company_name = v_table_name.kmi30_company_name and "DW"."IBL_PSX_INDICES".created_at = v_table_name.created_at """

    cursor.execute(update_sql_kse100)
    cursor.execute(update_sql_allshr)
    cursor.execute(update_sql_kse30)
    cursor.execute(update_sql_kmi30)
    conn.commit()
    cursor.close()

def insert_PSX_INDICES_data_To_GCS():
    df = pd.read_sql('select *  from "DW"."IBL_PSX_INDICES" order by created_at desc limit 1', conn);
    df_indices = pd.DataFrame(data=df)
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    filename =f'ibl_psx_indices_{curr_date}'

    bucket.blob(f'staging/psx/indices/{filename}.csv').upload_from_string(df_indices.to_csv(index=False), 'text/csv')
    conn.close()


IBL_PSX_INDICES = DAG(
    dag_id='IBL_PSX_INDICES',
    default_args=default_args,
    schedule_interval='0 04-11 * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='IBL_PSX_INDICES',
)


t0 = PythonOperator(
        task_id="extract_psx_indices_data_scrap_to_postgres",
        python_callable=extract_psx_indices_data_scrap_to_postgres,
        dag=IBL_PSX_INDICES
)


t1 =  PythonOperator(
    task_id="psx_indices_data_to_GCS",
    python_callable=insert_PSX_INDICES_data_To_GCS,
    dag=IBL_PSX_INDICES
)

t2 = GCSToBigQueryOperator(    
    task_id='psx_indices_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/psx/indices/ibl_psx_indices_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_PSX_INDICES',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'kse100_company_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'kse100_rate', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse100_change_value', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse100_change_percentage', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse100_quote_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'kse100_high', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse100_low', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse100_volume', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse100_1_year_change', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse100_ytd_change', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse100_previous_close', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_company_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'allshr_rate', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_change_value', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_change_percentage', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_quote_date', 'type': 'Timestamp', 'mode': 'NULLABLE'},
    {'name': 'allshr_high', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_low', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_volume', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_1_year_change', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_ytd_change', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_previous_close', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_company_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'kse30_rate', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_change_value', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_change_percentage', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_quote_date', 'type': 'Timestamp', 'mode': 'NULLABLE'},
    {'name': 'kse30_high', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_low', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_volume', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_1_year_change', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_ytd_change', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_previous_close', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_company_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'kmi30_rate', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_change_value', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_change_percentage', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_quote_date', 'type': 'Timestamp', 'mode': 'NULLABLE'},
    {'name': 'kmi30_high', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_low', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_volume', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_1_year_change', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_ytd_change', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_previous_close', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'Timestamp', 'mode': 'NULLABLE'},
    {'name': 'kse100_volume_difference', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'allshr_volume_difference', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kse30_volume_difference', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'kmi30_volume_difference', 'type': 'Numeric', 'mode': 'NULLABLE'}       
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=IBL_PSX_INDICES
)

t0 >> t1 >> t2
