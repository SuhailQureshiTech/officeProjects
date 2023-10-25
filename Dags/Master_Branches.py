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
import pyodbc
from hdbcli import dbapi
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import numpy as np

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Branches Master DAG has executed successfully."
    subject = f"Branches Master DAG has completed"
    send_email(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)

default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 5, 26),
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
curr_date = today.strftime("%d-%b-%Y")

def insert_MasterTable_Branches_To_GCS():
    # Initialize your connection
     conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
    # Read data from T001L
     T001L   = pd.read_sql(f'''SELECT
        T001L.MANDT AS storage_location_client,
        T001L.WERKS AS storage_location_plant,
        T001L.LGORT AS branch_code,
        T001L.LGOBE AS branch_description,
        T001L.SPART AS division,
        T001L.XLONG AS negative_stocks_allowed_in_branch,
        T001L.XBUFX AS inventory_balance_allowed_in_branch,
        T001L.DISKZ AS MRP_indicator_branch,
        T001L.XBLGO AS branch_authorization_for_goods_movement_active,
        T001L.XRESS AS branch_resource_allocated,
        T001L.XHUPF AS handling_unit_requirement,
        T001L.PARLG AS handling_unit_branch_partner,
        T001L.VKORG AS sales_organization,
        T001L.VTWEG AS distribution_channel,
        T001L.VSTEL AS shipping_receiving_point,
        T001L.LIFNR AS vendor_account_no,
        T001L.KUNNR AS customer_account_no,
        T001L.MESBS AS MES_business_system,
        T001L.MESST AS type_of_inventory_management_for_production_branch,
        T001L.OIH_LICNO AS license_number_untaxed_stock,
        T001L.OIG_ITRFL AS TD_intransit_flag,
        T001L.OIB_TNKASSIGN AS SILO_management_task_assignment_indicator
        FROM
        SAPABAP1.T001L''', conn);
     df_T001L = pd.DataFrame(data=T001L)

     # Read data from T001W
     T001W   = pd.read_sql(f'''SELECT
        T001W.MANDT AS branch_client,
        T001W.WERKS AS branch_plant,
        T001W.NAME1 AS branch_name,
        T001W.BWKEY AS branch_valuation_area,
        T001W.KUNNR AS customer_number_of_plant, 
        T001W.LIFNR AS vendor_number_of_plant,
        T001W.FABKL AS factory_calendar_key, 
        T001W.NAME2 AS branch_name2,
        T001W.STRAS AS branch_house_no_and_street,
        T001W.PFACH AS branch_po_box, 
        T001W.PSTLZ AS branch_postal_code,
        T001W.ORT01 AS branch_city,
        T001W.EKORG AS purchasing_organization,
        T001W.VKORG AS branch_sales_organization,
        T001W.CHAZV AS batch_status_management_active,
        T001W.KKOWK AS plant_level_conditions,
        T001W.KORDB AS source_list_requirement,
        T001W.BEDPL AS activating_requirements_planning, 
        T001W.LAND1 AS branch_country_key,
        T001W.REGIO AS branch_region,
        T001W.COUNC AS branch_country_code,
        T001W.CITYC AS branch_city_code,
        T001W.ADRNR AS branch_address,
        T001W.IWERK AS maintenance_planning_plant,
        T001W.TXJCD AS tax_jurisdiction,
        T001W.VTWEG AS branch_distribution_channel,
        T001W.SPART AS branch_division,
        T001W.SPRAS AS branch_language_key,
        T001W.WKSOP AS branch_SOP_plant,
        T001W.AWSLS AS branch_variance_key,
        T001W.VLFKZ AS plant_category,
        T001W.BZIRK AS sales_district,
        T001W.ZONE1 AS supply_region,
        T001W.TAXIW AS tax_indicator_plant_purchasing,
        T001W.LET01 AS no_of_days_first_reminder,
        T001W.LET02 AS no_of_days_second_reminder,
        T001W.LET03 AS no_of_days_third_reminder,
        T001W.TXNAM_MA1 AS text_name_1st_dunning_vendor_declarations,
        T001W.TXNAM_MA2 AS text_name_2nd_dunning_vendor_declarations,
        T001W.TXNAM_MA3 AS text_name_3rd_dunning_vendor_declarations,
        T001W.BETOL AS no_of_days_PO_tolerance,
        T001W.J_1BBRANCH AS business_place,
        T001W.FPRFW AS distribution_profile_plant_level,
        T001W.NODETYPE AS supply_chain_type,
        T001W.NSCHEMA AS name_formation_structure,
        T001W.VSTEL AS branch_shipping_receiving_point,
        T001W.OILIVAL AS exchange_valuation_indicator,
        T001W.OIHVTYPE AS vendor_type,
        T001W.OIHCREDIPI AS IPI_credit_allowed,
        T001W.STORETYPE AS store_category,
        T001W.DEP_STORE superior_department_store
        FROM
        SAPABAP1.T001W''', conn);
     df_T001W = pd.DataFrame(data=T001W)

     # Read data from TVKBT 
     TVKBT    = pd.read_sql(f'''SELECT
        TVKBT.MANDT AS sales_office_client,
        TVKBT.SPRAS AS sales_office_language_key,
        TVKBT.VKBUR AS sales_office_branch_code,
        TVKBT.BEZEI AS sales_office_branch_description
        FROM
        SAPABAP1.TVKBT ''', conn);
     df_TVKBT  = pd.DataFrame(data=TVKBT )    
     

     #df_KNA1['transfer_date'] = datetime.today()         
     #df_KNB1['transfer_date'] = datetime.today()           
     #df_KNVV['transfer_date'] = datetime.today()          
     file_path = r'/home/admin2/airflow/dag/Google_cloud'
     GCS_PROJECT = 'data-light-house-prod'
     GCS_BUCKET = 'ibloper'
     storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
     client = storage.Client(project=GCS_PROJECT)
     bucket = client.get_bucket(GCS_BUCKET)
     
     df_join1 = df_T001L.merge(df_T001W, how='inner', left_on=['STORAGE_LOCATION_CLIENT','STORAGE_LOCATION_PLANT'], right_on=['BRANCH_CLIENT','BRANCH_PLANT'])
     #print(df_join1.shape)
     df_join1 = df_join1.query("STORAGE_LOCATION_PLANT in ('6100','6300')")
     #print(df_join1.shape)     
     df_join2 = df_join1.merge(df_TVKBT, how='left', left_on=['STORAGE_LOCATION_CLIENT','BRANCH_CODE'], right_on=['SALES_OFFICE_CLIENT','SALES_OFFICE_BRANCH_CODE'])     
     #print(df_join2.shape)
     df_join2 = df_join2.query("BRANCH_LANGUAGE_KEY ==  'E'")
     #print(df_join2.shape)
    
     
     #df_join2.drop(['MATERIAL_GROUP_1', 'TVM1T_LANGUAGE_KEY'], inplace=True, axis = 1 )
     
     
     #df_join2 = df_join2.query("CUSTOMER_CREATEDON == '"+str(today)+"' or CONFIRMEDCHANGES_DATE == '"+str(today)+"'")
     df_join2['date'] = date.today()
     df_join2['created_at'] = datetime.today()
      
     filename_T001L =f'ibl_T001L_{curr_date}'
     filename_T001W =f'ibl_T001W_{curr_date}'
     filename_TVKBT =f'ibl_TVKBT_{curr_date}'     
     filename_branches = f'ibl_BRANCHES_{curr_date}'

     bucket.blob(f'staging/master_tables/Branch/{filename_T001L}.csv').upload_from_string(T001L.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Branch/{filename_T001W}.csv').upload_from_string(df_T001W.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Branch/{filename_TVKBT}.csv').upload_from_string(df_TVKBT.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Branch/{filename_branches}.csv').upload_from_string(df_join2.to_csv(index=False), 'text/csv')

     df_join2.drop(['NEGATIVE_STOCKS_ALLOWED_IN_BRANCH', 'INVENTORY_BALANCE_ALLOWED_IN_BRANCH','MRP_INDICATOR_BRANCH','BRANCH_AUTHORIZATION_FOR_GOODS_MOVEMENT_ACTIVE','BRANCH_RESOURCE_ALLOCATED','HANDLING_UNIT_BRANCH_PARTNER','MES_BUSINESS_SYSTEM','TYPE_OF_INVENTORY_MANAGEMENT_FOR_PRODUCTION_BRANCH','TD_INTRANSIT_FLAG','SILO_MANAGEMENT_TASK_ASSIGNMENT_INDICATOR','BRANCH_PO_BOX','BRANCH_POSTAL_CODE','BATCH_STATUS_MANAGEMENT_ACTIVE','PLANT_LEVEL_CONDITIONS','SOURCE_LIST_REQUIREMENT','ACTIVATING_REQUIREMENTS_PLANNING','BRANCH_COUNTRY_CODE','MAINTENANCE_PLANNING_PLANT','TAX_JURISDICTION','BRANCH_SOP_PLANT','BRANCH_VARIANCE_KEY','TAX_INDICATOR_PLANT_PURCHASING','NO_OF_DAYS_FIRST_REMINDER','NO_OF_DAYS_SECOND_REMINDER','NO_OF_DAYS_THIRD_REMINDER','TEXT_NAME_1ST_DUNNING_VENDOR_DECLARATIONS','TEXT_NAME_2ND_DUNNING_VENDOR_DECLARATIONS','TEXT_NAME_3RD_DUNNING_VENDOR_DECLARATIONS','NO_OF_DAYS_PO_TOLERANCE','DISTRIBUTION_PROFILE_PLANT_LEVEL','NAME_FORMATION_STRUCTURE','EXCHANGE_VALUATION_INDICATOR'], inplace=True,axis = 1)	
     filename_branches_t = f'ibl_BRANCHES_transformed_{curr_date}'
     bucket.blob(f'staging/master_tables/Branch/{filename_branches_t}.csv').upload_from_string(df_join2.to_csv(index=False), 'text/csv')   
     
     conn.close()


IBL_BRANCHES_SAP = DAG(
    dag_id='IBL_BRANCHES_SAP',
    default_args=default_args,
    schedule_interval='0 18 * * *',
    catchup=False,
    #schedule_interval='*/30 * * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='IBL_BRANCHES_SAP_TO_BQ',
)
 
t0 = PythonOperator(
    task_id="IBL_BRANCHES_SAP_To_GCS",
    python_callable=insert_MasterTable_Branches_To_GCS,
    dag=IBL_BRANCHES_SAP
)


t1=     GCSToBigQueryOperator(    
    task_id='IBL_Branches_SAP_To_BQ',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Branch/ibl_BRANCHES_transformed_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_BRANCH',
    schema_fields=[
     # Define schema as per the csv placed in google cloud storage
    {'name': 'storage_location_client', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'storage_location_plant', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_code', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_description', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'division', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'handling_unit_requirement', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_organization', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'distribution_channel', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'shipping_receiving_point', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'vendor_account_no', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_account_no', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'license_number_untaxed_stock', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_client', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_plant', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_name', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_valuation_area', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_number_of_plant', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'vendor_number_of_plant', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'factory_calendar_key', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_name2', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_house_no_and_street', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_city', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'purchasing_organization', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_sales_organization', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_country_key', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_region', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_city_code', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_address', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_distribution_channel', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_division', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_language_key', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'plant_category', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_district', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'supply_region', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'business_place', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'supply_chain_type', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'branch_shipping_receiving_point', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'vendor_type', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'ipi_credit_allowed', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'store_category', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'superior_department_store', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_office_client', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_office_language_key', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_office_branch_code', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_office_branch_description', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}           
     ],
     write_disposition='WRITE_TRUNCATE',
     skip_leading_rows = 1,
     dag=IBL_BRANCHES_SAP
  )

t0 >> t1 

