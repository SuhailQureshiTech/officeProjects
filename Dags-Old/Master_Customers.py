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
    msg = "Customer Master DAG has executed successfully."
    subject = f"Customer Master DAG has completed"
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

def insert_MasterTable_Customers_To_GCS():
    # Initialize your connection
     conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
    # Read data from KNA1
     KNA1   = pd.read_sql(f'''SELECT
        KNA1.MANDT AS client_number,
        KNA1.KUNNR AS customer_number,
        KNA1.LAND1 AS customer_country_key,
        KNA1.NAME1 AS customer_name,
        KNA1.NAME2 AS customer_name2,
        KNA1.ORT01 AS customer_city,
        KNA1.PSTLZ AS customer_postal_code,
        KNA1.REGIO AS customer_region,
        KNA1.SORTL AS customer_sortfield,
        KNA1.STRAS AS customer_street_houseno,
        KNA1.TELF1 AS customer_telephoneno,
        KNA1.TELFX AS customer_fax,
        KNA1.XCPDK AS customer_onetimeaccount,
        KNA1.ADRNR AS customer_address,
        KNA1.MCOD1 AS customer_searchmatchcode1,
        KNA1.MCOD2 AS customer_searchmatchcode2,
        KNA1.MCOD3 AS customer_searchmatchcode3,
        KNA1.ANRED AS customer_title,
        KNA1.BAHNE AS customer_express_trainstation,
        KNA1.BAHNS AS customer_trainstation,
        KNA1.BBBNR AS customer_internationallocation_1,
        KNA1.BBSNR AS customer_internationallocation_2,
        KNA1.BEGRU AS customer_authorization,
        KNA1.BRSCH AS customer_industry_key,
        KNA1.BUBKZ AS customer_internationallocation_checkdigit,
        CAST(KNA1.ERDAT AS date) AS customer_createdon,
        KNA1.ERNAM AS person_created_the_object,
        KNA1.FAKSD AS customer_central_buildingblock,
        KNA1.FISKN AS customer_accountno_masterrecord_with_fiscaladdress,
        KNA1.KNAZK AS customer_workingtime_calendar,
        KNA1.KNRZA AS customer_accountno_alternateplayer,
        KNA1.KONZS AS customer_groupkey,
        KNA1.KTOKD AS customer_accountgroup,
        KNA1.KUKLA AS customer_classification,
        KNA1.LIFNR AS accountno_vendor,
        KNA1.LIFSD AS customer_central_deliveryblock,
        KNA1.LOEVM AS customer_central_deliveryflag,
        KNA1.NAME3 AS customer_name3,
        KNA1.NAME4 AS customer_name4,
        KNA1.ORT02 AS customer_district,
        KNA1.PFACH AS customer_pobox,
        KNA1.PSTL2 AS customer_pobox_postalcode, 
        KNA1.COUNC AS customer_countrycode,
        KNA1.CITYC AS customer_citycode, 
        KNA1.RPMKR AS regional_market,
        KNA1.SPERR AS customer_central_postingblock,
        KNA1.SPRAS AS customer_languagekey,
        KNA1.STCD1 AS customer_taxno1,
        KNA1.STCD2 AS customer_taxno2,
        KNA1.STKZU AS customer_liableforVAT,
        KNA1.TELBX AS customer_teleboxno,
        KNA1.TELF2 AS customer_telephoneno2, 
        KNA1.TELTX AS customer_teletexno,
        KNA1.TELX1 AS customer_telexno, 
        KNA1.VBUND AS customer_companyid_tradingpartner,
        KNA1.STCEG AS VAT_registrationno,
        KNA1.DEAR1 AS competitor,
        KNA1.DEAR2 AS sales_partner,
        KNA1.DEAR3 AS sales_prospect,
        KNA1.DEAR4 AS customer_type4,
        KNA1.DEAR5 AS default_soldto_partyid,
        KNA1.GFORM AS legal_status,
        KNA1.BRAN1 AS industry_code1,
        KNA1.BRAN2 AS industry_code2,
        KNA1.BRAN3 AS industry_code3,
        KNA1.BRAN4 AS industry_code4,
        KNA1.BRAN5 AS industry_code5,
        KNA1.EKONT AS initial_contact,
        KNA1.UMSAT AS annual_sales,
        KNA1.UMJAH AS salesgiven_year,
        KNA1.UWAER AS salesfigure_currency, 
        KNA1.JMZAH AS no_of_employees_year,
        KNA1.JMJAH AS no_of_employees_yearly_given, 
        KNA1.KATR1 AS customer_attribute1,
        KNA1.KATR2 AS customer_attribute2,
        KNA1.KATR3 AS customer_attribute3,
        KNA1.KATR4 AS customer_attribute4,
        KNA1.KATR5 AS customer_attribute5,
        KNA1.KATR6 AS customer_attribute6,
        KNA1.KATR7 AS customer_attribute7,
        KNA1.KATR8 AS customer_attribute8,
        KNA1.KATR9 AS customer_attribute9,
        KNA1.KATR10 AS customer_attribute10,
        KNA1.STKZN AS natural_person,
        KNA1.TXJCD AS customer_tax_jurisdiction,
        KNA1.PERIV AS fiscal_year_variant,
        KNA1.ABRVW AS usage_indicator,
        KNA1.INSPBYDEBI AS customer_inspection_carriedout,
        KNA1.INSPATDEBI AS customer_inspection_deliverynote_outbound,
        KNA1.KTOCD AS reference_account_group_onetimeaccount,
        KNA1.PFORT AS customer_pobox_city,
        KNA1.WERKS AS plant,
        KNA1.SPERZ AS customer_paymentblock, 
        KNA1.CIVVE AS non_militaryuse_id,
        KNA1.MILVE AS militaryuse_id,
        KNA1.KDKG1 AS customer_conditiongroup1,
        KNA1.KDKG2 AS customer_conditiongroup2,
        KNA1.KDKG3 AS customer_conditiongroup3,
        KNA1.KDKG4 AS customer_conditiongroup4,
        KNA1.KDKG5 AS customer_conditiongroup5,
        KNA1.FITYP AS tax_type,
        KNA1.STCDT AS taxno_type,
        KNA1.STCD3 AS taxno_3,
        KNA1.STCD4 AS taxno_4,
        KNA1.STCD5 AS taxno_5,
        KNA1.XICMS AS is_customer_ICMSexempt,
        KNA1.XXIPI AS is_customer_IPIexempt,
        KNA1.CFOPC AS customer_CFOP_category,
        KNA1.TXLW1 AS tax_law_ICMS,
        KNA1.TXLW2 AS tax_law_IPI,
        KNA1.CASSD AS customer_central_salesblock,
        KNA1.KNURL AS uniform_resource_locator,
        KNA1.J_1KFREPRE AS representative_name,
        KNA1.J_1KFTBUS AS  business_type,
        KNA1.J_1KFTIND AS  industry_type,
        KNA1.CONFS AS status_of_authorizationchange,
        CAST(KNA1.UPDAT AS date) AS confirmedchanges_date,  
        KNA1.UPTIM AS confirmedchanges_time,  
        KNA1.NODEL AS customer_central_deletionblock, 
        KNA1.DEAR6 AS consumer,
        KNA1.SUFRAMA AS suframa_code,
        KNA1.RG AS RG_no,
        KNA1."EXP" AS issued_by,
        KNA1.UF AS state,
        CAST(KNA1.RGDATE AS DATE) AS RG_issue_date,
        KNA1.RIC AS RIC_no,
        KNA1.RNE AS foreign_national_registration,
        CAST(KNA1.RNEDATE AS DATE) AS RNE_issue_date,
        KNA1.CNAE AS CNAE,
        KNA1.LEGALNAT AS legal_nature, 
        KNA1.CRTN AS CRT_no,
        KNA1.ICMSTAXPAY AS ICMS_taxpayer,
        KNA1.INDTYP AS industry_main_type, 
        KNA1.TDT AS tax_declaration_type,
        KNA1.COMSIZE AS company_size,
        KNA1.ALC AS agency_location_code,
        KNA1.PMT_OFFICE AS payment_office,
        KNA1.PSOFG AS processor_group,
        KNA1.PSON1 AS name1,
        KNA1.PSON2 AS name2,
        KNA1.PSON3 AS name3,
        KNA1.PSOVN AS first_name,
        KNA1.PSOTL AS title,
        KNA1.ZZDEALER AS dealer,
        KNA1.ZZLICENSE AS license,
        CAST(KNA1.ZZLDATEFROM AS DATE) AS ZZL_datefrom,
        CAST(KNA1.ZZLDATETO AS DATE) AS ZZL_dateto,
        CAST(KNA1.ZZDDATEFROM  AS DATE) ZZ_datefrom,
        CAST(KNA1.ZZDDATETO  AS DATE) ZZ_dateto,
        KNA1.ZZDRUGLICENSENO AS license_no, 
        KNA1.ZZEXPIRYDATE AS  expiry_date
        FROM
        SAPABAP1.KNA1''', conn);
     df_KNA1 = pd.DataFrame(data=KNA1)

     # Read data from KNB1
     KNB1   = pd.read_sql(f'''SELECT
        KNB1.MANDT AS client_number,
        KNB1.KUNNR AS customer_number,
        KNB1.BUKRS AS company_code,
        KNB1.PERNR AS personnel_number,
        KNB1.SPERR AS postingblock_company_code, 
        KNB1.LOEVM AS company_code_level_deletionflag, 
        KNB1.BUSAB AS accounting_clerk,
        KNB1.AKONT AS reconcilation_account_in_GL,
        KNB1.BEGRU AS authorization_group,
        KNB1.KNRZE AS headoffice_account_no,
        KNB1.KNRZB AS alternateplayer_account_no, 
        KNB1.ZAMIM AS customer_payment_notice_cleared_items,
        KNB1.ZAMIV AS sales_dept_payment_notice,
        KNB1.ZAMIR AS legal_dept_payment_notice,
        KNB1.ZAMIB AS accounting_dept_payment_notice,
        KNB1.ZAMIO AS customer_payment_notice_wo_cleared_items,
        KNB1.ZWELS AS  payment_methods_to_consider,
        KNB1.XVERR AS clearing_between_customer_and_vendor,
        KNB1.ZAHLS AS payment_block_key,
        KNB1.ZTERM AS payment_terms_key,
        KNB1.VZSKZ AS interest_calculation_indicator,
        CAST(KNB1.ZINDT AS date) AS keydate_last_interest_calculation, 
        KNB1.ZINRT AS interest_calculation_frequency_in_months, 
        KNB1.EIKTO AS accountno_at_customer,
        KNB1.ZSABE AS user_at_customer,
        KNB1.KVERM AS memo,
        KNB1.FDGRV AS planning_group,
        KNB1.VLIBB AS amount_insured,
        KNB1.VRSZL AS insurance_lead_month,
        KNB1.VRSPR AS deductible_percentage_rate,
        KNB1.VRSNR AS insurance_number,
        CAST(KNB1.VERDT AS date) AS insurance_validity_date,
        KNB1.PERKZ AS collective_invoice_variant,
        KNB1.XDEZV AS local_processing,
        KNB1.XAUSZ AS periodic_account_statements,
        KNB1.WEBTR AS exchange_limit_bill,
        KNB1.REMIT AS next_payee,
        CAST(KNB1.DATLZ AS date) AS last_insurance_calculation_run_date,
        KNB1.XZVER AS record_payment_history,
        KNB1.TOGRU AS tolerance_group_for_business_partner_GL,
        KNB1.KULTG AS probable_time_until_check_is_paid, 
        KNB1.HBKID AS house_bank_shortkey,
        KNB1.XPORE AS pay_all_items_seperately,
        KNB1.ALTKN AS previous_master_record_no,
        KNB1.ZGRUP AS payment_grouping_key,
        KNB1.URLID AS known_leave_shortkey,
        KNB1.MGRUP AS dunning_notice_grouping_shortkey,
        KNB1.LOCKB AS lockbox_shortkey,
        KNB1.UZAWE AS payment_method_supplement,
        KNB1.EKVBD AS buying_group_accountno,
        KNB1.XEDIP AS send_payment_advices_EDI,
        KNB1.FRGRP AS release_approval_group,
        KNB1.VRSDG AS reason_code_conversion_version,
        KNB1.INTAD AS internet_address_partner_company_clerk,
        KNB1.XKNZB AS alternate_payer_using_account_no, 
        KNB1.GUZTE AS credit_memos_payment_key,
        KNB1.GRICD AS activity_code_gross_income_tax,
        KNB1.GRIDT AS employment_tax_distribution_type,
        KNB1.WBRSL AS value_adjustment_key,
        KNB1.CONFS AS authorization_change_status,
        CAST(KNB1.UPDAT AS date) AS  confirmed_changes_update_date,
        KNB1.UPTIM AS  confirmed_changes_update_time,
        KNB1.CESSION_KZ AS accounts_receivable_pledging_indicator,
        KNB1.AVSND AS send_payment_advices_XML,
        KNB1.AD_HASH AS email_address_for_Avis,
        KNB1.QLAND AS withholding_tax_country_key,  
        KNB1.GMVKZD AS customer_is_in_execution
        FROM
        SAPABAP1.KNB1''', conn);
     df_KNB1 = pd.DataFrame(data=KNB1)         
     

     #df_KNA1['transfer_date'] = datetime.today()         
     #df_KNB1['transfer_date'] = datetime.today()           
     #df_KNVV['transfer_date'] = datetime.today()          
     file_path = r'/home/admin2/airflow/dag/Google_cloud'
     GCS_PROJECT = 'data-light-house-prod'
     GCS_BUCKET = 'ibloper'
     storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
     client = storage.Client(project=GCS_PROJECT)
     bucket = client.get_bucket(GCS_BUCKET)
     
     df_join1 = df_KNA1.merge(df_KNB1, how='left', left_on=['CLIENT_NUMBER','CUSTOMER_NUMBER'], right_on=['CLIENT_NUMBER','CUSTOMER_NUMBER'])
     
     df_join1 = df_join1.query("CUSTOMER_LANGUAGEKEY == 'E'")
          
     #df_join2 = df_join1.merge(df_KNVV, how='left', left_on=['CLIENT_NUMBER','CUSTOMER_NUMBER'], right_on=['CLIENT_NUMBER','CUSTOMER_NUMBER'])     
     #print(df_join2.shape)
     #df_join2 = df_join2.query("SALES_ORGANIZATION in ('6100','6300')")
     #print(df_join2.shape)
    
     
     #df_join2.drop(['MATERIAL_GROUP_1', 'TVM1T_LANGUAGE_KEY'], inplace=True, axis = 1 )
     df_join1 = df_join1.loc[(df_join1['CUSTOMER_CREATEDON'] == today) | (df_join1['CONFIRMEDCHANGES_DATE'] == today)]

     #df_join1 = df_join1.query("CUSTOMER_CREATEDON = '"+today+"' or CONFIRMEDCHANGES_DATE = '"+today+"'")
     #df_join1 = df_join1.query("CUSTOMER_CREATEDON >= '"+str(today)+"'")
     print(df_join1['CUSTOMER_CREATEDON'])
     df_join1['date'] = date.today()
     df_join1['created_at'] = datetime.today()
           
     filename_KNA1 =f'ibl_KNA1_{curr_date}'
     filename_KNB1 =f'ibl_KNB1_{curr_date}'
     #filename_KNVV =f'ibl_KNVV_{curr_date}'     
     filename_customers = f'ibl_CUSTOMERS_{curr_date}'

     bucket.blob(f'staging/master_tables/Customer/{filename_KNA1}.csv').upload_from_string(df_KNA1.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Customer/{filename_KNB1}.csv').upload_from_string(df_KNB1.to_csv(index=False), 'text/csv')
     #bucket.blob(f'staging/master_tables/Customer/{filename_KNVV}.csv').upload_from_string(df_KNVV.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Customer/{filename_customers}.csv').upload_from_string(df_join1.to_csv(index=False), 'text/csv')
     
     df_join1.drop(['CUSTOMER_POSTAL_CODE', 'CUSTOMER_FAX','CUSTOMER_ONETIMEACCOUNT','CUSTOMER_EXPRESS_TRAINSTATION','CUSTOMER_TRAINSTATION','CUSTOMER_CENTRAL_BUILDINGBLOCK','CUSTOMER_ACCOUNTNO_MASTERRECORD_WITH_FISCALADDRESS','CUSTOMER_WORKINGTIME_CALENDAR','CUSTOMER_ACCOUNTNO_ALTERNATEPLAYER','CUSTOMER_GROUPKEY','CUSTOMER_CENTRAL_DELIVERYBLOCK','CUSTOMER_CENTRAL_DELIVERYFLAG','CUSTOMER_POBOX','CUSTOMER_POBOX_POSTALCODE','CUSTOMER_COUNTRYCODE','CUSTOMER_CENTRAL_POSTINGBLOCK','CUSTOMER_TELEBOXNO','CUSTOMER_TELETEXNO','CUSTOMER_TELEXNO','CUSTOMER_COMPANYID_TRADINGPARTNER','DEFAULT_SOLDTO_PARTYID','INDUSTRY_CODE1','INDUSTRY_CODE2','INDUSTRY_CODE3','INDUSTRY_CODE4','INDUSTRY_CODE5','ANNUAL_SALES','SALESGIVEN_YEAR','SALESFIGURE_CURRENCY','NO_OF_EMPLOYEES_YEAR','NO_OF_EMPLOYEES_YEARLY_GIVEN','CUSTOMER_ATTRIBUTE1','CUSTOMER_ATTRIBUTE2','CUSTOMER_ATTRIBUTE3','CUSTOMER_ATTRIBUTE4','CUSTOMER_ATTRIBUTE5','CUSTOMER_ATTRIBUTE6','CUSTOMER_ATTRIBUTE7','CUSTOMER_ATTRIBUTE8','CUSTOMER_ATTRIBUTE9','CUSTOMER_ATTRIBUTE10','NATURAL_PERSON','FISCAL_YEAR_VARIANT','USAGE_INDICATOR','CUSTOMER_INSPECTION_CARRIEDOUT','CUSTOMER_INSPECTION_DELIVERYNOTE_OUTBOUND','REFERENCE_ACCOUNT_GROUP_ONETIMEACCOUNT','CUSTOMER_POBOX_CITY','NON_MILITARYUSE_ID','MILITARYUSE_ID','CUSTOMER_CONDITIONGROUP1','CUSTOMER_CONDITIONGROUP2','CUSTOMER_CONDITIONGROUP3','CUSTOMER_CONDITIONGROUP4','CUSTOMER_CONDITIONGROUP5','TAX_TYPE','TAXNO_TYPE','TAXNO_3','TAXNO_4','TAXNO_5','TAX_LAW_ICMS','TAX_LAW_IPI','CUSTOMER_CENTRAL_SALESBLOCK','UNIFORM_RESOURCE_LOCATOR','REPRESENTATIVE_NAME','BUSINESS_TYPE','INDUSTRY_TYPE','STATUS_OF_AUTHORIZATIONCHANGE','CUSTOMER_CENTRAL_DELETIONBLOCK','CONSUMER','SUFRAMA_CODE','INDUSTRY_MAIN_TYPE','TAX_DECLARATION_TYPE','COMPANY_SIZE','AGENCY_LOCATION_CODE','PAYMENT_OFFICE','PROCESSOR_GROUP','POSTINGBLOCK_COMPANY_CODE','COMPANY_CODE_LEVEL_DELETIONFLAG','ACCOUNTING_CLERK','RECONCILATION_ACCOUNT_IN_GL','AUTHORIZATION_GROUP','HEADOFFICE_ACCOUNT_NO','ALTERNATEPLAYER_ACCOUNT_NO','CUSTOMER_PAYMENT_NOTICE_CLEARED_ITEMS','SALES_DEPT_PAYMENT_NOTICE','LEGAL_DEPT_PAYMENT_NOTICE','ACCOUNTING_DEPT_PAYMENT_NOTICE','CUSTOMER_PAYMENT_NOTICE_WO_CLEARED_ITEMS','PAYMENT_METHODS_TO_CONSIDER','CLEARING_BETWEEN_CUSTOMER_AND_VENDOR','PAYMENT_BLOCK_KEY','PAYMENT_TERMS_KEY','INTEREST_CALCULATION_INDICATOR','KEYDATE_LAST_INTEREST_CALCULATION','INTEREST_CALCULATION_FREQUENCY_IN_MONTHS','ACCOUNTNO_AT_CUSTOMER','USER_AT_CUSTOMER','PLANNING_GROUP','AMOUNT_INSURED','INSURANCE_LEAD_MONTH','DEDUCTIBLE_PERCENTAGE_RATE','INSURANCE_NUMBER','INSURANCE_VALIDITY_DATE','COLLECTIVE_INVOICE_VARIANT','LOCAL_PROCESSING','PERIODIC_ACCOUNT_STATEMENTS','EXCHANGE_LIMIT_BILL','NEXT_PAYEE','LAST_INSURANCE_CALCULATION_RUN_DATE','RECORD_PAYMENT_HISTORY','TOLERANCE_GROUP_FOR_BUSINESS_PARTNER_GL','PROBABLE_TIME_UNTIL_CHECK_IS_PAID','HOUSE_BANK_SHORTKEY','PAY_ALL_ITEMS_SEPERATELY','PREVIOUS_MASTER_RECORD_NO','PAYMENT_GROUPING_KEY','KNOWN_LEAVE_SHORTKEY','DUNNING_NOTICE_GROUPING_SHORTKEY','LOCKBOX_SHORTKEY','PAYMENT_METHOD_SUPPLEMENT','BUYING_GROUP_ACCOUNTNO','SEND_PAYMENT_ADVICES_EDI','RELEASE_APPROVAL_GROUP','REASON_CODE_CONVERSION_VERSION','INTERNET_ADDRESS_PARTNER_COMPANY_CLERK','ALTERNATE_PAYER_USING_ACCOUNT_NO','CREDIT_MEMOS_PAYMENT_KEY','ACTIVITY_CODE_GROSS_INCOME_TAX','EMPLOYMENT_TAX_DISTRIBUTION_TYPE','VALUE_ADJUSTMENT_KEY','AUTHORIZATION_CHANGE_STATUS','ACCOUNTS_RECEIVABLE_PLEDGING_INDICATOR','SEND_PAYMENT_ADVICES_XML','EMAIL_ADDRESS_FOR_AVIS','WITHHOLDING_TAX_COUNTRY_KEY','CUSTOMER_IS_IN_EXECUTION'], inplace=True, axis = 1)	
     filename_customers_t = f'ibl_CUSTOMERS_transformed_{curr_date}'
     bucket.blob(f'staging/master_tables/Customer/{filename_customers_t}.csv').upload_from_string(df_join1.to_csv(index=False), 'text/csv')	
     conn.close()

IBL_CUSTOMERS_SAP = DAG(
    dag_id='IBL_CUSTOMERS_SAP',
    default_args=default_args,
    schedule_interval='0 18 * * *',
    catchup=False,
    #schedule_interval='*/30 * * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='IBL_CUSTOMERS_SAP_TO_BQ',
)
 
t0 = PythonOperator(
    task_id="IBL_Customers_SAP_To_GCS",
    python_callable=insert_MasterTable_Customers_To_GCS,
    dag=IBL_CUSTOMERS_SAP
)

t1=     GCSToBigQueryOperator(    
    task_id='IBL_Customers_SAP_To_BQ',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Customer/ibl_CUSTOMERS_transformed_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_CUSTOMER',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'client_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_country_key', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_name2', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_city', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_region', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_sortfield', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_street_houseno', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_telephoneno', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_address', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_searchmatchcode1', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_searchmatchcode2', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_searchmatchcode3', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_title', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_internationallocation_1', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_internationallocation_2', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_authorization', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_industry_key', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_internationallocation_checkdigit', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_createdon', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'person_created_the_object', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_accountgroup', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_classification', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'accountno_vendor', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_name3', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_name4', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_district', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_citycode', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'regional_market', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_languagekey', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_taxno1', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_taxno2', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_liableforVAT', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_telephoneno2', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'VAT_registrationno', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'competitor', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_partner', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_prospect', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_type4', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'legal_status', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'initial_contact', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_tax_jurisdiction', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'plant', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_paymentblock', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'is_customer_ICMSexempt', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'is_customer_IPIexempt', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'customer_CFOP_category', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'confirmedchanges_date', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'confirmedchanges_time', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'RG_no', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'issued_by', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'RG_issue_date', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'RIC_no', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'foreign_national_registration', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'RNE_issue_date', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'CNAE', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'legal_nature', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'CRT_no', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'ICMS_taxpayer', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'name1', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'name2', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'name3', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'dealer', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'license', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'ZZL_datefrom', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'ZZL_dateto', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'ZZ_datefrom', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'ZZ_dateto', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'license_no', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'expiry_date', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'personnel_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'memo', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'confirmed_changes_update_date', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'confirmed_changes_update_time', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}        
     ],
     write_disposition='WRITE_APPEND',
     skip_leading_rows = 1,
     dag=IBL_CUSTOMERS_SAP
  )

t0 >> t1


