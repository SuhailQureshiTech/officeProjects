import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
import psycopg2 as pg
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
from datetime import datetime, timedelta
import traceback
import urllib3
import xmltodict
from dateutil import parser
from datetime import datetime,date
import os
from airflow.utils.email import send_email_smtp
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "JAZZ SMS Log Focall has executed successfully."
    subject = f"JAZZ SMS Log Focall DAG has completed"
    send_email_smtp(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)

default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 8, 22),
        # 'end_date': datetime(),
        'email_on_failure': True,
        'email': ['shehzad.lalani@iblgrp.com'],
        'on_success_callback':success_function,
        #'on_success_callback': dag_success_alert,     
        #'on_failure_callback': failure_email_function,        
        #'email_on_retry': False,
        # If a task fails, retry it once after wait
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),        
        }


current_datetime = datetime.now() + timedelta(hours=5) 
current_datetime_10 = current_datetime - timedelta(minutes=600)
current_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
current_datetime_10 = current_datetime_10.strftime("%Y-%m-%d %H:%M:%S")

#current_datetime_10 = '2023-02-01 00:00:00'


smsTo =[]
smsFrom=[]
smsMessage=[]
smsDate=[]
smsA=[]
smsH=[]
smsV=[]

conn = pg.connect(host="192.168.130.51", port= '5432', database="focall", user="postgres", password="kamil0343")
cur = conn.cursor()

def extract_sms_data_to_postgres():
     print(current_datetime)
     print(current_datetime_10)

     print(current_datetime)
     print(current_datetime_10)

     url = "http://221.132.117.58:7700/receivesms_xml.html"
     payload = "<SMSRequest>\r\n    <Username>03028501421</Username>\r\n    <Password>searle123</Password>\r\n    <Shortcode>7005101</Shortcode>\r\n    <FromDate>"+current_datetime_10+"</FromDate>\r\n    <ToDate>"+current_datetime+"</ToDate>\r\n</SMSRequest>"
     headers = {
     'Content-Type': 'text/plain'
     }
     response = requests.request("GET", url, headers=headers, data=payload)
     data = xmltodict.parse(response.text)
     print(data)
     
     try:
         smsTo.append(data['SMSRsponse']['SMSInfo'].get('smsTo'))
         smsFrom.append(data['SMSRsponse']['SMSInfo'].get('smsFrom'))
         smsMessage.append(data['SMSRsponse']['SMSInfo'].get('smsMessage'))
         smsDate.append(data['SMSRsponse']['SMSInfo'].get('smsDate'))
         smsA.append(data['SMSRsponse']['SMSInfo'].get('smsMessage').partition(" A=")[2].partition(",")[0].strip('()'))
         smsH.append(data['SMSRsponse']['SMSInfo'].get('smsMessage').partition("H=")[2].partition(",")[0].partition(" ")[0].strip('()'))
         smsV.append(data['SMSRsponse']['SMSInfo'].get('smsMessage').partition("V=")[2].partition(",")[0].strip('()'))
        
     except:
         if "SMSInfo" in data['SMSRsponse']:
             if len(data['SMSRsponse']['SMSInfo'])>1:
                 for i in range(0, len(data['SMSRsponse']['SMSInfo'])):
                     smsTo.append(data['SMSRsponse']['SMSInfo'][i]['smsTo'])
                     smsFrom.append(data['SMSRsponse']['SMSInfo'][i]['smsFrom'])
                     smsMessage.append(data['SMSRsponse']['SMSInfo'][i]['smsMessage'])
                     smsDate.append(data['SMSRsponse']['SMSInfo'][i]['smsDate'])
                     smsA.append(data['SMSRsponse']['SMSInfo'][i]['smsMessage'].partition(" A=")[2].partition(",")[0].strip('()'))
                     smsH.append(data['SMSRsponse']['SMSInfo'][i]['smsMessage'].partition("H=")[2].partition(",")[0].partition(" ")[0].strip('()'))
                     smsV.append(data['SMSRsponse']['SMSInfo'][i]['smsMessage'].partition("V=")[2].partition(",")[0].strip('()'))
         else:
             print("No sms log found.")               
     
     for i in range(len(smsA)):
         if smsA[i] == 'RM' or smsA[i] == 'RS':
                 smsA[i] = ''

     df_smsH = pd.DataFrame(smsH, columns = ['emp_sap_id'])
     df_smsA = pd.DataFrame(smsA, columns = ['activity_no'])
     df_smsV = pd.DataFrame(smsV, columns = ['amount'])
     df_SmsTo = pd.DataFrame(smsTo, columns = ['smsTo'])
     df_SmsFrom = pd.DataFrame(smsFrom, columns = ['smsFrom'])
     df_SmsMessage = pd.DataFrame(smsMessage, columns = ['smsMessage'])
     df_SmsDate = pd.DataFrame(smsDate, columns = ['smsDate'])

     result = pd.concat([df_SmsFrom, df_smsH, df_smsA, df_SmsDate, df_smsV], axis=1, join='inner')
     result.rename(columns = {'smsFrom':'contact_number', 'smsDate':'sms_date'}, inplace = True)
     result.insert(4,'latitude',None)
     result.insert(5,'longitude',None)
     result['activity_no'] = result['activity_no'].replace('',0)
     result['activity_no'] = result['activity_no'].astype(str)
     result['activity_no'] = result['activity_no'].apply(lambda x: x.zfill(10))


     #conn = pg.connect(host="192.168.130.51", port= '5432', database="focall", user="postgres", password="kamil0343")
     result.columns = result.columns.str.strip()
     #cursor = conn.cursor()
     if len(result) > 0:
         sms_columns = list(result)    
         columns = ",".join(sms_columns)  
        
         # create VALUES('%s', '%s",...) one '%s' per column
         values = "VALUES({})".format(",".join(["%s" for _ in sms_columns])) 
         #create INSERT INTO table (columns) VALUES('%s',...)
         insert_stmt = "INSERT INTO {} ({}) {}".format('"public"."ACTIVITY_SMS_LOG"',columns,values)
         pg.extras.execute_batch(cur, insert_stmt, result.values)
         conn.commit()
         cur.close()

def update_activity_status():
    df = pd.read_sql('select ikon_activity_id as activity_id from "public"."ACTIVITY" where is_complete=\'APPROVED\'', conn);
    df_activity = pd.DataFrame(data=df)
    #print(df_activity)
    df = pd.read_sql('select activity_no,contact_number,emp_sap_id,amount from "public"."ACTIVITY_SMS_LOG"', conn);
    df_sms = pd.DataFrame(data=df)  
    #print(df_sms)
    df = pd.read_sql('select contact_number,hr_id  from "EMPLOYEES" e where designation_id =\'10\'', conn);
    df_employee = pd.DataFrame(data=df)  
    #print(df_employee)

    df_activity["activity_id"] = df_activity["activity_id"].astype(str)
    #print(df_activity.activity_id.dtypes)
    df_sms['activity_no'] = df_sms['activity_no'].astype(str)
    df_combined = pd.merge(df_activity, df_sms, left_on='activity_id',right_on='activity_no', how='inner')
    df_combined['contact_number'] = df_combined['contact_number'].str.lstrip('+92')
    #print(df_combined)
    df_combined['contact_number'] = '0'+df_combined['contact_number']
    #print(df_combined)
    df_combined_final_contact_number = pd.merge(df_combined, df_employee, left_on='contact_number',right_on='contact_number', how='left')
    #print(df_combined_final_contact_number)
    df_combined_final = pd.merge(df_combined_final_contact_number, df_employee, left_on='emp_sap_id',right_on='hr_id', how='inner')
    
    df_final = df_combined_final[['activity_id','amount']]
    #print(df_final)
    cur.execute("""CREATE TEMP TABLE activity_id_matched(activity_id VARCHAR(50), amount INTEGER) ON COMMIT DROP""")
    #cur.execute("""CREATE TEMP TABLE amount_expense(activity_id INTEGER, amount INTEGER) ON COMMIT DROP""")
    if len(df_final) > 0:
        activity_columns = list(df_final)    
        columns = ",".join(activity_columns) 
        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in activity_columns])) 
        #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format('activity_id_matched',columns,values)
        pg.extras.execute_batch(cur, insert_stmt, df_final.values)

    cur.execute("""
        UPDATE "public"."ACTIVITY" 
        set is_complete  = 'COMPLETED', expense_amount  = activity_id_matched.amount
        from  activity_id_matched
        WHERE "public"."ACTIVITY".is_complete = 'APPROVED' and  "public"."ACTIVITY".ikon_activity_id = activity_id_matched.activity_id;
        """)    
    cur.rowcount
    conn.commit()
    cur.close()
    conn.close()



JAZZ_SMS_API_Focall = DAG(
    dag_id='JAZZ_SMS_API_Focall_integration',
    default_args=default_args,
    #start_date= datetime(2022, 5, 30),
    schedule_interval='0 * * * *',
    #on_success_callback=success_function, 
    #email_on_failure=failure_email_function,
    description='jazz_sms_log_focall',
    catchup=False
)
############### JAZZ SMS Get API Data ###################
JAZZ_SMS_Focall = PythonOperator(
        task_id="JAZZ_SMS_Focall",
        python_callable=extract_sms_data_to_postgres,
        #on_failure_callback=failure_email_function,
        dag=JAZZ_SMS_API_Focall 
)

############### UPDATE STATUS TO COMPLETED FROM APPROVED ###################
JAZZ_SMS_Focall_Update = PythonOperator(
        task_id="ACTIVITY_STATUS_UPDATION",
        python_callable=update_activity_status,
        #on_failure_callback=failure_email_function,
        dag=JAZZ_SMS_API_Focall 
)


JAZZ_SMS_Focall >> JAZZ_SMS_Focall_Update