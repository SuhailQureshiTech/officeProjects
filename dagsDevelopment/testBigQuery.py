from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.email import send_email_smtp
from airflow.operators.email_operator import EmailOperator
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
from datetime import date, datetime,timedelta
from airflow.utils.email import send_email

#step 2: Set default argument variable.

default_args = {
    'owner':'rc_user',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}
 
# step 3: Set BigQuery sql.
src_sql = """
insert into data-light-house-prod.EDW.cust_info values(642,'kevin');
"""

def send_email_success(context):
    #dag_run = context.get('dag_run')
    msg = "Markitt Sales -- DAG to BigQuery Failed.",
    subject = f"Markitt Sales DAG to BigQuery Failed",
    send_email_smtp(to=['muhammad.suhail@iblgrp.com']
                    ,cc=['orasyscojava@gmail.com']
                    ,subject=subject, html_content=msg
                    )

def multiple_email_send_test():
    email_list = ['muhammad.suhail@iblgrp.com', 'orasyscojava@gmail.com']
    for i in range(len(email_list)):
        send_email(
            to=str(email_list[i]),
            subject='Email Header',
            html_content= f"""
                    Hi {email_list[i]}, <br>
                    <p>This is the body of the email</p>
                    <br> Thank You. <br>
                """
        ) 

#step 4: initiate the DAG variable for all the task operators.
 
with DAG("test_bq_conn_dag",
         default_args=default_args,
         description="Run the sample Biqguery sql",
         schedule_interval='23 14 * * *',
         start_date=datetime(year=2023, month=4, day=30),
         catchup=False,
         on_success_callback=multiple_email_send_test,
        ) as dag:
       
    InsertBQ =  BigQueryOperator(
        task_id='Insertingtestdata',
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,

        sql=f'''
                insert into data-light-house-prod.EDW.cust_info values(642,'kevin')       
            '''
                    )

 
""" Dummy task """
# dummy_task = DummyOperator(task_id='DummyOperator')
    
#step 6:set dependency for the task
   
InsertBQ