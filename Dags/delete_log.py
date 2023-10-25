import psycopg2 as pg
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from dateutil import parser
from datetime import datetime,date
import sys
from http import client
import pandas as pds
import numpy as np
from datalab.context import Context
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.exceptions import AirflowException
from airflow.utils.state import State
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
from airflow.operators.email_operator import EmailOperator
# from sqlalchemy import create_engine
import pandas_gbq
from IPython.display import display
import pysftp
import csv
import pyodbc
#import conf
from airflow.operators.bash import BashOperator
from os import walk





def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Stock DAG has executed successfully."
    subject = f"Stock DAG has completed"
    send_email_smtp(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)

def failure_email_function(context):
    dag_run = context.get('dag_run')
    print("mic testing 123")
    msg = "Stock DAG has failed"
    subject = f"Stock DAG {dag_run} has failed"
    send_email_smtp(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)

default_args = {
        'owner': 'admin',
        'start_date': datetime(2022, 5, 30),
        # 'end_date': datetime(),
        'email': ['shehzad.lalani@iblgrp.com'],
        'on_success_callback':success_function,
        'email_on_failure': True,
        'on_failure_callback': failure_email_function
        #'on_success_callback': dag_success_alert,
        #'on_failure_callback': failure_email_function,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

delLogDag = DAG(
    dag_id='delete_log_files',
    default_args=default_args,
    start_date= datetime(2023, 1, 23),
    catchup=False,
    schedule_interval=None,
    # on_success_callback=success_function,
    #email_on_failure=failure_email_function,
    description='delete_log_files'
)


def printFiles():
    dir_path = r'/home/admin2/airflow/logs/dag_id=Booking_vs_Execution/'


# list to store files name
    for path in os.scandir(dir_path):
        if path.is_file():
            print(path.name)

printFilesTask = PythonOperator(
        task_id="Print_Files",
        python_callable=printFiles,
        dag=delLogDag
)

task_1 = BashOperator(task_id='shell_execute'
            ,bash_command='echo "Welcome to Dezyre testing the bashoperator" >> /home/admin2/airflow/logs/dag_id=Booking_vs_Execution/run_id=manual__2023-03-22T06:10:30.838493+00:00/task_id=DOWNLOAD_FILE_FROM_FTP/attempt=1.log'
            ,dag=delLogDag
            )

printFilesTask
task_1



