from genericpath import isfile
import imp
from fileinput import filename
import psycopg2 as pg
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
from datetime import datetime, date
import os,glob

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 10, 11),
    # 'end_date': datetime(),
    'depends_on_past': False,
    'email': ['shehzad.lalani@iblgrp.com'],
    'email_on_failure': True,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),

}

deleteStatusFile = DAG(
    dag_id='deleteStatusFiles',
    default_args=default_args,
    start_date=datetime(year=2022, month=11, day=29),
    schedule_interval='8 13,17 * * *',
    # dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    description='checkFileExists'
)


def deleteCheck():

    dir = "/home/admin2/airflow/dags/checkFile/"
    filelist = glob.glob(os.path.join(dir, "*"))
    for f in filelist:
        os.remove(f)

checkFileExists = PythonOperator(
    task_id="deleteAllStatusFile",
    python_callable=deleteCheck,
    dag=deleteStatusFile
)






