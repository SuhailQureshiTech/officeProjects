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
import os

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

checkFile = DAG(
    dag_id='checkFileExists',
    default_args=default_args,
    start_date=datetime(year=2022, month=9, day=27),
    schedule_interval=None,
    # dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    description='checkFileExists'
)


# def fileCheck():
#     path = "/home/admin2/airflow/dags/checkFile/check.txt"
#     isFile = os.path.isfile(path)
#     if isFile==True:
#         print('Yes file exists', isFile)
#         raise AirflowFailException('Faile file exists cannot merge')
#     else:
#         print('Yes file exists', isFile)

# checkFileExists = PythonOperator(
#     task_id="checkFile",
#     python_callable=fileCheck,
#     dag=checkFile
# )


def writeFile():
    path = "/home/admin2/airflow/dags/checkFile/check.txt"

    with open('/home/admin2/airflow/dags/checkFile/check.txt', 'w') as f:
        f.write('Create a new text file!')
    print('File Created...')



writeFile = PythonOperator(
    task_id="writeFile",
    python_callable=writeFile,
    dag=checkFile
)




