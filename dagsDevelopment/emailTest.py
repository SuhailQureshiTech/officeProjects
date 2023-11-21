from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from airflow import DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'email_tutorial',
    default_args=default_args,
    description='A simple email ',
    schedule_interval='20 16 * * *',
    start_date=datetime(year=2023, month=4, day=30),
    catchup=False
) as dag:
    send_email_notification=EmailOperator(
        task_id="send_test_email",
        to= ['muhammad.suhail@iblgrp.com','orasyscojava@gmail.com'],
        subject="Test email",
        html_content="<h2>Hogya Bhaiya....."   
    )