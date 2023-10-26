from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email_smtp

def failure_function(context):
    dag_run = context.get('dag_run')
    msg = "Production test....."
    subject = f"DAG {dag_run} Failed"
    send_email_smtp(to='muhammad.suhail@iblgrp.com', subject=subject, html_content=msg)

def success_function(context):
    dag_run = context.get('dag_run')
    msg = "Production Test........"
    subject = f"DAG {dag_run} has completed"
    send_email_smtp(to=['muhammad.suhail@iblgrp.com','suhailqureshi.ibl@gmail.com','Shehzad.Lalani@iblgrp.com'],
     subject=subject, html_content=msg)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 3, 21),
}

with DAG('check_task_status_pass_fail',
        default_args=default_args,
        description='Production Test',
        schedule_interval=None ,
        catchup=False
         ) as dag:

    say_hello = BashOperator(
        task_id='say_hello',
        on_failure_callback=failure_function,
        on_success_callback=success_function,
        bash_command='echo Hello'
    )

    open_temp_folder = BashOperator(
        task_id='open_temp_folder',
        on_failure_callback=failure_function,
        on_success_callback=success_function,
        bash_command='cd temp_folder'
    )
say_hello >> open_temp_folder

