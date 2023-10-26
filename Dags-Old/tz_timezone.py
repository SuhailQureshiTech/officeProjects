from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
# Import the Pendulum library.
import pendulum

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone('Asia/Karachi')

with DAG(
    dag_id='tz_test',
    schedule_interval='00 21 * * *',
    catchup=False,
    start_date=datetime(2022, 10, 25, tzinfo=local_tz)
) as dag:
    bash_operator_task = BashOperator(
        task_id='tz_aware_task',
        dag=dag,
        bash_command='date'
    )
