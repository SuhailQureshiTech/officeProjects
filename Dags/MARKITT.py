import json
from airflow.models.connection import Connection
from datetime import datetime
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


default_args = {
        'owner': 'admin',
        'depends_on_past': False,
        'email': ['muhammad.arslan@iblgrp.com'],
	    'email_on_failure': True,
        # 'start_date': datetime(2022,2,11)

      }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
   dag_id='MARKITT_INTEGRATION',
   # start date:28-03-2017
   start_date= datetime(year=2022, month=3, day=16),
   # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
   schedule_interval='30 06 * * *') as dag:

    c = Connection(
        conn_id="MSSQL_SERVER",
        conn_type="MSSQL",
        description="connection description",
        host="192.168.130.81\sqldw",
        login="pbironew",
        password="pbiro345-",
        extra=json.dumps(dict(this_param="some val", that_param="other val*")),
    )
t1 = MsSqlOperator(
		task_id='TRUNCATE_PHNX_SALES_DATA_TMP',
		mssql_conn_id="MSSQL_SERVER",	
		sql="""   
			SELECT * FROM EBS_INVOICE_ORDER_VW

			""",
		dag=dag,
	)




	
    # t1>>t2>>t3>>t4>>t5>>t6>>t7>>t8