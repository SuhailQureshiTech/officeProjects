from datetime import date, datetime,timedelta
from airflow import DAG
from airflow import models
import airflow.operators.dummy
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
import os
import sys
import pyodbc as odbc
from datetime import date,datetime,timedelta
import pydoc
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from http import client
import pyodbc 
import pandas as pd
from dateutil import parser
from datetime import date, datetime,timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
import time
from datetime import datetime
from time import time, sleep



default_args = {
        'owner': 'admin',
        'depends_on_past': False,
        'email': ['muhammad.arslan@iblgrp.com'],
	    'email_on_failure': True,
        # 'start_date': datetime(2022,2,11)

      }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
   dag_id='Insert_Sales_Data',
   # start date:28-03-2017
   start_date= datetime(year=2022, month=3, day=16),
   # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
   schedule_interval='30 03 * * *'
) as dag:



	t1 = PostgresOperator(
		task_id='TRUNCATE_PHNX_SALES_DATA_TMP',
		postgres_conn_id="Postgresql_IBL",	
		sql="""   
			truncate table "DW".phnx_sales_data_tmp

			""",
		dag=dag,
	)

	t2 = PostgresOperator(
		task_id='TRUNCATE_PHNX_SALES_DATA_TMP_KONV',
		postgres_conn_id="Postgresql_IBL",	
		sql="""   
			truncate table "DW".phnx_sales_data_tmp_konv

			""",
		dag=dag,
	)

	t3 = PostgresOperator(
		task_id='PHNX_SALES_DATA_TMP_INSERTION',
		postgres_conn_id="Postgresql_IBL",	
		sql="""
		INSERT INTO "DW".phnx_sales_data_tmp 
	(item_category,billing_type,up_key,document_no,cancelled_flag,company_code,document_condition,
	billing_date,channel,customer_code,billing_cancelled,payment_type,item_no,quantity,
	unit,gross_amount,unit_selling_price,amount,material_code,cost_centre,
	profit_centre,org_id,sales_order_no,business_line_id,booker_id,
	supplier_id,sales_order_type,bstnk,item_desc,unclaim_bonus_quantity,
	claim_bonus_quantity,claimable_discount,unclaimable_discount,
	tax_recoverable,billing_type_text,item_category_text,sd_document_category,knumv,posnr,loading_date)
	(select
		distinct
		"VBRP"."PSTYV" as ITEM_CATEGORY,
		"VBRK"."FKART" as BILLING_TYPE,
		concat("VBRK"."KNUMV", '-', "VBRP"."POSNR") UP_KEY,
		"VBRK"."VBELN" as DOCUMENT_NO,
		(case
			when "VBRK"."FKART" = 'ZUCC' then 'Cancelled'
			when "VBRK"."FKART" = 'ZURB' then 'Return'
		end) as CANCELLED_FLAG,
		"VBRK"."VKORG" as COMPANY_CODE,
		"VBRK"."KNUMV" as DOCUMENT_CONDITION,
		cast("FKDAT" as date) as BILLING_DATE,
		"KDGRP" as CHANNEL,
		"KUNAG" as CUSTOMER_CODE,
		"FKSTO" as BILLING_CANCELLED,
		"ZTERM" as PAYMENT_TYPE,
		"VBRP"."POSNR" as ITEM_NO,
		(case
			when "VBRP"."PSTYV" = 'ZBRU'
			or "VBRP"."PSTYV" = 'ZBRC' then "VBRP"."FKIMG" *-1
			else "VBRP"."FKIMG"
		end)as QUANTITY,
		"VBRP"."MEINS" as UNIT,
		0.00 GROSS_AMOUNT,
		0.00 UNIT_SELLING_PRICE,
		"VBRP"."NETWR" as AMOUNT,
		"VBRP"."MATNR" as MATERIAL_CODE,
		"VBRP"."KOSTL" as COST_CENTRE,
		"VBRP"."PRCTR" as PROFIT_CENTRE,
		"VBRP"."VKBUR" as ORG_ID,
		"VBRP"."AUBEL" as SALES_ORDER_NO,
		"VBRP"."MVGR1" as BUSINESS_LINE_ID,
		VBP1."PERNR" BOOKER_ID,
		"VBPA"."PERNR" SUPPLIER_ID,
		"VBAK"."AUART" SALES_ORDER_TYPE,
		"VBAK"."BSTNK" BSTNK,
		"VBRP"."ARKTX" as ITEM_DESC,
		(case when "VBRP"."PSTYV" = 'ZFOU' then "VBRP"."FKIMG"
			when "VBRP"."PSTYV" = 'ZBRU' then "VBRP"."FKIMG" *-1
		end) as UNCLAIM_BONUS_QUANTITY,
		(case
			when "VBRP"."PSTYV" = 'ZFCL' then "VBRP"."FKIMG"
			when "VBRP"."PSTYV" = 'ZBRC' then "VBRP"."FKIMG" *-1
		end) as CLAIM_BONUS_QUANTITY,
		0.00 CLAIMABLE_DISCOUNT,
		0.00 UNCLAIMABLE_DISCOUNT,
		0.00 TAX_RECOVERABLE,
		"TVFKT"."VTEXT" BILLING_TYPE_TEXT,
		"TVAPT"."VTEXT" ITEM_CATEGORY_TEXT,
		"VBRK"."VBTYP" as SD_DOCUMENT_CATEGORY,
		"VBRK"."KNUMV" knumv,
		"VBRP"."POSNR" posnr,
		"VBRP"."Loading_Date" loading_date
		from hanadb_prd."VBRK"
	inner  join hanadb_prd."VBRP" on ("VBRP"."VBELN" = "VBRK"."VBELN")
	inner  join hanadb_prd."VBPA" VBP1 on (vbp1 ."VBELN" = "VBRP"."VBELN" and vbp1 ."PARVW" = 'BK')
	inner  join hanadb_prd."VBPA" on ("VBPA"."VBELN" = "VBRP"."VBELN" and "VBPA"."PARVW" = 'ZS')
	inner join hanadb_prd."VBAK" on ("VBAK"."VBELN" = "VBRP"."AUBEL")
	left outer join hanadb_prd."TVFKT" on ("VBRK"."FKART" = "TVFKT"."FKART" and "TVFKT"."SPRAS"='E')
	left outer join hanadb_prd."TVAPT" on ("VBRP"."PSTYV" = "TVAPT"."PSTYV" and "TVAPT"."SPRAS" ='E' )
	where
		1 = 1
		and "FKDAT"  =  TO_CHAR((current_date-2)  ,'YYYYMMDD') 
				and "VBRK"."VKORG" in ('6300', '6100') and "VBRK"."FKART" in ( 'ZOPC', 'ZOCC', 'ZUBC', 'ZUCC', 'ZORE', 'ZORC', 'ZURB', 'ZUB1', 'ZNES', 'ZNEC'));
			""",
		dag=dag,
	)

	t4 = PostgresOperator(
		task_id='KONV_TABLE_INSERTION',
		postgres_conn_id="Postgresql_IBL",	
		sql="""   
	INSERT INTO "DW".phnx_sales_data_tmp_konv 
	(up_key,knumv,kposn,claimable_discount,unclaimable_discount,tax_recoverable,unit_selling_price)
	(
	select concat(KNUMV,'-', KPOSN) up_key,KNUMV,KPOSN,sum(CLAIMABLE_DISCOUNT) CLAIMABLE_DISCOUNT,sum(UNCLAIMABLE_DISCOUNT) UNCLAIMABLE_DISCOUNT,sum(TAX_RECOVERABLE) TAX_RECOVERABLE,sum(UNIT_SELLING_PRICE) UNIT_SELLING_PRICE
	from (
	SELECT 
		distinct
		KONV1."KNUMV" as KNUMV,
		KONV1."KPOSN" as KPOSN,
		(case when KONV1."KSCHL" ='ZCDP' or KONV1."KSCHL" ='ZCDV' or "KSCHL" ='ZCVD' or KONV1."KSCHL" ='ZCP' then  Sum(KONV1."KWERT")*-1 end ) as CLAIMABLE_DISCOUNT,
		(case when KONV1."KSCHL" ='ZUDP' or KONV1."KSCHL" ='ZUDV' or KONV1."KSCHL" ='ZUP'  then  Sum(KONV1."KWERT")*-1 end ) as UNCLAIMABLE_DISCOUNT,
		(case when KONV1."KSCHL" ='ZMWS' or KONV1."KSCHL" ='ZFTX' or KONV1."KSCHL" ='ZADV' or KONV1."KSCHL" ='ZMRT' or KONV1."KSCHL" ='ZEXT'  or KONV1."KSCHL" ='ZSRG' then  Sum(KONV1."KWERT") end ) as TAX_RECOVERABLE,
		(case when KONV1."KSCHL" ='ZTRP' then sum(KONV1."KBETR") end) as UNIT_SELLING_PRICE
		from hanadb_prd."KONV" KONV1
		where KONV1."KINAK"<>'Y' and  concat(KONV1."KNUMV",'-', KONV1."KPOSN")  in (select up_key from "DW".phnx_sales_data_tmp)
		group by  KONV1."KNUMV",KONV1."KPOSN" ,KONV1."KSCHL"
	) A 
	group by KNUMV,KPOSN
	)

			""",
		dag=dag,
	)

	t5 = PostgresOperator(
		task_id='UPDATE_PHNX_SALES_DATA_TMP',
		postgres_conn_id="Postgresql_IBL",	
		sql="""   
			UPDATE "DW".phnx_sales_data_tmp psl
	set (CLAIMABLE_DISCOUNT,UNCLAIMABLE_DISCOUNT,TAX_RECOVERABLE,UNIT_SELLING_PRICE,GROSS_AMOUNT) =
	(
	select CLAIMABLE_DISCOUNT,UNCLAIMABLE_DISCOUNT,TAX_RECOVERABLE,UNIT_SELLING_PRICE,
	(case
			when psl.BILLING_TYPE = 'ZURB' or psl.BILLING_TYPE = 'ZUCC' then (t.UNIT_SELLING_PRICE*psl.QUANTITY)*-1
			else (t.UNIT_SELLING_PRICE*psl.QUANTITY)
		end )GROSS_AMOUNT
	from "DW".phnx_sales_data_tmp_konv t
	where t.up_key= psl.UP_KEY
	)
			""",
		dag=dag,
	)


	# t6 = PostgresOperator(
	# 	task_id='Delete_FROM_PHNX_SALES_DETAIL_DATA',
	# 	postgres_conn_id="Postgresql_IBL",	
	# 	sql="""   
	# 		delete from  hanadb_prd.phnx_sales_details_data psd
	# where psd.document_no in (select document_no  from hanadb_prd.phnx_sales_data_tmp);

	# 		""",
	# 	dag=dag,
	# )

	# t7 = PostgresOperator(
	# 	task_id='Delete_Cancelled_FROM_PHNX_SALES_DETAIL_DATA',
	# 	postgres_conn_id="Postgresql_IBL",	
	# 	sql="""   
	# 		delete from  hanadb_prd.phnx_sales_details_data psd
    #         where psd.document_no in (select "VBELN"  from hanadb_prd."VBRK"  
    #                                  where  "VKORG" in ('6300', '6100') and "FKSTO" ='X');

	# 		""",
	# 	dag=dag,
	# )

	t8 = PostgresOperator(
		task_id='TRUNCATE_PHNX_SALES_DETAIL_DATA',
		postgres_conn_id="Postgresql_IBL",	
		sql="""   
			truncate table "DW"."PHNX_SALES_DETAIL_DATA"

			""",
		dag=dag,
	)
	t9 = PostgresOperator(
		task_id='Insert_INTO_PHNX_SALES_DETAIL_DATA',
		postgres_conn_id="Postgresql_IBL",	
		sql="""   
			INSERT INTO "DW"."PHNX_SALES_DETAIL_DATA" 
	(item_category,billing_type,up_key,document_no,cancelled_flag,company_code,document_condition,billing_date,channel,customer_code,billing_cancelled,payment_type,item_no,quantity,unit,gross_amount,unit_selling_price,amount,material_code,cost_centre,profit_centre,org_id,sales_order_no,business_line_id,booker_id,supplier_id,sales_order_type,bstnk,item_desc,unclaim_bonus_quantity,claim_bonus_quantity,claimable_discount,unclaimable_discount,tax_recoverable,billing_type_text,item_category_text,sd_document_category,knumv,posnr,loading_date)
	(
	select item_category,billing_type,up_key,document_no,cancelled_flag,company_code,document_condition,billing_date,channel,customer_code,billing_cancelled,payment_type,item_no,quantity,unit,gross_amount,unit_selling_price,amount,material_code,cost_centre,profit_centre,org_id,sales_order_no,business_line_id,booker_id,supplier_id,sales_order_type,bstnk,item_desc,unclaim_bonus_quantity,claim_bonus_quantity,claimable_discount,unclaimable_discount,tax_recoverable,billing_type_text,item_category_text,sd_document_category,knumv,posnr,loading_date
	from "DW".phnx_sales_data_tmp
	)

			""",
		dag=dag,
	)


	t10 = PostgresOperator(
		task_id='Insert_INTO_PHNX_SALES_DATA',
		postgres_conn_id="Postgresql_IBL",	
		sql="""   
			INSERT INTO "DW"."PHNX_SALES_DATA"(ITEM_CATEGORY
        , ORG_ID
        , ORG_DESC
        , TRX_DATE
        , TRX_NUMBER
        , BOOKER_ID
        , BOOKER_NAME
        , SUPPLIER_ID
        , SUPPLIER_NAME
        , BUSINESS_LINE_ID
        , BUSINESS_LINE
        , CHANNEL
        , CUSTOMER_ID
        , CUSTOMER_NUMBER
        , CUSTOMER_NAME
        , SALES_ORDER_TYPE
        , INVENTORY_ITEM_ID
        , ITEM_CODE
        , DESCRIPTION
        , UNIT_SELLING_PRICE
        , SOLD_QTY
        , BONUS_QTY
        , CLAIMABLE_DISCOUNT
        , UNCLAIMABLE_DISCOUNT
        , TAX_RECOVERABLE
        , NET_AMOUNT
        , GROSS_AMOUNT
        , TOTAL_DISCOUNT
        , CUSTOMER_TRX_ID
        , REASON_CODE
        , BILL_TYPE_DESC
        , COMPANY_CODE
        , BILLING_TYPE
        , SALES_ORDER_NO
        , ADD1
        , ADD2
        , ADD3
        , BSTNK
        , ITEM_NO
          ,RETURN_REASON_CODE
        )
	(
	SELECT
            ITEM_CATEGORY
            , ORG_ID
            , ORG_DESC
            , TRX_DATE
            , TRX_NUMBER
            , BOOKER_ID
            , BOOKER_NAME
            , SUPPLIER_ID
            , SUPPLIER_NAME
            , BUSINESS_LINE_ID
            , BUSINESS_LINE
            , CHANNEL
            , CUSTOMER_ID
            , CUSTOMER_NUMBER
            , CUSTOMER_NAME
            , SALES_ORDER_TYPE
            , INVENTORY_ITEM_ID
            , ITEM_CODE
            , DESCRIPTION
            , UNIT_SELLING_PRICE
            , SOLD_QTY
            , BONUS_QTY
            , CLAIMABLE_DISCOUNT
            , UNCLAIMABLE_DISCOUNT
            , TAX_RECOVERABLE
            , NET_AMOUNT
            , GROSS_AMOUNT
            , TOTAL_DISCOUNT
            , CUSTOMER_TRX_ID
            , REASON_CODE
            , BILL_TYPE_DESC
            , COMPANY_CODE
            , BILLING_TYPE
            , SALES_ORDER_NO
            , ADD1
            , ADD2
            , ADD3
            , BSTNK
            , ITEM_NO
             , RETURN_REASON_CODE
            FROM "DW"."PHNX_SALES_VIEW"
	)

			""",
		dag=dag,
	)


    
    

t1>>t2>>t3>>t4>>t5>>t8>>t9>>t10
    