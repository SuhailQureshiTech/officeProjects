from datetime import date, datetime,timedelta
from hmac import trans_36
from pickle import NONE
from airflow import DAG
from airflow import models
import airflow.operators.dummy
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import time
from datetime import datetime
from time import time, sleep
from fileinput import filename
# import imp
import  pysftp
from datetime import date
import pandas as pd
import glob
import pypyodbc as odbc
import psycopg2.extras
from pytest import param
import psycopg2 as pg
import math
import os
import shutil
from numpy import source
# from airflow.providers.postgres.operators.postgres import PostgresOperator
import platform
from hdbcli import dbapi
import pandas as pd
import urllib
import psycopg2
import numpy as np
import pyodbc
from datetime import date, datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil import parser
from datetime import datetime,date,timezone
import sys
from http import client
import pandas as pds
import numpy as np
from datalab.context import Context
from datetime import timedelta, date
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from airflow.exceptions import AirflowFailException
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
# from sqlalchemy import create_engine
import pandas_gbq
from IPython.display import display
import pysftp
import csv
from datetime import date
import pandas as pd
import calendar
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
day_diff=90
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=day_diff)
d1 = past_date.strftime("%Y-%m-%d")
df = pd.DataFrame()
# verify the architecture of Python
# print("Platform architecture: " + platform.architecture()[0])
today = date.today()
vEndDate = datetime.date(datetime.today()-timedelta(days=day_diff)).strftime("%Y%m%d")
vMaxDate = datetime.date(datetime.today()-timedelta(days=1)).strftime("%Y%m%d")

vEndDate = "'"+vEndDate+"'"
vMaxDate = "'"+vMaxDate+"'"

vPostEndDate = datetime.date(datetime.today()-timedelta(days=day_diff)).strftime("%Y-%m-%d")
vPostMaxDate = datetime.date(datetime.today()-timedelta(days=1)).strftime("%Y-%m-%d")

vPostEndDate = "'"+vPostEndDate+"'"
vPostMaxDate = "'"+vPostMaxDate+"'"

print('end date: ', vEndDate)
# Initialize your connection

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate = date + timedelta(hours=5)

conn = dbapi.connect(

    # Option2, specify the connection parameters
    address='10.210.134.204',
    port='33015',
    user='Etl',
    password='Etl@2023'
)

print('connected')
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                "SERVER=192.168.130.81\sqldw;"
                                "DATABASE=ibl_dw;"
                                "UID=pbironew;"
                                "PWD=pbiro345-")

cursor = conn.cursor()


def insertSapBkpfData():

    print('vEndDate :',vEndDate)
    print('vMaxDate :',vMaxDate)

    print('vPostEndDate :', vPostEndDate)
    print('vPostMaxDate :', vPostMaxDate)

    sql_command = f'''SELECT 	"MANDT"
	, "BUKRS"
	, "BELNR"
	, "GJAHR"
	, "BLART"
	, "BLDAT"
	, "BUDAT"
	, "MONAT"
	, "CPUDT"
	, "CPUTM"
	, "AEDAT"
	, "UPDDT"
	, "WWERT"
	, "USNAM"
	, "TCODE"
	, "BVORG"
	, "XBLNR"
	, "DBBLG"
	, "STBLG"
	, "STJAH"
	, "BKTXT"
	, "WAERS"
	, "KURSF"
	, "KZWRS"
	, "KZKRS"
	, "BSTAT"
	, "XNETB"
	, "FRATH"
	, "XRUEB"
	, "GLVOR"
	, "GRPID"
	, "DOKID"
	, "ARCID"
	, "IBLAR"
	, "AWTYP"
	, "AWKEY"
	, "FIKRS"
	, "HWAER"
	, "HWAE2"
	, "HWAE3"
	, "KURS2"
	, "KURS3"
	, "BASW2"
	, "BASW3"
	, "UMRD2"
	, "UMRD3"
	, "XSTOV"
	, "STODT"
	, "XMWST"
	, "CURT2"
	, "CURT3"
	, "KUTY2"
	, "KUTY3"
	, "XSNET"
	, "AUSBK"
	, "XUSVR"
	, "DUEFL"
	, "AWSYS"
	, "TXKRS"
	, "LOTKZ"
	, "XWVOF"
	, "STGRD"
	, "PPNAM"
	, "BRNCH"
	, "NUMPG"
	, "ADISC"
	, "XREF1_HD"
	, "XREF2_HD"
	, "XREVERSAL"
	, "REINDAT"
	, "RLDNR"
	, "LDGRP"
	, "PROPMANO"
	, "XBLNR_ALT"
	, "VATDATE"
	, "DOCCAT"
	, "XSPLIT"
	, "CASH_ALLOC"
	, "FOLLOW_ON"
	, "XREORG"
	, "SUBSET"
	, "KURST"
	, "KURSX"
	, "KUR2X"
	, "KUR3X"
	, "XMCA"
	, "RESUBMISSION"
	---, "/SAPF15/STATUS"
	, "PSOTY"
	, "PSOAK"
	, "PSOKS"
	, "PSOSG"
	, "PSOFN"
	, "INTFORM"
	, "INTDATE"
	, "PSOBT"
	, "PSOZL"
	, "PSODT"
	, "PSOTM"
	, "FM_UMART"
	, "CCINS"
	, "CCNUM"
	, "SSBLK"
	, "BATCH"
	, "SNAME"
	, "SAMPLED"
	, "EXCLUDE_FLAG"
	, "BLIND"
	, "OFFSET_STATUS"
	, "OFFSET_REFER_DAT"
	, "PENRC"
	, "KNUMV"
	, "CTXKRS"
    FROM SAPABAP1.bkpf
    WHERE 1=1 and mandt=300 and budat between '20221101' and '20230131'
    AND BUKRS  IN (1900,1600,1500,1100,1200,1000)
    '''
    # vEndDate = datetime.date(
    # datetime.today()-timedelta(days=day_diff)).strftime("%d-%b-%Y")
    # vEndDate = "'"+vEndDate+"'"
    # print('postgress end date : ', vEndDate)
    df = pd.read_sql(sql_command, conn)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.strip()
    print(df.info())

    # df['UNIT_SELLING_PRICE'] = df['UNIT_SELLING_PRICE'].fillna(0)
    # df['SOLD_QTY'] = df['SOLD_QTY'].fillna(0)
    # df['BONUS_QTY'] = df['BONUS_QTY'].fillna(0)
    # df['CLAIMABLE_DISCOUNT'] = df['CLAIMABLE_DISCOUNT'].fillna(0)
    # df['UNCLAIMABLE_DISCOUNT'] = df['UNCLAIMABLE_DISCOUNT'].fillna(0)
    # df['TAX_RECOVERABLE'] = df['TAX_RECOVERABLE'].fillna(0)
    # df['NET_AMOUNT'] = df['NET_AMOUNT'].fillna(0)
    # df['GROSS_AMOUNT'] = df['GROSS_AMOUNT'].fillna(0)
    # df['TOTAL_DISCOUNT'] = df['TOTAL_DISCOUNT'].fillna(0)
    # df['COMPANY_CODE_NUM'] = df['COMPANY_CODE_NUM'].fillna(0)

    conn2 = pg.connect(host="35.216.168.189", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    cursor = conn2.cursor()

    # delete_sql = f'''DELETE FROM "DW"."IBL_SALES" where billing_Date>=(current_date-{day_diff})
    #                         and company_code in ('6300','6100')  '''
    # cursor.execute(delete_sql)
    # conn2.commit()

    if len(df) > 0:
            df_columns = list(df)
            columns = ",".join(df_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {}".format('"hanadb_prd"."bkpf_new"', columns, values)
    pg.extras.execute_batch(cursor, insert_stmt, df.values)
    conn2.commit()
    cursor.close()

# bseg
def insertSapBsegData():

    print('vEndDate :',vEndDate)
    print('vMaxDate :',vMaxDate)

    print('vPostEndDate :', vPostEndDate)
    print('vPostMaxDate :', vPostMaxDate)

    sql_command = f'''SELECT
	MANDT
	, BUDAT
	, BUKRS
	, BELNR
	, GJAHR
	, BUZEI
	, BUZID
	, AUGDT
	, AUGCP
	, AUGBL
	, BSCHL
	, KOART
	, UMSKZ
	, UMSKS
	, ZUMSK
	, SHKZG
	, GSBER
	, PARGB
	, MWSKZ
	, QSSKZ
	, DMBTR
	, WRBTR
	, KZBTR
	, PSWBT
	, PSWSL
	, TXBHW
	, TXBFW
	, MWSTS
	, WMWST
	, HWBAS
	, FWBAS
	, HWZUZ
	, FWZUZ
	, SHZUZ
	, STEKZ
	, MWART
	, TXGRP
	, KTOSL
	, QSSHB
	, KURSR
	, GBETR
	, BDIFF
	, BDIF2
	, VALUT
	, ZUONR
	, SGTXT
	, ZINKZ
	, VBUND
	, BEWAR
	, ALTKT
	, VORGN
	, FDLEV
	, FDGRP
	, FDWBT
	, FDTAG
	, FKONT
	, KOKRS
	, KOSTL
	, PROJN
	, AUFNR
	, VBELN
	, VBEL2
	, POSN2
	, ETEN2
	, ANLN1
	, ANLN2
	, ANBWA
	, BZDAT
	, PERNR
	, XUMSW
	, XHRES
	, XKRES
	, XOPVW
	, XCPDD
	, XSKST
	, XSAUF
	, XSPRO
	, XSERG
	, XFAKT
	, XUMAN
	, XANET
	, XSKRL
	, XINVE
	, XPANZ
	, XAUTO
	, XNCOP
	, XZAHL
	, SAKNR
	, HKONT
	, KUNNR
	, LIFNR
	, FILKD
	, XBILK
	, GVTYP
	, HZUON
	, ZFBDT
	, ZTERM
	, ZBD1T
	, ZBD2T
	, ZBD3T
	, ZBD1P
	, ZBD2P
	, SKFBT
	, SKNTO
	, WSKTO
	, ZLSCH
	, ZLSPR
	, ZBFIX
	, HBKID
	, BVTYP
	, NEBTR
	, MWSK1
	, DMBT1
	, WRBT1
	, MWSK2
	, DMBT2
	, WRBT2
	, MWSK3
	, DMBT3
	, WRBT3
	, REBZG
	, REBZJ
	, REBZZ
	, REBZT
	, ZOLLT
	, ZOLLD
	, LZBKZ
	, LANDL
	, DIEKZ
	, SAMNR
	, ABPER
	, VRSKZ
	, VRSDT
	, DISBN
	, DISBJ
	, DISBZ
	, WVERW
	, ANFBN
	, ANFBJ
	, ANFBU
	, ANFAE
	, BLNBT
	, BLNKZ
	, BLNPZ
	, MSCHL
	, MANSP
	, MADAT
	, MANST
	, MABER
	, ESRNR
	, ESRRE
	, ESRPZ
	, KLIBT
	, QSZNR
	, QBSHB
	, QSFBT
	, NAVHW
	, NAVFW
	, MATNR
	, WERKS
	, MENGE
	, MEINS
	, ERFMG
	, ERFME
	, BPMNG
	, BPRME
	, EBELN
	, EBELP
	, ZEKKN
	, ELIKZ
	, VPRSV
	, PEINH
	, BWKEY
	, BWTAR
	, BUSTW
	, REWRT
	, REWWR
	, BONFB
	, BUALT
	, PSALT
	, NPREI
	, TBTKZ
	, SPGRP
	, SPGRM
	, SPGRT
	, SPGRG
	, SPGRV
	, SPGRQ
	, STCEG
	, EGBLD
	, EGLLD
	, RSTGR
	, RYACQ
	, RPACQ
	, RDIFF
	, RDIF2
	, PRCTR
	, XHKOM
	, VNAME
	, RECID
	, EGRUP
	, VPTNR
	, VERTT
	, VERTN
	, VBEWA
	, DEPOT
	, TXJCD
	, IMKEY
	, DABRZ
	, POPTS
	, FIPOS
	, KSTRG
	, NPLNR
	, AUFPL
	, APLZL
	, PROJK
	, PAOBJNR
	, PASUBNR
	, SPGRS
	, SPGRC
	, BTYPE
	, ETYPE
	, XEGDR
	, LNRAN
	, HRKFT
	, DMBE2
	, DMBE3
	, DMB21
	, DMB22
	, DMB23
	, DMB31
	, DMB32
	, DMB33
	, MWST2
	, MWST3
	, NAVH2
	, NAVH3
	, SKNT2
	, SKNT3
	, BDIF3
	, RDIF3
	, HWMET
	, GLUPM
	, XRAGL
	, UZAWE
	, LOKKT
	, FISTL
	, GEBER
	, STBUK
	, TXBH2
	, TXBH3
	, PPRCT
	, XREF1
	, XREF2
	, KBLNR
	, KBLPOS
	, STTAX
	, FKBER
	, OBZEI
	, XNEGP
	, RFZEI
	, CCBTC
	, KKBER
	, EMPFB
	, XREF3
	, DTWS1
	, DTWS2
	, DTWS3
	, DTWS4
	, GRICD
	, GRIRG
	, GITYP
	, XPYPR
	, KIDNO
	, ABSBT
	, IDXSP
	, LINFV
	, KONTT
	, KONTL
	, TXDAT
	, AGZEI
	, PYCUR
	, PYAMT
	, BUPLA
	, SECCO
	, LSTAR
	, CESSION_KZ
	, PRZNR
	, PPDIFF
	, PPDIF2
	, PPDIF3
	, PENLC1
	, PENLC2
	, PENLC3
	, PENFC
	, PENDAYS
	, PENRC
	, GRANT_NBR
	, SCTAX
	, FKBER_LONG
	, GMVKZ
	, SRTYPE
	, INTRENO
	, MEASURE
	, AUGGJ
	, PPA_EX_IND
	, DOCLN
	, SEGMENT
	, PSEGMENT
	, PFKBER
	, HKTID
	, KSTAR
	, XLGCLR
	, TAXPS
	, PAYS_PROV
	, PAYS_TRAN
	, MNDID
	, XFRGE_BSEG
	, PGEBER
	, PGRANT_NBR
	, BUDGET_PD
	, PBUDGET_PD
	, PEROP_BEG
	, PEROP_END
	, FASTPAY
	, IGNR_IVREF
	, FMFGUS_KEY
	, FMXDOCNR
	, FMXYEAR
	, FMXDOCLN
	, FMXZEKKN
	, PRODPER
	, SQUAN
	, J_1TPBUPL
    FROM etl.bseg_details
    WHERE 	budat  between  '20221101' and '20230131'
    '''
    # vEndDate = datetime.date(
    # datetime.today()-timedelta(days=day_diff)).strftime("%d-%b-%Y")
    # vEndDate = "'"+vEndDate+"'"
    # print('postgress end date : ', vEndDate)

    df = pd.read_sql(sql_command, conn)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.strip()
    print(df.info())

    # df['UNIT_SELLING_PRICE'] = df['UNIT_SELLING_PRICE'].fillna(0)
    # df['SOLD_QTY'] = df['SOLD_QTY'].fillna(0)
    # df['BONUS_QTY'] = df['BONUS_QTY'].fillna(0)
    # df['CLAIMABLE_DISCOUNT'] = df['CLAIMABLE_DISCOUNT'].fillna(0)
    # df['UNCLAIMABLE_DISCOUNT'] = df['UNCLAIMABLE_DISCOUNT'].fillna(0)
    # df['TAX_RECOVERABLE'] = df['TAX_RECOVERABLE'].fillna(0)
    # df['NET_AMOUNT'] = df['NET_AMOUNT'].fillna(0)
    # df['GROSS_AMOUNT'] = df['GROSS_AMOUNT'].fillna(0)
    # df['TOTAL_DISCOUNT'] = df['TOTAL_DISCOUNT'].fillna(0)
    # df['COMPANY_CODE_NUM'] = df['COMPANY_CODE_NUM'].fillna(0)

    conn2 = pg.connect(host="35.216.168.189", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    cursor = conn2.cursor()

    # delete_sql = f'''DELETE FROM "DW"."IBL_SALES" where billing_Date>=(current_date-{day_diff})
    #                         and company_code in ('6300','6100')  '''
    # cursor.execute(delete_sql)
    # conn2.commit()

    if len(df) > 0:
            df_columns = list(df)
            columns = ",".join(df_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {}".format('"hanadb_prd"."bseg_new"', columns, values)
    pg.extras.execute_batch(cursor, insert_stmt, df.values)
    conn2.commit()
    cursor.close()

default_args = {
            'owner': 'admin',
            'depends_on_past': False,
            'email': ['muhammad.arslan@iblgrp.com'],
            'email_on_failure': True,
            # 'start_date': datetime(2022,2,11)
            'retries': 2,
            'retry_delay': timedelta(minutes=10)
        }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
        dag_id='SAP_FINANCE_DATA',
        # start date:28-03-2017
        start_date= datetime(year=2022, month=10, day=24),
        # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
        catchup=False,
        # schedule_interval='0/30 0-0 * * *'
        schedule_interval=None
        ) as dag:

    insertBkpfPostgres= PythonOperator(
		task_id='insert_BKPF_Data',
        python_callable= insertSapBkpfData,
        dag=dag,
	)

    insertBsegPostgres = PythonOperator(
        task_id='insert_BSEG_Data',
        python_callable=insertSapBsegData,
        dag=dag,
    )

# t1>>t2>>t3>>t4>>Load_Sale_Data_to_gcs>>Delete_From_BQ>>Load_sale_data_gcs_to_bq
# checkFileExists>>t1 >> t2 >> t3 >> Load_Sale_Data_to_gcs >> Delete_From_BQ >> Load_sale_data_gcs_to_bq>>writeFile
insertBkpfPostgres
# insertBsegPostgres
