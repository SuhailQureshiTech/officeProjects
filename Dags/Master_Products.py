import time
import requests
import regex as re
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as pg
import psycopg2.extras
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from dateutil import parser
from datetime import datetime,date
from datalab.context import Context
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.utils.email import send_email
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
# from sqlalchemy import create_engine
import pyodbc
from hdbcli import dbapi
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import numpy as np

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Products Master DAG has executed successfully."
    subject = f"Products Master DAG has completed"
    send_email(to=['shehzad.lalani@iblgrp.com','muhammad.aamir@iblgrp.com'], subject=subject, html_content=msg)


default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 5, 30),
        # 'end_date': datetime(),
        'depends_on_past': False,
        'email_on_failure': True,
        'email': ['shehzad.lalani@iblgrp.com'],
        'on_success_callback':success_function,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

def insert_MasterTable_Products_To_GCS():
    # Initialize your connection
     conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
    # Read data from MARA
     MARA   = pd.read_sql(f'''SELECT
                    MARA.MANDT AS client_name,
                    MARA.MATNR AS material_number,
                    CAST(MARA.ERSDA AS date) AS created_on,
                    MARA.ERNAM AS person_created_the_object,
                    CAST(MARA.LAEDA AS date) AS last_change_date,
                    MARA.AENAM AS person_changed_the_object, 
                    MARA.VPSTA AS maintenance_status_of_completed_object,
                    MARA.PSTAT AS maintenance_status,
                    MARA.LVORM AS client_level_flag_material_deletion,
                    MARA.MTART AS material_type,
                    MARA.MBRSH AS industry_sector,
                    MARA.MATKL AS material_group,
                    MARA.BISMT AS old_material_number,
                    MARA.MEINS AS base_unit_of_measure,
                    MARA.BSTME AS purchase_order_unit_of_measure,
                    MARA.ZEINR AS document_number,
                    MARA.ZEIAR AS document_type,
                    MARA.ZEIVR AS document_version,
                    MARA.ZEIFO AS document_page_format,
                    MARA.FERTH AS production_memo,
                    MARA.GROES AS dimensions,
                    MARA.WRKST AS basic_material,
                    MARA.NORMT AS mapping_code,
                    MARA.LABOR AS laboratory_office,
                    MARA.EKWSL AS purchasing_value_key,
                    MARA.BRGEW AS gross_weight,
                    MARA.NTGEW AS net_weight,
                    MARA.GEWEI AS weight_unit,
                    MARA.VOLUM AS volume,
                    MARA.VOLEH AS volume_unit,
                    MARA.BEHVO AS container_requirements,
                    MARA.RAUBE AS storage_conditions,
                    MARA.TEMPB AS temperature_conditions_indicator,
                    MARA.DISST AS low_level_code,
                    MARA.TRAGR AS transportation_group,
                    MARA.STOFF AS hazardous_material_number,
                    MARA.SPART AS division,
                    MARA.KUNNR AS competitor,
                    MARA.WESCH AS slips_quantity_to_printed,
                    MARA.BWSCL AS supply_source,
                    MARA.SAISO AS season_category,
                    MARA.ETIAR AS label_type,
                    MARA.ETIFO AS label_form,
                    MARA.ENTAR AS deactivated,
                    MARA.EAN11 AS international_article_number,
                    MARA.NUMTP AS category_of_international_article_number,
                    MARA.LAENG AS length,
                    MARA.BREIT AS width,
                    MARA.HOEHE AS height,
                    MARA.MEABM AS dimension_unit,
                    MARA.PRDHA AS product_hierarchy,
                    MARA.AEKLK AS stock_transfer_net_changing_cost,
                    MARA.ERGEW AS allowed_packaging_weight,
                    MARA.ERGEI AS unit_of_weight,
                    MARA.ERVOL AS allowed_packaging_volume,
                    MARA.ERVOE AS volume_unit_of_allowed_packaging_volume,
                    MARA.VABME AS variable_purchase_order_unit_active,
                    MARA.KZKFG AS configurable_material,
                    MARA.XCHPF AS batch_management_requirement_indicator,
                    MARA.VHART AS packaging_material_type,
                    MARA.FUELG AS max_level_by_volume,
                    MARA.STFAK AS stacking_factor,
                    MARA.MAGRV AS material_group_packaging_materials,
                    MARA.BEGRU AS authorization_group,
                    CAST(MARA.DATAB AS date) AS valid_from_date,
                    CAST(MARA.LIQDT AS date) AS deletion_date,
                    MARA.SAISJ AS season_year,
                    MARA.PLGTP AS price_bond_category,
                    MARA.MLGUT AS empties_bill_of_material,
                    MARA.EXTWG AS external_material_group,
                    MARA.SATNR AS cross_plant_configurable_material,
                    MARA.ATTYP AS material_category,
                    MARA.KZKUP AS material_can_be_coproduct,
                    MARA.KZNFM AS material_has_followup_material,
                    MARA.PMATA AS pricing_reference_material,
                    MARA.MSTAE AS cross_plant_material_status,
                    MARA.MSTAV AS cross_distribution_chain_material_status,
                    CAST(MARA.MSTDE AS DATE) AS date_from_cross_plant_material_status_validated, 
                    CAST(MARA.MSTDV AS DATE) AS date_from_cross_distribution_chain_material_status_validated, 
                    MARA.TAKLV AS tax_classification_material,
                    MARA.RBNRM AS catalog_profile,
                    MARA.MHDRZ AS minimum_remaining_shelf_life, 
                    MARA.MHDHB AS total_shelf_life,
                    MARA.MHDLP AS storage_percentage,
                    MARA.INHME AS content_unit,
                    MARA.INHAL AS net_contents,
                    MARA.VPREH AS comparison_price_unit,
                    MARA.INHBR AS gross_contents,
                    MARA.CMETH AS quantity_conversion_method,
                    MARA.CUOBF AS internal_object_number,
                    MARA.KZUMW AS environmentally_relevant,
                    MARA.SPROF AS pricing_profile_for_variants,
                    MARA.MFRPN AS manufacturer_part_number,
                    MARA.MFRNR AS manufacturer_number,
                    MARA.MPROF AS manufacturer_part_profile,
                    MARA.KZWSM AS units_of_measure_usage,
                    MARA.IHIVI AS highly_viscous,
                    MARA.ILOOS AS in_bulk,
                    MARA.KZGVH AS packaging_material_is_closed_packaging,
                    MARA.XGCHP AS approved_batch_record_required,
                    MARA.COMPL AS material_completion_level,
                    MARA.IPRKZ AS period_indicator_shelflife_expiration,
                    MARA.RDMHD AS rounding_rule_for_SLED_calculation,
                    MARA.PRZUS AS product_composition,
                    MARA.MTPOS_MARA AS general_item_category_group,
                    MARA.BFLME AS generic_material_logistical_variants,
                    MARA.MATFI AS material_locked,
                    MARA.CMREL AS relevant_configuration_management,
                    MARA.BBTYP AS assortment_list_type,
                    MARA.SLED_BBD AS expiration_date,
                    MARA.GTIN_VARIANT AS global_trade_item_number_variant,
                    MARA.GENNR AS material_number_generic_materials_in_prepacked_materials, 
                    MARA.RMATP AS reference_material_materials_packed,
                    MARA.GDS_RELEVANT AS global_data_synchronization_relevant,
                    MARA.WEORA AS acceptance_at_origin,
                    MARA.HUTYP_DFLT AS standard_HU_type,
                    MARA.PILFERABLE AS pilferable,
                    MARA.WHSTC AS warehouse_storage_condition,
                    MARA.WHMATGR AS warehouse_material_group,
                    MARA.HNDLCODE AS handling_indicator,
                    MARA.HUTYP AS handling_unit_type,
                    MARA.TARE_VAR AS variable_tare_unit,
                    MARA.MAXC AS max_allowed_capacity_packaging_material,
                    MARA.MAXC_TOL AS overcapacity_tolerance_handling_unit,
                    MARA.MAXL AS max_packaging_length_packaging_material,
                    MARA.MAXB AS max_packaging_width_packaging_material, 
                    MARA.MAXH AS max_packaging_height_packaging_material,
                    MARA.MAXDIM_UOM AS unit_of_measure_max_packaging,
                    MARA.HERKL AS material_country_of_origin,
                    MARA.MFRGR AS  material_freight_group,
                    MARA.QQTIME AS quarantine_period,
                    MARA.QQTIMEUOM AS time_unit_quarantine_period,
                    MARA.QGRP AS quality_inspection_group,
                    MARA.SERIAL AS serial_number_profile,
                    MARA.LOGUNIT AS logistics_unit_of_measure,
                    MARA.CWQREL AS material_catch_weight_material,
                    MARA.ADPROF AS adjustment_profile,
                    MARA.IPMIPPRODUCT AS intellectual_property_id,
                    MARA.ALLOW_PMAT_IGNO AS variant_price_allowed,
                    MARA.MEDIUM AS medium,
                    MARA.COMMODITY AS physical_commodity,
                    MARA.TEXTILE_COMP_IND AS new_textile_composition_function,
                    MARA.ANP AS   ANP_code,
                    MARA."/BEV1/LULEINH" AS loading_units,
                    MARA."/BEV1/LULDEGRP" AS  is_beverage,
                    MARA."/BEV1/NESTRUCCAT" AS structure_category_for_material_relationship,
                    MARA."/DSD/SL_TOLTYP" AS tolerance_typeid,
                    MARA."/DSD/SV_CNT_GRP" AS   counting_group,
                    MARA."/DSD/VC_GROUP" AS DSD_grouping,
                    MARA."/VSO/R_TILT_IND" AS  material_maybe_tilted,
                    MARA."/VSO/R_STACK_IND" AS stacking_not_allowed,
                    MARA."/VSO/R_BOT_IND" AS  bottom_layer,
                    MARA."/VSO/R_TOP_IND" AS top_layer,
                    MARA."/VSO/R_STACK_NO" AS stacking_factor_vso, 
                    MARA."/VSO/R_PAL_IND" AS load_without_packaging_material_vso,
                    MARA."/VSO/R_PAL_OVR_D" AS permissible_overhand_depth_of_packaging_material,
                    MARA."/VSO/R_PAL_OVR_W" AS permissible_overhand_width_of_packaging_material,
                    MARA."/VSO/R_PAL_B_HT" AS permissible_overhand_height__of_packaging_material,
                    MARA."/VSO/R_NO_P_GVH" AS no_of_materials_for_each_closed_packaging_material,
                    MARA."/VSO/R_QUAN_UNIT" AS unit_of_measure_vso,
                    MARA."/VSO/R_KZGVH_IND" AS closed_packaging_material_required,
                    MARA.PACKCODE AS packaging_code,
                    MARA.DG_PACK_STATUS AS dangerous_goods_packaging_status,
                    MARA.MCOND AS material_condition_management,
                    MARA.RETDELC AS return_code,
                    MARA.LOGLEV_RETO AS return_to_logistics_level,
                    MARA.NSNID AS NATO_stock_number,
                    MARA.BSTAT AS craetion_status_seasonal_procurement, 
                    MARA.COLOR_ATINN AS color_number_characterstics,
                    MARA.SIZE1_ATINN AS mainsizes_number_characterstics,
                    MARA.SIZE2_ATINN AS secondsizes_number_characterstics,
                    MARA.COLOR AS characterstic_value_variant_color,
                    MARA.SIZE1 AS characterstic_value_variant_mainsizes,
                    MARA.SIZE2 AS characterstic_value_variant_secondsizes,
                    MARA.FREE_CHAR AS characterstic_value_evluationpurposes,
                    MARA.CARE_CODE AS care_code,
                    MARA.BRAND_ID AS brand,
                    MARA.FASHGRD AS fashion_grade
                    FROM SAPABAP1.MARA''', conn);
     df_MARA = pd.DataFrame(data=MARA)

     # Read data from MVKE
     MVKE   = pd.read_sql(f'''SELECT 
                    MVKE.MANDT AS client_name,
                    MVKE.MATNR AS material_number,
                    MVKE.VKORG AS sales_organization,
                    MVKE.VTWEG AS distribution_channel,
                    MVKE.LVORM AS deletion_flag_material_distribution_chain_level,
                    MVKE.VERSG AS material_statistics_group,
                    MVKE.BONUS AS volume_rebate_group,
                    MVKE.PROVG AS commission_group,
                    MVKE.SKTOF AS cash_discount_indicator,
                    MVKE.VMSTA AS distribution_chain_specific_material_status,
                    CAST(MVKE.VMSTD AS DATE) AS date_from_distribution_chain_specific_material_status_validated, 
                    MVKE.AUMNG AS min_order_quantity,
                    MVKE.LFMNG AS min_delivery_quantity,
                    MVKE.EFMNG AS min_make_to_order_quantity,
                    MVKE.SCMNG AS delivery_unit,
                    MVKE.SCHME AS unit_of_measure_delivery_unit,
                    MVKE.VRKME AS sales_unit,
                    MVKE.MTPOS AS item_category_group,
                    MVKE.DWERK AS delivering_plant,
                    MVKE.PRODH AS MVKE_pricing_reference_material,
                    MVKE.KONDM AS material_pricing_group,
                    MVKE.KTGRM AS account_assignment_group,
                    MVKE.MVGR1 AS material_group1,
                    MVKE.MVGR2 AS material_group2,
                    MVKE.MVGR3 AS material_group3,
                    MVKE.MVGR4 AS material_group4,
                    MVKE.MVGR5 AS material_group5,
                    MVKE.SSTUF AS assotment_grade,
                    MVKE.PFLKS AS external_assortment_priority,
                    CAST(MVKE.LDVFL AS DATE) AS listed_in_store_from_date,
                    CAST(MVKE.LDBFL AS DATE) AS listed_in_store_to_date,
                    CAST(MVKE.LDVZL AS DATE) AS listed_in_distributioncenter_from_date,
                    CAST(MVKE.LDBZL AS DATE) AS listed_in_distributioncenter_to_date,
                    CAST(MVKE.VDVFL AS DATE) AS sold_in_store_from_date,
                    CAST(MVKE.VDBFL AS DATE) AS sold_in_store_to_date,
                    CAST(MVKE.VDVZL AS DATE) AS sold_in_distributioncenter_from_date,
                    CAST(MVKE.VDBZL AS DATE) AS sold_in_distributioncenter_to_date,
                    MVKE.PRAT1 AS product_attribute_id1,
                    MVKE.PRAT2 AS product_attribute_id2,
                    MVKE.PRAT3 AS product_attribute_id3,
                    MVKE.PRAT4 AS product_attribute_id4,
                    MVKE.PRAT5 AS product_attribute_id5,
                    MVKE.PRAT6 AS product_attribute_id6,
                    MVKE.PRAT7 AS product_attribute_id7,
                    MVKE.PRAT8 AS product_attribute_id8,
                    MVKE.PRAT9 AS product_attribute_id9,
                    MVKE.PRATA AS product_attribute_id10,
                    MVKE.RDPRF AS rounding_profile,
                    MVKE.MEGRU AS unit_of_measure_group,
                    MVKE.LFMAX AS max_delivery_quantity_store_order_processing,
                    MVKE.RJART AS rackjobber_material_flag,
                    MVKE.PBIND AS price_fixing_indicator,
                    MVKE.VAVME AS variable_sales_unit_not_allowed,
                    MVKE.MATKC AS  material_completion_characterstics,
                    MVKE.PVMSO AS material_sorting_product_proposal,
                    MVKE."/BEV1/EMLGRP" AS empties_group,
                    MVKE."/BEV1/RPSFA" AS total_category_assignment1,
                    MVKE."/BEV1/RPSKI" AS total_category_assignment2,
                    MVKE."/BEV1/RPSCO" AS total_category_assignment3,
                    MVKE."/BEV1/RPSSO" AS total_category_assignment4,
                    MVKE.PLGTP AS price_band_category
                    FROM SAPABAP1.MVKE''', conn);
     df_MVKE = pd.DataFrame(data=MVKE)

     # Read data from MAKT
     MAKT   = pd.read_sql(f'''SELECT
                    MAKT.MANDT AS client_name,
                    MAKT.MATNR AS material_number, 
                    MAKT.SPRAS AS language_key,
                    MAKT.MAKTX AS material_description,
                    MAKT.MAKTG AS short_material_description
                    FROM SAPABAP1.MAKT''', conn);
     df_MAKT = pd.DataFrame(data=MAKT)

     # Read data from TVM1T
     TVM1T   = pd.read_sql(f'''SELECT 
		    TVM1T.MANDT AS client_name,
                    TVM1T.MVGR1 AS material_group_1,
		    TVM1T.SPRAS AS TVM1T_language_key,
                    TVM1T.BEZEI AS material_pricing_description
                    FROM SAPABAP1.TVM1T''', conn);
     df_TVM1T = pd.DataFrame(data=TVM1T)

     
     #df_MARA['created_at'] = pd.to_datetime('now')         
     #df_MVKE['created_at'] = pd.to_datetime('now')         
     #df_MAKT['created_at'] = pd.to_datetime('now')         
     #df_TVM1T['created_at'] = pd.to_datetime('now')           
     file_path = r'/home/admin2/airflow/dag/Google_cloud'
     GCS_PROJECT = 'data-light-house-prod'
     GCS_BUCKET = 'ibloper'
     storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
     client = storage.Client(project=GCS_PROJECT)
     bucket = client.get_bucket(GCS_BUCKET)
     
     df_join1 = df_MARA.merge(df_MVKE, how='inner', left_on=['CLIENT_NAME','MATERIAL_NUMBER'], right_on=['CLIENT_NAME','MATERIAL_NUMBER'])
     df_join1 = df_join1.query("SALES_ORGANIZATION in ('6100','6300','3300')")
          
     df_join2 = df_join1.merge(df_MAKT, how='inner', left_on=['CLIENT_NAME','MATERIAL_NUMBER'], right_on=['CLIENT_NAME','MATERIAL_NUMBER'])     
     df_join2 = df_join2.query("LANGUAGE_KEY == 'E'")
     
    
     df_join3 = df_join2.merge(df_TVM1T, left_on=['CLIENT_NAME','MATERIAL_GROUP1'], right_on=['CLIENT_NAME','MATERIAL_GROUP_1'],how='left')     
     #df_join3 = df_join3.query("TVM1T_LANGUAGE_KEY == 'E'")
     #print(df_join3.shape)
     df_join3.drop(['MATERIAL_GROUP_1', 'TVM1T_LANGUAGE_KEY'], inplace=True, axis = 1 )
     #print(df_join3.query("MATERIAL_NUMBER == '000000003319008479'"))
     df_join3['date'] = date.today()
     df_join3['CREATED_ON'] = pd.to_datetime(df_join3['CREATED_ON'])
     df_join3['LAST_CHANGE_DATE'] = pd.to_datetime(df_join3['LAST_CHANGE_DATE'])
     #print(today)
     #print(str(today))
     #df_join3 = df_join3.query("CREATED_ON == '"+str(today)+"' or LAST_CHANGE_DATE == '"+str(today)+"'")
     df_join3['created_at'] = datetime.today()
     print(df_join3.query("MATERIAL_NUMBER == '000000003319008479'"))


      
     filename_MARA =f'ibl_MARA_{curr_date}'
     filename_MAKT =f'ibl_MAKT_{curr_date}'
     filename_MVKE =f'ibl_MVKE_{curr_date}'
     filename_TVM1T =f'ibl_TVM1T_{curr_date}'
     filename_products = f'ibl_PRODUCTS_{curr_date}'

     bucket.blob(f'staging/master_tables/Products/{filename_MARA}.csv').upload_from_string(df_MARA.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Products/{filename_MAKT}.csv').upload_from_string(df_MAKT.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Products/{filename_MVKE}.csv').upload_from_string(df_MVKE.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Products/{filename_TVM1T}.csv').upload_from_string(df_TVM1T.to_csv(index=False), 'text/csv')
     bucket.blob(f'staging/master_tables/Products/{filename_products}.csv').upload_from_string(df_join3.to_csv(index=False), 'text/csv')
     
     df_join3.drop(['MAINTENANCE_STATUS_OF_COMPLETED_OBJECT', 'MAINTENANCE_STATUS','OLD_MATERIAL_NUMBER','PURCHASING_VALUE_KEY','LOW_LEVEL_CODE','TRANSPORTATION_GROUP','HAZARDOUS_MATERIAL_NUMBER','SLIPS_QUANTITY_TO_PRINTED','SEASON_CATEGORY','DEACTIVATED','CATEGORY_OF_INTERNATIONAL_ARTICLE_NUMBER','ALLOWED_PACKAGING_WEIGHT','ALLOWED_PACKAGING_VOLUME','VOLUME_UNIT_OF_ALLOWED_PACKAGING_VOLUME','BATCH_MANAGEMENT_REQUIREMENT_INDICATOR','MATERIAL_GROUP_PACKAGING_MATERIALS','AUTHORIZATION_GROUP','SEASON_YEAR','PRICE_BOND_CATEGORY','EMPTIES_BILL_OF_MATERIAL','EXTERNAL_MATERIAL_GROUP','CROSS_PLANT_CONFIGURABLE_MATERIAL','CROSS_PLANT_MATERIAL_STATUS','CROSS_DISTRIBUTION_CHAIN_MATERIAL_STATUS','DATE_FROM_CROSS_PLANT_MATERIAL_STATUS_VALIDATED','DATE_FROM_CROSS_DISTRIBUTION_CHAIN_MATERIAL_STATUS_VALIDATED','MINIMUM_REMAINING_SHELF_LIFE','QUANTITY_CONVERSION_METHOD','INTERNAL_OBJECT_NUMBER','ENVIRONMENTALLY_RELEVANT','PRICING_PROFILE_FOR_VARIANTS','HIGHLY_VISCOUS','PACKAGING_MATERIAL_IS_CLOSED_PACKAGING','APPROVED_BATCH_RECORD_REQUIRED','PERIOD_INDICATOR_SHELFLIFE_EXPIRATION','ROUNDING_RULE_FOR_SLED_CALCULATION','GENERAL_ITEM_CATEGORY_GROUP','GENERIC_MATERIAL_LOGISTICAL_VARIANTS','MATERIAL_LOCKED','RELEVANT_CONFIGURATION_MANAGEMENT','ASSORTMENT_LIST_TYPE','GLOBAL_TRADE_ITEM_NUMBER_VARIANT','MATERIAL_NUMBER_GENERIC_MATERIALS_IN_PREPACKED_MATERIALS','REFERENCE_MATERIAL_MATERIALS_PACKED','GLOBAL_DATA_SYNCHRONIZATION_RELEVANT','ACCEPTANCE_AT_ORIGIN','PILFERABLE','WAREHOUSE_STORAGE_CONDITION','HANDLING_INDICATOR','VARIABLE_TARE_UNIT','MAX_ALLOWED_CAPACITY_PACKAGING_MATERIAL','OVERCAPACITY_TOLERANCE_HANDLING_UNIT','MAX_PACKAGING_LENGTH_PACKAGING_MATERIAL','MAX_PACKAGING_WIDTH_PACKAGING_MATERIAL','MAX_PACKAGING_HEIGHT_PACKAGING_MATERIAL','UNIT_OF_MEASURE_MAX_PACKAGING','QUARANTINE_PERIOD','TIME_UNIT_QUARANTINE_PERIOD','SERIAL_NUMBER_PROFILE','LOGISTICS_UNIT_OF_MEASURE','MATERIAL_CATCH_WEIGHT_MATERIAL','ADJUSTMENT_PROFILE','INTELLECTUAL_PROPERTY_ID','VARIANT_PRICE_ALLOWED','MEDIUM','PHYSICAL_COMMODITY','NEW_TEXTILE_COMPOSITION_FUNCTION','STRUCTURE_CATEGORY_FOR_MATERIAL_RELATIONSHIP','TOLERANCE_TYPEID','COUNTING_GROUP','DSD_GROUPING','MATERIAL_MAYBE_TILTED','STACKING_NOT_ALLOWED','BOTTOM_LAYER','TOP_LAYER','STACKING_FACTOR_VSO','LOAD_WITHOUT_PACKAGING_MATERIAL_VSO','PERMISSIBLE_OVERHAND_DEPTH_OF_PACKAGING_MATERIAL','PERMISSIBLE_OVERHAND_WIDTH_OF_PACKAGING_MATERIAL','PERMISSIBLE_OVERHAND_HEIGHT__OF_PACKAGING_MATERIAL','NO_OF_MATERIALS_FOR_EACH_CLOSED_PACKAGING_MATERIAL','UNIT_OF_MEASURE_VSO','CLOSED_PACKAGING_MATERIAL_REQUIRED','PACKAGING_CODE','DANGEROUS_GOODS_PACKAGING_STATUS','MATERIAL_CONDITION_MANAGEMENT','RETURN_TO_LOGISTICS_LEVEL','NATO_STOCK_NUMBER','CRAETION_STATUS_SEASONAL_PROCUREMENT','COLOR_NUMBER_CHARACTERSTICS','MAINSIZES_NUMBER_CHARACTERSTICS','SECONDSIZES_NUMBER_CHARACTERSTICS','CHARACTERSTIC_VALUE_VARIANT_COLOR','CHARACTERSTIC_VALUE_VARIANT_MAINSIZES','CHARACTERSTIC_VALUE_VARIANT_SECONDSIZES','CHARACTERSTIC_VALUE_EVLUATIONPURPOSES','CARE_CODE','DELETION_FLAG_MATERIAL_DISTRIBUTION_CHAIN_LEVEL','MATERIAL_STATISTICS_GROUP','VOLUME_REBATE_GROUP','COMMISSION_GROUP','CASH_DISCOUNT_INDICATOR','DISTRIBUTION_CHAIN_SPECIFIC_MATERIAL_STATUS','DATE_FROM_DISTRIBUTION_CHAIN_SPECIFIC_MATERIAL_STATUS_VALIDATED','MIN_ORDER_QUANTITY','MIN_DELIVERY_QUANTITY','MIN_MAKE_TO_ORDER_QUANTITY','DELIVERY_UNIT','UNIT_OF_MEASURE_DELIVERY_UNIT','SALES_UNIT','ITEM_CATEGORY_GROUP','DELIVERING_PLANT','MVKE_PRICING_REFERENCE_MATERIAL','MATERIAL_PRICING_GROUP','ACCOUNT_ASSIGNMENT_GROUP','ASSOTMENT_GRADE','EXTERNAL_ASSORTMENT_PRIORITY','LISTED_IN_STORE_FROM_DATE','LISTED_IN_STORE_TO_DATE','LISTED_IN_DISTRIBUTIONCENTER_FROM_DATE','LISTED_IN_DISTRIBUTIONCENTER_TO_DATE','SOLD_IN_STORE_FROM_DATE','SOLD_IN_STORE_TO_DATE','SOLD_IN_DISTRIBUTIONCENTER_FROM_DATE','SOLD_IN_DISTRIBUTIONCENTER_TO_DATE','PRODUCT_ATTRIBUTE_ID1','PRODUCT_ATTRIBUTE_ID2','PRODUCT_ATTRIBUTE_ID3','PRODUCT_ATTRIBUTE_ID4','PRODUCT_ATTRIBUTE_ID5','PRODUCT_ATTRIBUTE_ID6','PRODUCT_ATTRIBUTE_ID7','PRODUCT_ATTRIBUTE_ID8','PRODUCT_ATTRIBUTE_ID9','PRODUCT_ATTRIBUTE_ID10','ROUNDING_PROFILE','UNIT_OF_MEASURE_GROUP','MAX_DELIVERY_QUANTITY_STORE_ORDER_PROCESSING','RACKJOBBER_MATERIAL_FLAG','PRICE_FIXING_INDICATOR','VARIABLE_SALES_UNIT_NOT_ALLOWED','MATERIAL_COMPLETION_CHARACTERSTICS','MATERIAL_SORTING_PRODUCT_PROPOSAL','EMPTIES_GROUP','TOTAL_CATEGORY_ASSIGNMENT1','TOTAL_CATEGORY_ASSIGNMENT2','TOTAL_CATEGORY_ASSIGNMENT3','TOTAL_CATEGORY_ASSIGNMENT4','PRICE_BAND_CATEGORY'], inplace=True, axis = 1)	
     filename_products_t = f'ibl_PRODUCTS_transformed_{curr_date}'
     bucket.blob(f'staging/master_tables/Products/{filename_products_t}.csv').upload_from_string(df_join3.to_csv(index=False), 'text/csv') 

	
     conn.close()

IBL_PRODUCTS_SAP = DAG(
    dag_id='IBL_PRODUCTS_SAP',
    default_args=default_args,
    schedule_interval='0 18 * * *',
    catchup=False,
    #schedule_interval='*/30 * * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='IBL_PRODUCTS_SAP_TO_BQ',
)
 
t0 = PythonOperator(
    task_id="IBL_Products_SAP_To_GCS",
    python_callable=insert_MasterTable_Products_To_GCS,
    dag=IBL_PRODUCTS_SAP
)

t1=     GCSToBigQueryOperator(    
    task_id='IBL_Products_SAP_To_BQ',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Products/ibl_PRODUCTS_transformed_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_PRODUCT',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'client_name', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'created_on', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'person_created_the_object', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'last_change_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'person_changed_the_object', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'client_level_flag_material_deletion', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_type', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'industry_sector', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_group', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'base_unit_of_measure', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'purchase_order_unit_of_measure', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'document_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'document_type', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'document_version', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'document_page_format', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'production_memo', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'dimensions', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'basic_material', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'mapping_code', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'laboratory_office', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'gross_weight', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'net_weight', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'weight_unit', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'volume', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'volume_unit', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'container_requirements', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'storage_conditions', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'temperature_conditions_indicator', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'division', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'competitor', 'type': 'STRING', 'mode': 'NULLABLE'} , 
    {'name': 'supply_source', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'label_type', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'label_form', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'international_article_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'length', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'width', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'height', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'dimension_unit', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'product_hierarchy', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'stock_transfer_net_changing_cost', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'unit_of_weight', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'variable_purchase_order_unit_active', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'configurable_material', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'packaging_material_type', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'max_level_by_volume', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'stacking_factor', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'valid_from_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'deletion_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'material_category', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_can_be_coproduct', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_has_followup_material', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'pricing_reference_material', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'tax_classification_material', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'catalog_profile', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'total_shelf_life', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'storage_percentage', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'content_unit', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'net_contents', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'comparison_price_unit', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'gross_contents', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'manufacturer_part_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'manufacturer_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'manufacturer_part_profile', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'units_of_measure_usage', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'in_bulk', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_completion_level', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'product_composition', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'expiration_date', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'standard_HU_type', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'warehouse_material_group', 'type': 'STRING', 'mode': 'NULLABLE'} , 
    {'name': 'handling_unit_type', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_country_of_origin', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_freight_group', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'quality_inspection_group', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'ANP_code', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'loading_units', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'is_beverage', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'return_code', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'fashion_grade', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'sales_organization', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'distribution_channel', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_group1', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_group2', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_group3', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_group4', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_group5', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'language_key', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_description', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'short_material_description', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'material_pricing_description', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}         
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=IBL_PRODUCTS_SAP
)

t2 = BigQueryOperator(
        task_id="merge_staging_products_to_ibl_products",
        bigquery_conn_id='bigquery',    
	use_legacy_sql=False,
        sql='''    
	    #standardSQL	
	    merge  `data-light-house-prod.EDW.IBL_PRODUCTS` a
        using  `data-light-house-prod.EDW.IBL_PRODUCT` b 
        on a.material_number=b.material_number and a.sales_organization=b.sales_organization
        WHEN MATCHED THEN
        update set a.last_change_date=b.last_change_date,
           a.person_changed_the_object=b.person_changed_the_object,
           a.material_type=b.material_type,
           a.industry_sector=b.industry_sector,
           a.material_group=b.material_group,
           a.base_unit_of_measure=b.base_unit_of_measure,
           a.material_description	=b.material_description,
           a.short_material_description=b.short_material_description,
           a.material_pricing_description=b.material_pricing_description,
           a.date=b.date,
           a.created_at=b.created_at
        when not matched then
        INSERT (client_name,material_number,created_on,person_created_the_object,last_change_date,person_changed_the_object,client_level_flag_material_deletion,material_type,industry_sector,material_group,base_unit_of_measure,purchase_order_unit_of_measure,document_number,document_type,document_version,document_page_format,production_memo,dimensions,basic_material,mapping_code,laboratory_office,gross_weight,net_weight,weight_unit,volume,volume_unit,container_requirements,storage_conditions,temperature_conditions_indicator,division,competitor,supply_source,label_type,label_form,international_article_number,length,width,height,dimension_unit,product_hierarchy,stock_transfer_net_changing_cost,unit_of_weight,variable_purchase_order_unit_active,configurable_material,packaging_material_type,max_level_by_volume,stacking_factor,valid_from_date,deletion_date,material_category,material_can_be_coproduct,material_has_followup_material,pricing_reference_material,tax_classification_material,catalog_profile,total_shelf_life,storage_percentage,content_unit,net_contents,comparison_price_unit,gross_contents,manufacturer_part_number,manufacturer_number,manufacturer_part_profile,units_of_measure_usage,in_bulk,material_completion_level,product_composition,expiration_date,standard_HU_type,warehouse_material_group,handling_unit_type,material_country_of_origin,material_freight_group,quality_inspection_group,ANP_code,loading_units,is_beverage,return_code,brand,fashion_grade,sales_organization,distribution_channel,material_group1,material_group2,material_group3,material_group4,material_group5,language_key,material_description,short_material_description,material_pricing_description,date,created_at) 
        VALUES 
               (client_name,material_number,created_on,person_created_the_object,last_change_date,person_changed_the_object,client_level_flag_material_deletion,material_type,industry_sector,material_group,base_unit_of_measure,purchase_order_unit_of_measure,document_number,document_type,document_version,document_page_format,production_memo,dimensions,basic_material,mapping_code,laboratory_office,gross_weight,net_weight,weight_unit,volume,volume_unit,container_requirements,storage_conditions,temperature_conditions_indicator,division,competitor,supply_source,label_type,label_form,international_article_number,length,width,height,dimension_unit,product_hierarchy,stock_transfer_net_changing_cost,unit_of_weight,variable_purchase_order_unit_active,configurable_material,packaging_material_type,max_level_by_volume,stacking_factor,valid_from_date,deletion_date,material_category,material_can_be_coproduct,material_has_followup_material,pricing_reference_material,tax_classification_material,catalog_profile,total_shelf_life,storage_percentage,content_unit,net_contents,comparison_price_unit,gross_contents,manufacturer_part_number,manufacturer_number,manufacturer_part_profile,units_of_measure_usage,in_bulk,material_completion_level,product_composition,expiration_date,standard_HU_type,warehouse_material_group,handling_unit_type,material_country_of_origin,material_freight_group,quality_inspection_group,ANP_code,loading_units,is_beverage,return_code,brand,fashion_grade,sales_organization,distribution_channel,material_group1,material_group2,material_group3,material_group4,material_group5,language_key,material_description,short_material_description,material_pricing_description,date,created_at) 
	    ''',
        dag=IBL_PRODUCTS_SAP

)

t0 >> t1 >> t2