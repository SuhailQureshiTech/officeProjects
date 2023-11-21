import string
import re
import connectionClass
import pandas as pds
# storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
# storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB
sapCon=connectionClass
conn1=sapCon.sapConn()
specChars=connectionClass.getSpecChars()

spec_chars=connectionClass.getSpecChars()
def generateItemsData():
    print('in........')
    global fileName
    global fran_sale_df

    # GCS_PROJECT = V_GCS_PROJECT
    # GCS_BUCKET = V_GCS_BUCKET

    # storage_client = storage.Client.from_service_account_json(
    #     r'/home/airflow/airflow/data-light-house-prod.json')
    
    # client = storage.Client(project=GCS_PROJECT)
    # bucket = client.get_bucket(GCS_BUCKET)


    # SELECT
    # MANDT,MATNR,MATNR_DESC,MAPPING_CODE,COMPANY,BUSLINE_ID,BUSLINE_DESC
    # FROM ETL.MATNR_WITH_COMPANY_BUSLIEN

    df = pds.read_sql(f'''
                        SELECT
                        MANDT,MATNR,MATNR_DESC,MAPPING_CODE,COMPANY
                        FROM ETL.MATNR_WITH_COMPANY_BUSLIEN
                        ''', conn1)
    print(df.info())
    df.columns = df.columns.str.strip()
    df.replace(',', '', regex=True, inplace=True)
    # df['transfer_date'] = creationDate
    fileName = 'SAP_ITEMS_DATA.csv'

    for char in spec_chars:
        df['MATNR_DESC'] = df['MATNR_DESC'].str.replace(char,' ',regex=True)
        df['MATNR_DESC'] = df['MATNR_DESC'].str.split().str.join(" ")

    # bucket.blob(f'''staging/temp/MasterData/{fileName}''').upload_from_string(
    #     df.to_csv(index=False), 'text/csv')
    df.to_csv('item.csv')
    print(df)

generateItemsData()