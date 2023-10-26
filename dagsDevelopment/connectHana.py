from pickle import TRUE
import platform
from hdbcli import dbapi
import pandas as pd
from google.cloud import storage
import os
from google.oauth2 import service_account

#verify the architecture of Python
print ("Platform architecture: " + platform.architecture()[0])

#Initialize your connection
conn = dbapi.connect(

    # Option 1, retrieve the connection parameters from the hdbuserstore
    # key='USER1UserKey', # address, port, user and password are retrieved from the hdbuserstore

    # Option2, specify the connection parameters
    address='10.210.134.204',
    port='33015',
    user='Etl',
    password='Etl@2025'

    # Additional parameters
    # encrypt=True, # must be set to True when connecting to HANA as a Service
    # As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud)
    # sslValidateCertificate=False #Must be set to false when connecting
    # to an SAP HANA, express edition instance that uses a self-signed certificate.
)


Location_df = pd.read_sql(f'''
                        SELECT
                        MARA.MANDT,MARA.MATNR,MAKT.MAKTX
                        FROM SAPABAP1.MARA ,SAPABAP1.MAKT
                        WHERE 1=1 AND MARA.MANDT=300 AND MAKT.MANDT=MARA.MANDT AND MAKT.MATNR=MARA.MATNR AND MAKT.SPRAS='E'
                                                                    ''', conn)

Location_df["MAKTX"] = Location_df["MAKTX"].str.replace(
    r"[\" ]+", " ").str.strip()

# Location_df.to_csv('D:\\TEMP\\TESTO.CSV')

print(Location_df)
