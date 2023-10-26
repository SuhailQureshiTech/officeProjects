import connectionClass
import pandas as pds
from sqlalchemy import create_engine
import sqlalchemy


connClass = connectionClass
sqlServerConn = connClass.markittSqlServer()
sapConn = connClass.sapConn()

def get_markitt_POS_header_data():
    global df_header
    # print('max date : ', maxDate)
    tablePOS = 'Markitt_POSDetailListTAB'
    # nan = np.nan
    # print('data date : ', dateDate)
    # print('fromSapDate : ',fromSapDate)
    # print('toSapDate : ', toSapDate)

    conn = sqlServerConn
    query_header = f'''SELECT BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo, SerialNo,
        upper(TRIM(BarCode)) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount,
        PaymentMode FROM [Markitt2021-2022].dbo.Markitt_POSDetailListTAB
        where cast(billdate as date) between '2023-07-16' and '2023-07-31'
        '''
        #  {maxDate} and {dateDate}

    query_return = f'''SELECT BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo, SerialNo,
        upper(TRIM(BarCode)) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount, PaymentMode,
        RecordNo FROM[Markitt2021-2022].dbo.Markitt_SRTDetailListTAB
        where cast(billdate as date) between '2023-07-16' and '2023-07-31'
        '''
        #  {maxDate} and {dateDate}
    df_header = pds.read_sql(query_header, conn)
    df_header.columns = df_header.columns.str.strip()
    df_header['BarCode'] = df_header['BarCode'].str.upper()
    df_header.insert(0, 'MANDT', '300')

    df_return = pds.read_sql(query_return, conn)
    df_return.columns = df_return.columns.str.strip()
    df_return.insert(0, 'MANDT', '300')
    df_return['FLAG'] = 'R'

    data_concat = pds.concat([df_header, df_return],             # Append two pandas DataFrames
                            ignore_index=True,
                            sort=False)
   
    # print(data_concat.info())

    data_concat = data_concat.rename(columns={
        'MANDT': 'MANDT', 'BillNo': 'BILLNO', 'SerialNo': 'SERIALNO', 'BranchCode': 'BRANCH', 'BillDate': 'BILLDATE', 'BarCode': 'BARCODE', 'Quantity': 'QUANTITY', 'Rate': 'RATE', 'Amount': 'AMOUNT', 'DiscountAmount': 'ITEMDISCOUNTAMOUNT', 'NetAmount': 'NETAMOUNT', 'ItemGSTAmount': 'ITEMGSTAMOUNT', 'PaymentMode': 'ZMODE', 'BillDate': 'BILLDATE', 'BarCode': 'BARCODE', 'Quantity': 'QUANTITY', 'FLAG': 'FLAG'
                                        })

    rearrange_columns = ['MANDT', 'BILLNO', 'SERIALNO', 'BRANCH', 'BILLDATE', 'QUANTITY', 'RATE', 'AMOUNT'
                        ,'ITEMDISCOUNTAMOUNT', 'NETAMOUNT', 'ITEMGSTAMOUNT', 'ZMODE', 'FLAG','BARCODE']

    data_concat = data_concat.reindex(columns=rearrange_columns)


    hdb_user = 'ETL'
    hdb_password = 'Etl@2025'
    hdb_host = '10.210.134.204'
    hdb_port = '33015'
    connection_string ='hana://%s:%s@%s:%s/?encrypt=false&sslvalidatecertificate=false' % (
        hdb_user, hdb_password, hdb_host, hdb_port)


    # connection_string = connClass.sapConnAlchemy()
    # hana+hdbcli://<user>:<password>@<example.com>/<my-database>?encrypt=true
    # connection_string='hana+hdbcli://ETL:Etl@2025@10.210.134.204/<my-database>?encrypt=true'
    engine1 = sqlalchemy.create_engine(connection_string)
    print('engine : ', engine1)
    
    # print(data_concat)


    # delRecQuery = f'''
    #             delete from ZMARKIT_ITEM where cast(billdate as date) between '2023-07-16' and '2023-07-31'
    #         '''
    #         #   {maxDate} and {dateDate}
    # engine1.execute(delRecQuery)

    data_concat.to_sql('ZMARKIT_ITEM',
                    schema='ETL',
                    con=engine1,
                    index=False,
                    if_exists='append'
                    )

    # updateMaterial = f''' UPDATE etl.ZMARKIT_ITEM zi
    #                         SET MATERIAL=(SELECT MATNR  FROM MARKITT_ITEMS  WHERE NORMT = zi.barcode)
    #                         WHERE CAST(BILLDATE  AS date) between  '2023-07-16' and '2023-07-31'
    #         '''
    #             #   {maxDate} and {dateDate}

    # engine1.execute(updateMaterial)

    # insertZmarkittItems=f''' INSERT INTO SAPABAP1.ZMARKIT_ITEM  (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG)
    #         SELECT MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,COALESCE(FLAG,' ')
    #         FROM ZMARKIT_ITEM zi WHERE  1=1   AND cast(BILLDATE as date) between  '2023-07-16' and '2023-07-31'
    # '''
    #         #  {maxDate} and {dateDate}

    # insertZmarkitt = f''' INSERT INTO SAPABAP1.ZMARKIT(MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG)
    #             select * from ( SELECT 300, BillNo, SerialNo, Branch, BillDate, TRIM(MATERIAL) AS material, Quantity	, Rate, Amount
	# 			,ITEMDISCOUNTAMOUNT, NetAmount, ItemGSTAmount
    #             ,ZMODE, row_number() over (partition by BillNo order by BillDate) as row_number
    #             from ZMARKIT_ITEM zi
    #             WHERE 1=1 AND cast(billdate as date) between   '2023-07-16' and '2023-07-31'
    #             ) as rows
    #                 where row_number = 1
    #             '''
                #  {maxDate} and {dateDate}

    # engine1.execute(insertZmarkittItems)


get_markitt_POS_header_data()