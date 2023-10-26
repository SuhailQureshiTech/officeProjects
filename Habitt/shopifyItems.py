import pandas as pd
import sys
import sqlalchemy as sa
from sqlalchemy import create_engine
import urllib

# sys.path.append("/Habitt")

df,df2=pd.DataFrame(),pd.DataFrame()

def readShopifyItems():
    global df,df2
    df = pd.read_csv('shopifyHabittitems.csv', encoding='latin-1')
    # df['body_html'] = df['body_html'].str.replace('\W', '', regex=True)

    # df["body_html"].fillna("NaN", inplace=True)
    # df['body_html'] = df['body_html'].str.replace("<p>", "")
    # df['body_html'] = df['body_html'].str.replace("<h2>", "")
    # df['body_html'] = df['body_html'].str.replace("</p>", "")
    # df['body_html'] = df['body_html'].str.replace("</h2>", "")
    # df['body_html'] = df['body_html'].str.replace("<br>", "")
    # df['body_html'] = df['body_html'].str.replace("</br>", "")

    # # <li>
    # df['body_html'] = df['body_html'].str.replace("<li>", "")
    # df['body_html'] = df['body_html'].str.replace("</li>", "")

    # df2['body_html'] = df['body_html']

    df2[
        ['Product_Code', 'sku', 'barcode',
            'Inventory_Item_id', 'weight_unit', 'Product_Type', 'Product_Desc', 'status', 'published_at', 'published'
            , 'price', 'compare_at_price', 'Vendor', 'body_html'
            , 'Inventory_Quantity', 'taxable' , 'Product_Image_Url'
            ]
        ] = df[
        ['Product_Code', 'sku', 'barcode', 'Inventory_Item_id', 'weight_unit', 'Product_Type', 'Product_Desc'
         , 'status', 'published_at', 'published', 'price', 'compare_at_price', 'Vendor', 'body_html'
         , 'Inventory_Quantity', 'taxable', 'Product_Image_Url'
                ]
            ]

    # df2 = df['sku'].astype('string')
    # df2 = df['barcode'].astype('string')
    # df2 = df['Inventory_Item_id'].astype('string')



readShopifyItems()

print(df.info())
# df2.to_excel('suhailTestItems.xlsx')

# print(df.info());

def insertShopifyItems():
    global df,df2
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                "SERVER=192.168.130.81;"
                                "DATABASE=habittTarzDB;"
                                "UID=pbironew;"
                                "PWD=pbiro1234_456")

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

    df2.to_sql('ShopifyItems',
                    schema='dbo',
                    con=engine,
                    index=False,
                    if_exists='append'
            )

insertShopifyItems()
