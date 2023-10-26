
import pandas as pd
import numpy as np
import re
import requests

API_KEY = 'a8e17787e2074ce995b75465022349de'
PASSWORD = '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'
SHOP_NAME = 'habitt-store'
API_VERSION = '2023-04'
shop_url = "https://%s.myshopify.com/admin" % (SHOP_NAME)
STORE='habitt-store'

def get_all_orders():
    last = 0
    orders = pd.DataFrame()
    while True:
        url = f"https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json?limit=250&fulfillment_status=unfulfilled&since_id={last}"
        response = requests.request("GET", url)

        df = pd.DataFrame(response.json()['products'])
        orders = pd.concat([orders, df])
        last = df['id'].iloc[-1]
        if len(df) < 250:
            break
    return(orders)

# 5460017381537
def get_product():
    last = 0
    products = pd.DataFrame()
    url = f"https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json?ids=5460017381537"
    response = requests.request("GET", url)

    df = pd.DataFrame(response.json()['products'])
    products = pd.concat([products, df])

    return(products)



# df = get_all_orders()
# print(df.head())
# df.to_csv('suhaillllll.csv')

df=get_product()
# print(df.info())
# print(df['variants'])
# print(df.head())
df.to_json('suhailJson.json')
