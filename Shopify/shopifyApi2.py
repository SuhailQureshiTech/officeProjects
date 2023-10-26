import pandas as pd
import numpy as np
import re
import requests

# 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     Admin API access token
# a8e17787e2074ce995b75465022349de           API key
# fed65f219a44c6c36c367b5a3381cf0f           API Secret Key

API_KEY = 'a8e17787e2074ce995b75465022349de'
API_SECRET = 'fed65f219a44c6c36c367b5a3381cf0f'
ACCESS_TOKEN = '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'
shop_url = "habitt-store.myshopify.com"
STORE_NAME='habitt-store'
api_version = '2023-04'


def get_all_orders():
    last = 0
    products = pd.DataFrame()
    # urlPath=f'''

    # '''
    while True:
        url =f"https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json?limit=250&since_id={last}"

        response = requests.request("GET", url)
        df = pd.DataFrame(response.json()['products'])
        products = pd.concat([products, df])
        last = df['id'].iloc[-1]
        if len(df) < 250:
            break
    return(products)


df = get_all_orders()
print(df.info())
# print(len(df['products']))


df.to_csv('habittshopify.csv')
# print(df['id'])
# print(df.loc[1,:])
