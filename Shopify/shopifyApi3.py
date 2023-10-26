import pandas as pd
import numpy as np
import re
import requests
import shopify

API_KEY = 'a8e17787e2074ce995b75465022349de'
API_SECRET = 'fed65f219a44c6c36c367b5a3381cf0f'
ACCESS_TOKEN = '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'
shop_url = "habitt-store.myshopify.com"
api_version = '2023-04'


shopify.Session.setup(api_key=API_KEY, secret=API_SECRET)
newSession = shopify.Session(shop_url, api_version)
print(newSession)
session = shopify.Session(shop_url, api_version, ACCESS_TOKEN)
shopify.ShopifyResource.activate_session(session)

shop = shopify.Shop.current()  # Get the current shop
products = shopify.product.find()
# print(type(products))
