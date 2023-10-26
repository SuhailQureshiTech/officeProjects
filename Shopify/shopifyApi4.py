import pandas as pd
import numpy as np
import re
import requests
import shopify
import json

API_KEY = 'a8e17787e2074ce995b75465022349de'
API_SECRET = 'fed65f219a44c6c36c367b5a3381cf0f'
ACCESS_TOKEN = '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'
STORE='habitt-store'
shop_url = "habitt-store.myshopify.com"
api_version = '2023-04'
product=None
productsJSON=[]
my_json=json

def get_all_products(limit=250):
    shop_url = "https://{}.myshopify.com/admin/api/{}".format(
    STORE, api_version)
    shopify.ShopifyResource.set_site(shop_url)
    shopify.ShopifyResource.set_user(API_KEY)
    shopify.ShopifyResource.set_password(ACCESS_TOKEN)

    get_next_page = True
    since_id = 0
    while get_next_page:
        products = shopify.Product.find(since_id=since_id, limit=limit)

        for product in products:
            yield product
            since_id = product.id

        if len(products) < limit:
            get_next_page = False


def get_product(id):
    shop_url = "https://{}.myshopify.com/admin/api/{}".format(
        STORE, api_version)
    shopify.ShopifyResource.set_site(shop_url)
    shopify.ShopifyResource.set_user(API_KEY)
    shopify.ShopifyResource.set_password(ACCESS_TOKEN)

    get_next_page = True
    since_id = 0
    products = shopify.Product.find()
    productsJSON =None

    for product in products:
        productsJSON.append(product.to_dict())

    df=pd.DataFrame(productsJSON)
    print(df)




# end method


# def main():
#     shop_url = "https://{}.myshopify.com/admin/api/{}".format(
#         STORE, api_version)
#     shopify.ShopifyResource.set_site(shop_url)
#     shopify.ShopifyResource.set_user(API_KEY)
#     shopify.ShopifyResource.set_password(ACCESS_TOKEN)

#     for product in get_all_products():
#         # Do something with product
#         print(product.title)


# if __name__ == "__main__":
#     main()

# get_all_products()

# id=[]
# title=[]
# for product in get_all_products():
#         # Do something with product
#     # print(product.title)
#     id.append(product.id)
#     title.append(product.title)

# df=pd.DataFrame()
# df['id']=np.array(id)
# df['title']=np.array(title)
# df.to_excel('habittProductList.xlsx')

# get_product(5460017381537)


# for product in get_all_products():
#     productsJSON.append(product.to_dict())

# for product1 in product:
#     productsJSON.append(product.to_dict())

# df = pd.DataFrame(product)
# print(df)

def get_product3():
    shop_url = "https://{}.myshopify.com/admin/api/{}".format(
        STORE, api_version)
    shopify.ShopifyResource.set_site(shop_url)
    shopify.ShopifyResource.set_user(API_KEY)
    shopify.ShopifyResource.set_password(ACCESS_TOKEN)

    get_next_page = True
    since_id = 0
    # productsJSON1=[]
    global productsJSON, my_json
    while get_next_page:
        products = shopify.Product.find(since_id=since_id, limit=250)
        for product in products:
            since_id = product.id
            productsJSON.append(product.to_dict())
            # productsJSON1.append(product)
            # print(productsJSON1)

        if len(products) < 250:
            get_next_page = False

    # productsJSON=productsJSON1
    # my_json = json.loads(productsJSON1)
    # my_json = json.dumps(productsJSON1)
    # df_products_shopify2 = pd.DataFrame.from_dict(my_json['products'])


    # print(products)

get_product3()
print('type')
print(type(productsJSON))
# df=pd.DataFrame(my_json)
# print(df)

