from matplotlib.pyplot import title
import shopify
import json
import requests

API_KEY = 'a8e17787e2074ce995b75465022349de'
PASSWORD = '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'
SHOP_NAME = 'habitt-store'
API_VERSION = '2023-04'

shop_url = f'''https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json
'''
shopify.ShopifyResource.set_user(API_KEY)
shopify.ShopifyResource.set_password(PASSWORD)

shopify.ShopifyResource.set_site(shop_url)


def get_products():
    """
    Returns a list of all products in form of response JSON
    from Shopify API endpoint connected to storefront.

    * Note: Shopify API allows 250 pruducts per call.

    :return:
        product_list (list):    List containing all product response
                                JSONs from Shopify API.
    """

    products = []
    is_remaining = True
    i = 1
    while is_remaining:

        if i == 1:
            params = {
                "limit": 250,
                "page": i
            }

            response = requests.get(
                'https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json')

            products.append(response.json()['products'])
            i += 1

        elif len(products[i-2]) % 250 == 0:
            params = {
                "limit": 250,
                "page": i
            }

            response = requests.get('https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json')

            products.append(response.json()['products'])
            i += 1

        else:
            is_remaining = False

    products = [products[i][j]
                for i in range(0, len(products))
                for j in range(0, len(products[i]))
                ]

    return products

products=get_products()
# products
# print(products)
jj=json.dumps(products)
jsonFile = open("data.json", "w")
jsonFile.write(jj)
jsonFile.close()
# print(jj)
