
import shopify
from json import dumps


API_KEY = 'a8e17787e2074ce995b75465022349de'
PASSWORD = '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'
SHOP_NAME = 'habitt-store'
API_VERSION = '2023-04'
shop_url = "https://%s.myshopify.com/admin" % (SHOP_NAME)

token=PASSWORD
merchant=API_KEY


# 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     Admin API access token
# a8e17787e2074ce995b75465022349de           API key
# fed65f219a44c6c36c367b5a3381cf0f           API Secret Key


api_session=shopify.Session('habitt-store','2023-04',PASSWORD)
shopify.ShopifyResource.activate_session(api_session)
# print(api_session)

def getData(object_name):
    all_data=[]
    attribute=getattr(shopify,object_name)
    data=attribute.find(since_id=0,limit=250)
    for d in data:
        all_data.append(d)
    while data.has_next_page():
        data=data.next_page()
        for d in data:
            all_data.append(d)

    return all_data


products=getData('Product')
# print(products[0].attributes)

print(products['variants'].attributes)

# print(getattr(products,['variants']))


## print(products['variants'])
# print(list(products.keys()))
# print(products)


