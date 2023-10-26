import shopify
# from shopify import session

# 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     Admin API access token
# a8e17787e2074ce995b75465022349de           API key
# fed65f219a44c6c36c367b5a3381cf0f           API Secret Key

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
product = shopify.Product.find()  # Get a specific product

print(product.title)
# print(product.title)
# print(product.variants['price'])
# for r in product.variants:
#     print('r' )
#     print(r)
