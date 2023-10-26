from matplotlib.pyplot import title
import shopify
from json import dumps

API_KEY = '79e782797963dfd783b6797f12a3482d'
PASSWORD = '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'
SHOP_NAME = 'orasyscojava'
API_VERSION = '2022-01'
shop_url = "https://%s.myshopify.com/admin" % (SHOP_NAME)
shopify.ShopifyResource.set_user(API_KEY)
shopify.ShopifyResource.set_password(PASSWORD)

shopify.ShopifyResource.set_site(shop_url)

new_customer = shopify.Customer()
new_customer.first_name = "kunwara"
new_customer.last_name = "cepeda"
new_customer.addresses = [
    {"address1": "123 Oak st",
                        "city": "Ottawa", "phone": "9876543210", "company": "Apple"},
    {"address2": "999 Saddar",
                        "city": "Ottawa", "phone": "9876543210", "company": "Apple"}

                        ]
new_customer.default_address = {
    "address1": "456 Oak st", "city": "Ottawa", "phone": "9876543210", "company": "Apple"
    }
new_customer.save()


# res = shopify.Customer.post('{}/addresses'.format(5578379395132), body=dumps({
#     "address": {
#                             "address1": "1 Rue des Carrieres",
#                             "address2": "Suite 1234",
#                             "city": "Montreal",
#                             "company": "Fancy Co.",
#                             "first_name": "Samuel",
#                             "last_name": "de Champlain",
#                             "phone": "819-555-5555",
#                             "province": "Quebec",
#                             "country": "Canada",
#                             "zip": "G1R 4P5",
#                             "name": "Samuel de Champlain",
#                             "province_code": "QC",
#                             "country_code": "CA",
#                             "country_name": "Canada"
#                             }
# }).encode('utf-8'))
# print(res)
