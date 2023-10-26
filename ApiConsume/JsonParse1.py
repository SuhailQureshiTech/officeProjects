import pandas as pd
import requests

# url ='https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json?ids=7533862617306'
# df=pd.read_json(url)
# print(df)

url = 'https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json?ids=7533862617306'
r=requests.get(url)
data=r.json()
df=pd.DataFrame(data)
df.to_json('habitt.json')
print(df)
