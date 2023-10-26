import requests as req
import pandas as pd
from pandas import json_normalize
import json

api_url = 'https://7467a2f820d5299de56290ae1ffc5dc0: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@craftt-culture.myshopify.com/admin/api/2022-01/customers.json'
response = req.get(api_url)
r = response.json()

df2=json_normalize(r['customers'])
df2.to_csv("D:\\TEMP\\CUSTOMERSAPI.CSV")
# print(df2)


# df2 = pd.DataFrame.from_dict(r, orient="index")
# print(df2)
