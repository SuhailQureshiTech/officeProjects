#Import


from flatten_json import flatten
# import json
import pandas as pd
from pandas.io.json import json_normalize
import requests as req
import json
import requests
import numpy as np
import regex as re

# Import -- End

api_url = 'https://a8e17787e2074ce995b75465022349de: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@habitt-store.myshopify.com/admin/api/2023-04/products.json'

# working block -- backup
# response = req.get(url=api_url)
# data1=response.json()

# df=pd.json_normalize(data1,record_path=['products'])
# df.to_csv('habitt_items.csv')
# df = pd.json_normalize(data=data1['products'], record_path=['variants'], meta=['id', 'title'], record_prefix='_')
# df.to_csv('habitt_items1.csv')

# df = pd.json_normalize(data=data1['products'], max_level=2, record_prefix='_')
# df.to_csv('habitt_items2.csv')

payload = ""
headers = {
    'Cookie': '_master_udr=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaEpJaWs0TkRnMVlqVTNPUzAyTUdVeUxUUmlabU10T1dZNU9TMWlZbVE0TmpJd1pEUXpOVElHT2daRlJnPT0iLCJleHAiOiIyMDI0LTA3LTIyVDA5OjMyOjU2Ljc0M1oiLCJwdXIiOiJjb29raWUuX21hc3Rlcl91ZHIifX0%3D--02a70f3d3e49c5ed101b1ba9f1cddc7d3fca9be5; _secure_admin_session_id=3bee420534f8a94f4bae452d83ca6e21; _secure_admin_session_id_csrf=3bee420534f8a94f4bae452d83ca6e21; _landing_page=%2F; _orig_referrer=; _shopify_y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; _y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; cart=f3eb468c2860b7280b73c773081878cc; cart_sig=7bbc5f066eacf57aad2ad40c36b54299; cart_ts=1658483398; cart_ver=gcp-us-central1%3A1; identity-state=BAhbCkkiJTM3M2ZkYmFjMTBmMTVlM2UzNzY5YzZmZTBlNzAyMTRmBjoGRUZJIiUxOGZlMWFiMWZjMTFiOWNlOTE2Yjc2YmE5MzQxMzkwMwY7AEZJIiUwZTc4MjQzMTI2MmU2YTc2ZDY3MDAwYzJjOWM3MjYxNQY7AEZJIiVhM2E2YTA2YWRjYjE0NWMyNGQwYzZjOTYzODI3MGFiMgY7AEZJIiViZWEyNGY3ZTQwOThiZDdiZTdiYmJmM2I4NDc3ZjZhZAY7AEY%3D--25bb4197b0669f65438b1a778cb0e46457623657; identity-state-0e782431262e6a76d67000c2c9c72615=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1OTE3NjcxOS4xNTE2Mjc4SSIKbm9uY2UGOwBUSSIlZmRlNWIxNjgwOTA4NmI5OGVkMDA5OTdiZGYyNjVmZGIGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--ffc86af6f9dac44773160b94c756a1bad0c359b1; identity-state-18fe1ab1fc11b9ce916b76ba93413903=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1OTE3NjY3MS44OTE2NzQ1SSIKbm9uY2UGOwBUSSIlNmMwMzgxNTY2ZmJmZTg4OTZkNmMzMTE4ZWY3NWVlMDMGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--662a09720035d833e95925cb7eee5395dea12917; identity-state-373fdbac10f15e3e3769c6fe0e70214f=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYWMTY1OTE3NjY2NS45NTg1OTFJIgpub25jZQY7AFRJIiU3ZjZiMjVhNmUyMDEwOGJmYzRjMWMwNDk3YTY3NDk0YgY7AEZJIgpzY29wZQY7AFRbD0kiCmVtYWlsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZGVzdGluYXRpb25zLnJlYWRvbmx5BjsAVEkiC29wZW5pZAY7AFRJIgxwcm9maWxlBjsAVEkiTmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvcGFydG5lcnMuY29sbGFib3JhdG9yLXJlbGF0aW9uc2hpcHMucmVhZG9ubHkGOwBUSSIwaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9iYW5raW5nLm1hbmFnZQY7AFRJIkJodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL21lcmNoYW50LXNldHVwLWRhc2hib2FyZC5ncmFwaHFsBjsAVEkiPGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvc2hvcGlmeS1jaGF0LmFkbWluLmdyYXBocWwGOwBUSSI3aHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9mbG93LndvcmtmbG93cy5tYW5hZ2UGOwBUSSI%2BaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9vcmdhbml6YXRpb24taWRlbnRpdHkubWFuYWdlBjsAVEkiD2NvbmZpZy1rZXkGOwBUSSIMZGVmYXVsdAY7AFQ%3D--7fa3a2bc321672832aafed93ff92705d5b6fc8b5; identity-state-a3a6a06adcb145c24d0c6c9638270ab2=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1OTE3NzQwNC42NDIxNTY0SSIKbm9uY2UGOwBUSSIlNzA4ODc0M2VkMDNkZWMxMDc4M2E3ZDQwYmFlMTRkMmMGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--a177c8043441d93f9763fdacaad67b9d40774397; identity-state-bea24f7e4098bd7be7bbbf3b8477f6ad=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1OTE3NzQxMi40MjU4OTE2SSIKbm9uY2UGOwBUSSIlZmFiZjI4NTU5NTEwNWU2NjkyNDFhNGViY2YxZWViOTQGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--ab80324275015a11a6bfca9b46168dd4f7be371e; localization=PK; secure_customer_sig='
}
response = requests.request("GET", api_url, headers=headers, data=payload)
barcodes = []
price=[]
vImages=[]
vImages1=[]
vSku=[]
data = response.content
my_json = json.loads(data)
df_products_shopify2 = pd.DataFrame.from_dict(my_json['products'])

spec_chars = ["!", '"', "#", "%", "&", "'", "(", ")",
                "*", "+", ",", "-", ".", "/", ":", ";", "<",
                "=", ">", "?", "@", "[", "\\", "]", "^", "_",
                "`", "{", "|", "}", "~", "–"]


for i in range(0, len(df_products_shopify2['variants'])):
    barcodes.append(df_products_shopify2['variants'][i][0]['barcode'])

for i in range(0, len(df_products_shopify2['variants'])):
    price.append(df_products_shopify2['variants'][i][0]['price'])

for i in range(0, len(df_products_shopify2['variants'])):
    vSku.append(df_products_shopify2['variants'][i][0]['sku'])

for i in range(0, len(df_products_shopify2['images'])):
    vImages.append(df_products_shopify2['images'][i][0]['src'])
    # vImages1.append(df_products_shopify2['images'][i][1]['src'])


df_products_shopify2['barcodes'] = np.array(barcodes)
df_products_shopify2['sku'] = np.array(vSku)
df_products_shopify2['price'] = np.array(price)
df_products_shopify2['images'] = np.array(vImages)
# df_products_shopify2['images1'] = np.array(vImages1)

# df_products_shopify_final['barcodes'] = "'"+df_products_shopify_final['barcodes']+"',"

df_products_shopify_final = df_products_shopify2[[
    'id', 'title', 'vendor', 'product_type', 'barcodes','sku', 'price', 'images'
    # , 'images1'
    ]]



for char in spec_chars:
    df_products_shopify_final['title'] = df_products_shopify_final['title'].str.replace(
            char, ' ', regex=True)
df_products_shopify_final['title'] = df_products_shopify_final['title'].str.split(
    ).str.join(" ")

# # print(df_products_shopify_final['title'])
print(df_products_shopify_final)
df_products_shopify_final.to_excel('habitt_prod_data.xlsx',index=False)
