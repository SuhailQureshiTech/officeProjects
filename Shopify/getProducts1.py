
import shopify
import json
import requests
import os
import pandas as pds
import numpy as np

API_KEY = 'a8e17787e2074ce995b75465022349de'
PASSWORD = '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'
SHOP_NAME = 'habitt-store'
API_VERSION = '2023-04'
shop_url = "https://%s.myshopify.com/admin" % (SHOP_NAME)
shopify.ShopifyResource.set_user(API_KEY)
shopify.ShopifyResource.set_password(PASSWORD)

shopify.ShopifyResource.set_site(shop_url)
cus=shopify.customer
print(cus)

def update_shopify_products_sap():

    url = f"https://{API_KEY}:{PASSWORD}@habitt-store.myshopify.com/admin/api/2023-04/products.json"
    payload = ""
    headers = {
    'Cookie': '_master_udr=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaEpJaWs0TkRnMVlqVTNPUzAyTUdVeUxUUmlabU10T1dZNU9TMWlZbVE0TmpJd1pEUXpOVElHT2daRlJnPT0iLCJleHAiOiIyMDI0LTA3LTIyVDA5OjMyOjU2Ljc0M1oiLCJwdXIiOiJjb29raWUuX21hc3Rlcl91ZHIifX0%3D--02a70f3d3e49c5ed101b1ba9f1cddc7d3fca9be5; _secure_admin_session_id=3bee420534f8a94f4bae452d83ca6e21; _secure_admin_session_id_csrf=3bee420534f8a94f4bae452d83ca6e21; _landing_page=%2F; _orig_referrer=; _shopify_y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; _y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; cart=f3eb468c2860b7280b73c773081878cc; cart_sig=7bbc5f066eacf57aad2ad40c36b54299; cart_ts=1658483398; cart_ver=gcp-us-central1%3A1; identity-state=BAhbCkkiJTM3M2ZkYmFjMTBmMTVlM2UzNzY5YzZmZTBlNzAyMTRmBjoGRUZJIiUxOGZlMWFiMWZjMTFiOWNlOTE2Yjc2YmE5MzQxMzkwMwY7AEZJIiUwZTc4MjQzMTI2MmU2YTc2ZDY3MDAwYzJjOWM3MjYxNQY7AEZJIiVhM2E2YTA2YWRjYjE0NWMyNGQwYzZjOTYzODI3MGFiMgY7AEZJIiViZWEyNGY3ZTQwOThiZDdiZTdiYmJmM2I4NDc3ZjZhZAY7AEY%3D--25bb4197b0669f65438b1a778cb0e46457623657; identity-state-0e782431262e6a76d67000c2c9c72615=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1OTE3NjcxOS4xNTE2Mjc4SSIKbm9uY2UGOwBUSSIlZmRlNWIxNjgwOTA4NmI5OGVkMDA5OTdiZGYyNjVmZGIGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--ffc86af6f9dac44773160b94c756a1bad0c359b1; identity-state-18fe1ab1fc11b9ce916b76ba93413903=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1OTE3NjY3MS44OTE2NzQ1SSIKbm9uY2UGOwBUSSIlNmMwMzgxNTY2ZmJmZTg4OTZkNmMzMTE4ZWY3NWVlMDMGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--662a09720035d833e95925cb7eee5395dea12917; identity-state-373fdbac10f15e3e3769c6fe0e70214f=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYWMTY1OTE3NjY2NS45NTg1OTFJIgpub25jZQY7AFRJIiU3ZjZiMjVhNmUyMDEwOGJmYzRjMWMwNDk3YTY3NDk0YgY7AEZJIgpzY29wZQY7AFRbD0kiCmVtYWlsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZGVzdGluYXRpb25zLnJlYWRvbmx5BjsAVEkiC29wZW5pZAY7AFRJIgxwcm9maWxlBjsAVEkiTmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvcGFydG5lcnMuY29sbGFib3JhdG9yLXJlbGF0aW9uc2hpcHMucmVhZG9ubHkGOwBUSSIwaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9iYW5raW5nLm1hbmFnZQY7AFRJIkJodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL21lcmNoYW50LXNldHVwLWRhc2hib2FyZC5ncmFwaHFsBjsAVEkiPGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvc2hvcGlmeS1jaGF0LmFkbWluLmdyYXBocWwGOwBUSSI3aHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9mbG93LndvcmtmbG93cy5tYW5hZ2UGOwBUSSI%2BaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9vcmdhbml6YXRpb24taWRlbnRpdHkubWFuYWdlBjsAVEkiD2NvbmZpZy1rZXkGOwBUSSIMZGVmYXVsdAY7AFQ%3D--7fa3a2bc321672832aafed93ff92705d5b6fc8b5; identity-state-a3a6a06adcb145c24d0c6c9638270ab2=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1OTE3NzQwNC42NDIxNTY0SSIKbm9uY2UGOwBUSSIlNzA4ODc0M2VkMDNkZWMxMDc4M2E3ZDQwYmFlMTRkMmMGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--a177c8043441d93f9763fdacaad67b9d40774397; identity-state-bea24f7e4098bd7be7bbbf3b8477f6ad=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1OTE3NzQxMi40MjU4OTE2SSIKbm9uY2UGOwBUSSIlZmFiZjI4NTU5NTEwNWU2NjkyNDFhNGViY2YxZWViOTQGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--ab80324275015a11a6bfca9b46168dd4f7be371e; localization=PK; secure_customer_sig='
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    barcodes=[]
    data = response.content
    my_json = json.loads(data)
    df_products_shopify = pds.DataFrame.from_dict(my_json['products'])
    last_id = df_products_shopify['id'].iat[-1]
    url2 = f"https://{API_KEY}:{PASSWORD}@habitt-store.myshopify.com/admin/api/2022-01/products.json"
    response2 = requests.request("GET", url2, headers=headers, data=payload)
    data2 = response2.content
    my_json2 = json.loads(data2)
    df_products_shopify2 = pds.DataFrame.from_dict(my_json2['products'])

    df_products_shopify_final = pds.concat([df_products_shopify, df_products_shopify2], axis=0, ignore_index=True)

    for i in range(0,len(df_products_shopify_final['variants'])):
        barcodes.append(df_products_shopify_final['variants'][i][0]['barcode'])

    df_products_shopify_final['barcodes'] = np.array(barcodes)
    # df_products_shopify_final['barcodes'] = "'"+df_products_shopify_final['barcodes']+"',"
    df_products_shopify_final = df_products_shopify_final[['id','title','barcodes']]
    print(df_products_shopify)
    # df_products_shopify_final.to_csv('C:\shopify.csv')
    #credentials = service_account.Credentials.from_service_account_file(
    #'C:/rfc/data-light-house-prod-0baa98f57152.json')

    # credentials = service_account.Credentials.from_service_account_file(
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=["https://www.googleapis.com/auth/cloud-platform"],
    # )

    # client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    #client = bigquery.Client(credentials= credentials,project=project_id)
    res = []
    for val in barcodes:
        if val != None:
            res.append(val)
    t = tuple(res)
    print('tuple ',t)
    # query_string = "SELECT item_code, sum(qty) as sku,max(trade_price) as price FROM `data-light-house-prod.EDW.VW_IBL_STOCK` where  item_code in {} and org_id=8044 and subinventory_code_desc='Sellable' group by item_code;".format(t)
    # df = (
    #     client.query(query_string)
    #     .result()
    #     .to_dataframe(
    #         create_bqstorage_client=True,
    #     )
    # )

    # df['sku'] = df['sku'].astype('int')
    # df_products_shopify_final['barcodes'] = df_products_shopify_final['barcodes'].astype('str')
    # df_final = pds.merge(df, df_products_shopify_final, left_on='item_code', right_on='barcodes', how='inner')

    # df_final = df_final.drop_duplicates()
    # df_final = df_final.to_json(orient='records')
    # df_parsed = json.loads(df_final)
    # #print(len(df_parsed))

    # for i in range(0,len(df_parsed)):
    #     url_update = f"https://{API_KEY}:{PASSWORD}@habitt-store.myshopify.com/admin/api/2022-01/products/{df_parsed[i]['id']}.json"
    #     #print(url_update + ' ' + str(i))
    #     # payload_update = json.dumps({
    #     # "product": {
    #     #     "variants": [
    #     #     {
    #     #         "barcode": df_parsed[i]['item_code'],
    #     #         "price": df_parsed[i]['price'],
    #     #         "sku": df_parsed[i]['sku'],
    #     #         "inventory_quantity": df_parsed[i]['sku'],
    #     #         "old_inventory_quantity": df_parsed[i]['sku'],
    #     #     }
    #     #     ]
    #     # }})

    #     headers = {
    #         'Content-Type': 'application/json',
    #         'Cookie': '_master_udr=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaEpJaWs0TkRnMVlqVTNPUzAyTUdVeUxUUmlabU10T1dZNU9TMWlZbVE0TmpJd1pEUXpOVElHT2daRlJnPT0iLCJleHAiOiIyMDI0LTA3LTIyVDA5OjMyOjU2Ljc0M1oiLCJwdXIiOiJjb29raWUuX21hc3Rlcl91ZHIifX0%3D--02a70f3d3e49c5ed101b1ba9f1cddc7d3fca9be5; _secure_admin_session_id=3bee420534f8a94f4bae452d83ca6e21; _secure_admin_session_id_csrf=3bee420534f8a94f4bae452d83ca6e21; _landing_page=%2F; _orig_referrer=; _s=afe68edd-3902-4a10-b2fb-09725cc7da69; _shopify_s=afe68edd-3902-4a10-b2fb-09725cc7da69; _shopify_y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; _y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; cart=f3eb468c2860b7280b73c773081878cc; cart_sig=7bbc5f066eacf57aad2ad40c36b54299; cart_ts=1658483398; cart_ver=gcp-us-central1%3A1; identity-state=BAhbCkkiJTgwNmQwNmVhNmViYjQzYWIwYjkzMDU1Njk4NDAxODkzBjoGRUZJIiVkMzU2YTYzMjQ5ODNhMzU0YjIxNTVmMTdlYmJjMDRiMQY7AEZJIiVmNjdkNzBmYjA3NzBlOTYxMGM4NGM4OTA3ZTAyNmMwNAY7AEZJIiU5MTMxNjkzMTNhMTQxNTcxZjI5MDFiMWVkMGQ3MDZmZQY7AEZJIiVjYjI0YWQ1YWQyMjU1ZmRmMWY4YWMxZWQ3MmYwOWY3NAY7AEY%3D--e01ed4f2dca0d304ba6bf74674dba99e0794252e; identity-state-806d06ea6ebb43ab0b93055698401893=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTE3My45MTU5MzMxSSIKbm9uY2UGOwBUSSIlMzMxNjZlYjgwMzQzZjY3NmJiYzlhNTIyZWIwOGRlZDIGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--fce94b4576233e803f9d8cfc1e4862b5ab1c305f; identity-state-913169313a141571f2901b1ed0d706fe=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTUzNC40NzE3NzM0SSIKbm9uY2UGOwBUSSIlMDkyNWJlMTBmNzAyYmI5NjdiNTA3ZWZiYTNhZWExMjUGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--96a369bfd784ddb76b3a783b4d6717904755e6e1; identity-state-cb24ad5ad2255fdf1f8ac1ed72f09f74=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTgwMy42MjcwMDA4SSIKbm9uY2UGOwBUSSIlMjg0YmQyNTAxZDI2ODcwNGIxNjM2ZTJlZTU0NDZkMTMGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--420e6d7d9d90a196c3061494f16eb0b07838a709; identity-state-d356a6324983a354b2155f17ebbc04b1=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTQ3NS4wMDk1MTk2SSIKbm9uY2UGOwBUSSIlNmE2MWI0OTY5ZGE2ZjhkYjBiOTIzY2IzMzYwMDgxNmIGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--326bef54ec84536cbbdcdaedaac4df51c02d764b; identity-state-f67d70fb0770e9610c84c8907e026c04=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTUyMy40MzQ4NzcySSIKbm9uY2UGOwBUSSIlYjQzODkxMWI4ZGU5NmNhZjVlMDVmYzJiODk1NmMxOTAGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--aeb8b045c1784bf84f441648de4f04ea85bfb227; localization=PK; secure_customer_sig='
    #     }

    #     # response = requests.request("PUT", url_update, headers=headers, data=payload_update)


update_shopify_products_sap()
