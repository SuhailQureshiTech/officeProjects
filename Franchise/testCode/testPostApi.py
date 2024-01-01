
# Import pandas library
import requests
import pandas as pd
 
 
# initialize list of lists
url = 'http://35.216.155.219:9001/api/addDistMapping'
 
data={'rmo_id':['44','66'],'distritbutor_id':[23,22]}

# data={'rmo_id':['44','66','Sahil','77']}
df=pd.DataFrame(data)
print(df.to_json())
# print(df)
# requests.post(url, data=df.to_json())