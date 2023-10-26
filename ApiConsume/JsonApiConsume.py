import  requests as req
# from pandas import json_normalize,DataFrame as df
import pandas as pd
import time
from datetime import date, datetime, timedelta

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))

def convert_to_preferred_format(sec):
   sec = sec % (24 * 3600)
   hour = sec // 3600
   sec %= 3600
   min = sec // 60
   sec %= 60
   return "%02d:%02d:%02d" % (hour, min, sec) 
   n = 10000
   return convert(n)

def ikonSales():
    start_time=time.time()
    
    if vTodayDate == 1:
        print('two')

        vEndDate = datetime.date(datetime.today()-timedelta(days=1))
        vdayDiff = int(vEndDate.strftime("%d"))
        vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

    else:

        # vStartDate = datetime.date(datetime.today().replace(day=1))
        vStartDate = datetime.date(datetime.today()-timedelta(days=30))
        vEndDate = datetime.date(datetime.today()-timedelta(days=1))

    vStartDate = vStartDate.strftime("%Y-%m-%d")
    vEndDate = vEndDate.strftime("%Y-%m-%d")

    # print('Start Date : ', vStartDate)
    # print('End   Date : ', vEndDate)

    vStartDate='2023-07-01'
    vEndDate='2023-07-10'
    
    # vStartDate="'"+vStartDate+"'"
    # vEndDate="'"+vEndDate+"'"
    
    # print(vStartDate)
    # print(vEndDate)
    
    # PARAMS = {'fromDate': vStartDate, 'toDate': vEndDate,
    #             'apikeyCode': 'iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup'
    #         }

    # api_url ='http://34.65.6.130:9000/sales/dist'
    # response = req.get(url=api_url,params=PARAMS)

    api_url ='http://35.216.155.219:9000/sales/dist?fromDate=2023-08-01&toDate=2023-08-31&apikeyCode=iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup'

    response = req.get(api_url)
    # print(response.json())

    r = response.json()

    df = pd.DataFrame.from_dict(r)
    # print(df)
    df.to_csv("D:\\TEMP\\IKONSEP.csv")
    print('completed in ',convert_to_preferred_format(time.time()-start_time))

def ikonFullCustomer():
    api_url = 'http://34.65.6.130:9000/Full_Load_IBL_CUSTOMER?apikeyCode=iblykgiOBb2K7O3DIrxfgFpCyQTEHuUKxAR5r6cJ79JEZFhqEbCnmPQTA95Lyjup'
    # print(api_url)
    response = req.get(url=api_url)
    # response = req.get(api_url)
    # print(response.json())

    r = response.json()

    df = pd.DataFrame.from_dict(r)
    # print(df)
    df.to_csv("D:\\TEMP\\IKONCUSTOMERS.csv")


# ikonFullCustomer()
ikonSales()
print('done')