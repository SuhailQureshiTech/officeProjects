from datetime import date
from dateutil.relativedelta import relativedelta

today = date.today()
d = today - relativedelta(months=1)

def previousMonthStartDate():
    vStartDate = date(d.year, d.month, 1)
    vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
    print(vStartDate)
    return vStartDate
    
def previousMonthEndDate():
    vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)
    vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"
    print(vEndDate)
    return vEndDate