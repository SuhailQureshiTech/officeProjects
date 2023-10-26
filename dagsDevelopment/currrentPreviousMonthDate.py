



def returnDataDate():
    vStartDate=None
    vEndDate=None
    from datetime import date, datetime, timedelta
    from datetime import date
    from datetime import datetime,date,timezone
    from dateutil.relativedelta import relativedelta
    from dateutil import parser

    vTodayDate = datetime.date(datetime.today())
    vTodayDate = int(vTodayDate.strftime("%d"))

    if vTodayDate==2:
        # from Previous month to current...
        # print('vtoday :',vTodayDate)
        today = date.today()
        d = today - relativedelta(months=1)
        # print('d ',d)
        vStartDate = date(d.year, d.month, 1)

        import dateutil.relativedelta
        first = today.replace(day=1)
        vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)

    else:
        print('else')
        vStartDate = datetime.date(datetime.today().replace(day=1))
        vEndDate = datetime.date(datetime.today()-timedelta(days=1))

    # print('Start Date : ', vStartDate)
    # print('End   Date : ', vEndDate)

    return vStartDate,vEndDate
