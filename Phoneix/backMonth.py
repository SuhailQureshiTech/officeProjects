from datetime import date, timedelta
import datetime
import dateutil.relativedelta
now = datetime.datetime.now()
print (now)
print (now + dateutil.relativedelta.relativedelta(months=-1))

prev = date.today().replace(day=1) - timedelta(days=1)
print (prev.month)
last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)

start_day_of_prev_month = date.today().replace(
    day=1) - timedelta(days=last_day_of_prev_month.day)

# For printing results
print("First day of prev month:", start_day_of_prev_month)
print("Last day of prev month:", last_day_of_prev_month)
