
from datetime import datetime
now = datetime.now()
current_time = now.strftime("%H")
if current_time=='17' or current_time=='18':
    print('yes validate')
    print("Current Time =", current_time)
else:
    print('not')