
import pandas as pd
import json
from pandas import json_normalize
data = '''
{
"technologies":
        [
        { "Courses": "Spark", "Fee": 22000,"Duration":"40Days"},
        { "Courses": "PySpark","Fee": 25000,"Duration":"60Days"},
        { "Courses": "Hadoop", "Fee": 23000,"Duration":"50Days"}
        ],
"status": ["ok"]
}
'''
print(data)


# Use json_normalize() to convert JSON to DataFrame
dict = json.loads(data)
df2 = json_normalize(dict['technologies'])
print(df2)
