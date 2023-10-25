import pandas as pd

# Original code
df = pd.DataFrame({'a': [1, 2, 3, 'bad', 5],
                   'b': [0.1, 0.2, 0.3, 0.4, 0.5],
                   'item': ['a', 'b', 'c', 'd', 'e']})
df = df.set_index('item')
# print(df)

a = pd.to_numeric(df.a, errors='coerce')
idx=a.isna()
print(df[idx])


