
import pandas as pd
df=pd.DataFrame()
df=pd.read_csv('HC_ALL_LOC_SALE.txt',sep='$')
df.drop(list(df.filter(regex='SPL')),axis=1,inplace=True)

# df.drop(list(df.filter(regex='name')), axis=1, inplace=True)

df['BR_CD']=df['BR_CD'].astype(str)
           
print(df.info())
# print(df)

dfGroupSum=df.groupby('BR_CD')['GROSS_VALUE'].sum().astype('float128')
dfGroupSum.to_csv('mangoooo.csv')
print(dfGroupSum)