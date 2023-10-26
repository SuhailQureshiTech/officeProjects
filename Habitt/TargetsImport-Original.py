import pandas as pd
import pypyodbc as odbc
global sqlConnectionHabitt
global cursor_Habitt

df = pd.read_excel(
    r'\\192.168.130.81\HabittTarget\ShopDay WiseTargetPowerBI.xls')

print(df.info())

# df = pd.read_excel(r'D:\\TEMP\\ShopDay WiseTargetPowerBI.xls')

# print(df)

# Replacing null value
# df["Department"].fillna('NaN', inplace=True)
# df["Target_Value"].fillna("0", inplace=True)

# sqlConnectionHabitt = odbc.connect("Driver={SQL Server Native Client 11.0};"
#                                 "Server=192.168.130.59;"
#                                 "Database=habittTarzDB;"
#                                 "uid=habittid;pwd=habitt123")

# cursor_Habitt = sqlConnectionHabitt.cursor()

# # print('Sql Server 59 Connection : ', sqlConnectionHabitt)
# # print('Sql Server 59 Cursor : ', cursor_Habitt)

# deleteStatement = 'delete from target_tmp_data'
# cursor_Habitt.execute(deleteStatement)
# cursor_Habitt.commit()

# deleteStatement = 'delete from target_tmp'
# cursor_Habitt.execute(deleteStatement)
# cursor_Habitt.commit()

# for index, row in df.iterrows():
#         insertStatement=f''' insert into target_tmp
#         ([shop],[shop_id],[Full Name],[department],[month],[date],[target_value])
#         values(?,?,?,?,?,?,?)
#                 '''
# cursor_Habitt.executemany(insertStatement,df.values.tolist())
# cursor_Habitt.commit()

# insertStatement =f'''
#         insert into target_tmp_data
#         (shop,shop_id,[full name],department,MONTH,date,target_value)
#         SELECT SHOP,SHOP_ID,[FULL NAME], DEPARTMENT,MONTH, DATE
#         ,TARGET_VALUE
#         FROM TARGET_TMP
#         EXCEPT
#         SELECT SHOP,SHOP_ID,[FULL NAME] ,DEPARTMENT,MONTH, DATE
#         ,TARGET_VALUE
#         FROM TARGET1
#                 '''

# cursor_Habitt.execute(insertStatement)
# cursor_Habitt.commit()

# deleteStatement = f'''
#         DELETE
#         FROM TARGET1 WHERE 1=1 AND SHOP+CAST(Shop_ID AS varchar)+[Full Name]+Department+CAST(MONTH AS varchar)+CAST(DATE AS VARCHAR) IN
#         (
#         select  T.SHOP+CAST(T.Shop_ID AS varchar)+T.[Full Name]+T.Department+CAST(T.MONTH AS varchar)+CAST(T.DATE AS VARCHAR)
#         ---+T.MONTH+T.DATE
#         from target1 T INNER JOIN target_tmp_data TTD ON (TTD.SHOP=T.Shop AND TTD.SHOP_ID=T.Shop_ID AND TTD.[FULL NAME]=T.[Full Name] AND TTD.DEPARTMENT=T.Department
#         AND TTD.MONTH=T.Month AND TTD.DATE=T.Date)
#         )
# '''
# cursor_Habitt.execute(deleteStatement)
# cursor_Habitt.commit()
# insertStatement = f'''
#         insert into target1 select * from target_tmp_data;
#                 '''

# cursor_Habitt.execute(insertStatement)
# cursor_Habitt.commit()

# cursor_Habitt.close()
# sqlConnectionHabitt.close()
# print('Done.....')


