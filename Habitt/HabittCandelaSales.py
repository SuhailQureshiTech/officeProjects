from datetime import date, datetime, timedelta
from typing import Iterator
import cx_Oracle as xo
import pypyodbc as odbc
import pandas as pd
import time
import datetime
import sys
import os
import inspect

global connectionDB, sqlConnection, records, vStartDate, numberOfRecords
global oracleServerAdd, oraclePort, oracleServiceName, oracleUserName, oraclePassword
oracleConnectionDB = None

global sqlConnectionCandela
global sqlConnectionHabitt

records = None
global cursor_candela
global cursor_Habitt

global oracleCursor
vday = 3
numberOfRecords = 0
total = 0

# vStartDate = date.today()
# datetime.date.today()
# datetime.date.today()
# vStartDate = vStartDate-timedelta(days=vday)
# vStartDate=vStartDate.replace(day=1)
# vStartDate = vStartDate.strftime('%Y%m%d')

vStartDate = date.today()
vStartDate = vStartDate-timedelta(days=vday)

vStartDate1 = date.today()
vStartDate1 = vStartDate1-timedelta(days=1)

vStartDate = "'"+str(vStartDate)+"'"
vStartDate1 = "'"+str(vStartDate1)+"'"

print(vStartDate)
print(vStartDate1)

sqlConnectionCandela = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                    "Server=192.168.130.72;"
                                    "Database=candela;"
                                    "uid=habitt;pwd=habitt")

cursor_candela = sqlConnectionCandela.cursor()

sqlConnectionHabitt = odbc.connect("Driver={SQL Server};"
                                "Server=192.168.130.81;"
                                "Database=habittTarzDB;"
                                "uid=pbironew;pwd=pbiro1234_456")

cursor_Habitt = sqlConnectionHabitt.cursor()

def closeConnections():
    print('close')
    sqlConnectionCandela.close()
    cursor_candela.close()
    sqlConnectionHabitt.close()
    cursor_Habitt.close()

def processCandelaReords():
    sql = f''' select
                'S' Source
                ,CAST(WS.sale_date AS DATE)Sales_DT
                ,CAST(WS.SALE_DATE AS DATE)Sales_Date
                ,WS.shop_id,WS.[Shop Name]
                ,WS.[Product Code],WS.[Product Name],WS.[Invoice No]
				,WS.line_item_id DEPT_ID
				,WS.[Line Item Name] SUB_DEPARTMENT_CODE
				,WS.[Line Item Name] SUB_DEPARTMENT
                ,WS.[Category Name] Category
                ,WS.[Sub Category]	Sub_Category
                ,WS.Quantity QTY
                ,WS.Unit_Price	TP
                ,WS.[Gross Sale]   GROSS_SALE
                ,WS.[Total Discount]	DISCOUNT
                ,WS.[Net Sale]	NET_SALE
                ---
                ,WS.SM_ID
                ,WS.SM_NAME

                ,ISNULL(WS.Customer_ID,-99)customer_id
                ,customer_name
                ,WS.Cust_Address

                ,ISNULL(PRODUCT_GROUP_ID,-99) PRODUCT_GROUP_ID
				,WS.[Line Item Name] target_dept
				from
                (
                SELECT     s.sale_date, s.shop_id, sp.shop_name AS [Shop Name], Prd.Product_number AS [Product Code], Prd.item_name AS [Product Name], s.sale_id AS [Invoice No], li.line_item_id,
                                    li.field_name AS [Line Item Name], Ctg.category_id, Ctg.field_name AS [Category Name], SubCatg.defect_id, SubCatg.field_name AS [Sub Category], VAddBy.Value_Addition_By_ID,
                                    VAddBy.Field_Name AS [Value Added By], PrdGrp.product_group_id, PrdGrp.field_name AS [Product Group], AgeGrp.age_group_id, AgeGrp.field_name AS [Age Group], ISNULL(i.Unit_price, 0)
                                    AS Unit_Price, i.qty AS Quantity, i.qty * ISNULL(i.Unit_price, 0) AS [Gross Sale], ISNULL(i.product_discount_amount, 0) * ISNULL(i.qty, 0) + ISNULL(i.mem_discount_amount, 0) * ISNULL(i.qty, 0)
                                    + ISNULL(i.product_mkt_Discount, 0) - ISNULL(i.product_adj_Discount, 0) AS [Total Discount], i.qty * ISNULL(i.Unit_price, 0) - (ISNULL(i.product_discount_amount, 0) * ISNULL(i.qty, 0)
                                    + ISNULL(i.mem_discount_amount, 0) * ISNULL(i.qty, 0) + ISNULL(i.product_mkt_Discount, 0) - ISNULL(i.product_adj_Discount, 0)) AS [Net Sale], s.employee_id AS SM_ID,
                                    dbo.tblDefShopEmployees.field_name AS SM_NAME, s.member_id AS Customer_ID, ISNULL(ISNULL(CAST(s.member_id AS varchar(10)), 'NULL') + '@' + ISNULL(CAST(s.shop_id AS varchar(10)),
                                    'NULL'), 'NEW') AS CustomerUnq, dbo.tblMemberInfo.member_name AS Customer_Name, dbo.tblMemberInfo.Cust_Address, dbo.tblMemberInfo.phone_Mobile
                FROM         dbo.tblSales AS s LEFT OUTER JOIN
                                    dbo.tblMemberInfo ON s.member_id = dbo.tblMemberInfo.member_id AND s.shop_id = dbo.tblMemberInfo.shop_id LEFT OUTER JOIN
                                    dbo.tblDefShopEmployees ON s.shop_id = dbo.tblDefShopEmployees.shop_id AND s.employee_id = dbo.tblDefShopEmployees.shop_employee_id LEFT OUTER JOIN
                                    dbo.tblSalesLineItems AS i ON s.shop_id = i.shop_id AND s.sale_id = i.sale_id LEFT OUTER JOIN
                                    dbo.tblDefProducts AS Prd ON i.Product_code = Prd.Product_number LEFT OUTER JOIN
                                    dbo.tblDefLineItems AS li ON Prd.line_item_id = li.line_item_id LEFT OUTER JOIN
                                    dbo.tblDefProductValueAdditionBy AS VAddBy ON Prd.Value_addition_by_ID = VAddBy.Value_Addition_By_ID LEFT OUTER JOIN
                                    dbo.tblDefCategory AS Ctg ON Prd.category_id = Ctg.category_id LEFT OUTER JOIN
                                    dbo.tblDefProductGroups AS PrdGrp ON Prd.Product_group_id = PrdGrp.product_group_id LEFT OUTER JOIN
                                    dbo.tblDefPackagingCodes AS PrdPkgCode ON Prd.Packaging_code_id = PrdPkgCode.packaging_code_id LEFT OUTER JOIN
                                    dbo.tblDefAgeGroups AS AgeGrp ON Prd.Age_groups_id = AgeGrp.age_group_id LEFT OUTER JOIN
                                    dbo.tblDefSuppliers AS Sup ON Prd.supplier_id = Sup.supplier_id LEFT OUTER JOIN
                                    dbo.tblDefCalendarSeasons AS Clndr ON Prd.calendar_season_id = Clndr.calendar_season_id LEFT OUTER JOIN
                                    dbo.tblDefDefects AS SubCatg ON Prd.subcategory_id = SubCatg.defect_id LEFT OUTER JOIN
                                    dbo.cboDiscountTypes AS disType ON Prd.Pur_Con_Unit = disType.discount_type_id LEFT OUTER JOIN
                                    dbo.tblDefShops AS sp ON s.shop_id = sp.shop_id
                            WHERE     (NOT (Prd.Product_number IN (N'NULL')))
                            )ws
                            where 1=1
                            AND CAST(WS.sale_date AS DATE) between {vStartDate} and {vStartDate1}
            '''
    # AND CAST(WS.sale_date AS DATE)>='01-JUL-20'
    recList = []
    Chunksize = 20000

    for chunk in pd.read_sql_query(sql, sqlConnectionCandela, chunksize=Chunksize):
        recList.append(chunk)
        insertSalesSql1(chunk.values.tolist())

    closeConnections()


def insertSalesSql1(rec):
    sqlInsert = '''
            insert into HabittID.HABITT_SALES
            (
            Source
                ,Sales_DT
                ,Sales_Date
                ,shop_id
                ,Shop_Name
                ,Product_Code
                ,Product_Name
                ,Invoice_No
				,DEPT_ID
				,SUB_DEPARTMENT_CODE
				,SUB_DEPARTMENT
                ,Category
                ,Sub_Category
                ,QTY
                ,TP
                ,GROSS_SALE
                ,DISCOUNT
                ,NET_SALE
                ,SM_ID
                ,SM_NAME
                ,customer_id
                ,customer_name
                ,Cust_Address
                ,PRODUCT_GROUP_ID
                ,TARGET_DEPT

            )
            values
            (
                ?,?,?,?,?,
                ?,?,?,?,?,
                ?,
                ?,?,?,?,?,?,?,
                ?,?,
                ?,?,
                ?,
                ?,?
                )
            '''
    try:
        cursor_Habitt.executemany(sqlInsert, rec)
        cursor_Habitt.commit()
    except Exception as e:
        cursor_Habitt.rollback()
        print(e)


def deleteRecords():
    print('block : '+vStartDate)
    delQuery =f'''
            delete from HabittID.habitt_sales
            where 1=1
            and sales_dt>={vStartDate}

            '''


    # print(delQuery)

    try:
        cursor_Habitt.execute(delQuery)
        cursor_Habitt.commit()
    except Exception as e:
        cursor_Habitt.rollback()
        print(e)


deleteRecords()
processCandelaReords()
