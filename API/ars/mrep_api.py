from google.cloud import bigquery
import uvicorn
from google.oauth2 import service_account
from fastai import FastAPI
credentials = service_account.Credentials.from_service_account_file('d:\\TEMP\\ars\\data-light-house-prod-0baa98f57152.json')

project_id = 'data-light-house-prod'
client = bigquery.Client(credentials= credentials,project=project_id)



########################################### Branches  ###############################################################
branches_job = client.query("""
   SELECT *
   FROM `data-light-house-prod.EDW.IBL_BRANCH`""")

branches_results = branches_job.result() #
branches_df = branches_results.to_dataframe()

########################################### Full_Load_IBL_CUSTOMER  ###############################################################
Full_Load_IBL_CUSTOMER = client.query("""
   select * from `data-light-house-prod.EDW.IBL_CUSTOMER`
where date>='2022-05-01' """)

Full_Load_IBL_CUSTOMER_results = Full_Load_IBL_CUSTOMER.result() #
Full_Load_IBL_CUSTOMER_df = Full_Load_IBL_CUSTOMER_results.to_dataframe()

########################################### Incremental_Load_IBL_CUSTOMER  ###############################################################
Incremental_Load_IBL_CUSTOMER = client.query("""
select
client_number,
customer_number,
customer_country_key,
customer_name,
customer_name2,
customer_city,
customer_region,
customer_sortfield,
customer_street_houseno,
customer_telephoneno,
customer_address,
customer_searchmatchcode1,
customer_searchmatchcode2,
customer_searchmatchcode3,
customer_title,
customer_internationallocation_1,
customer_internationallocation_2,
customer_authorization,
customer_industry_key,
customer_internationallocation_checkdigit,
customer_createdon,
person_created_the_object,
customer_accountgroup,
customer_classification,
accountno_vendor,
customer_name3,
customer_name4,
customer_district,
customer_citycode,
regional_market,
customer_languagekey,
customer_taxno1,
customer_taxno2,
customer_liableforVAT,
customer_telephoneno2,
VAT_registrationno,
competitor,
sales_partner,
sales_prospect,
customer_type4,
legal_status,
initial_contact,
customer_tax_jurisdiction,
plant,
customer_paymentblock,
is_customer_ICMSexempt,
is_customer_IPIexempt,
customer_CFOP_category,
RG_no,
issued_by,
state,
RG_issue_date,
RIC_no,
foreign_national_registration,
RNE_issue_date,
CNAE,
legal_nature,
CRT_no,
ICMS_taxpayer,
name1,
name2,
name3,
first_name,
title,
dealer,
expiry_date,
company_code,
personnel_number,
from `data-light-house-prod.EDW.IBL_CUSTOMER`
where customer_createdon = (current_date-1) or CONFIRMEDCHANGES_DATE =  (current_date-1); """)

Incremental_Load_IBL_CUSTOMER_results = Incremental_Load_IBL_CUSTOMER.result() #
Incremental_Load_IBL_CUSTOMER_df1 = Incremental_Load_IBL_CUSTOMER_results.to_dataframe()
Incremental_Load_IBL_CUSTOMER_df = Incremental_Load_IBL_CUSTOMER_df1.to_dict(orient = 'records')

########################################### Full_Load_IBL_PRODUCTS  ###############################################################
Full_Load_IBL_PRODUCTS = client.query(f'''
        SELECT
            *
            FROM(
                select branch_id BR_CD ,
                document_no AS BILL_NO,
                trx_date1 AS BILL_DT        ,
                replace(CUSTOMER_NUMBER,'.0','') AS EBS_CUST,
                CUSTOMER_NAME AS CUSTOMER_NAME,
                ifnull(address_1,'-')ADD1,
                ifnull(address_2,   '-') ADD2,
                Ifnull(address_3,'-') ADD3,
                CHANNEL AS CH_CD,
                ITEM_CODE AS ITEM_CODE,
                item_description AS description,
                ' '  AS BATCH_NO,
                unit_selling_price AS price,
                cast(SUM(sold_qty) as string) AS SOLD_QTY,
                cast(SUM(BONUS_QTY) as int) AS BON_QTY,
                cast(SUM(DISCOUNT) as float64) AS disc_amt,
                cast(SUM(NET_AMT) as float64) AS NET_amt,
                cast(SUM(GROSS_VALUE) as float64) AS GROSS_VALUE,
                cast(SUM(discounted_rate) as float64) AS discounted_rate,
                ifnull(case when esa.SALES_ORDER_TYPE = 'Bill Near Exp Sales'
                then 'Near Expiry'
                when esa.SALES_ORDER_TYPE in ('Cancel Bill NE Sales',
                    'OPS Cancel. Invoice', 'OPS-Cancel Cred Memo') then 'Cancel'
                when esa.SALES_ORDER_TYPE = 'OPS Sales Tax Cash'
                then 'Sale'
                when esa.SALES_ORDER_TYPE = 'OPS-Sales Returns'
                then 'Return'
                when upper(esa.SALES_ORDER_TYPE) like '%RET%'  then 'Return'
                when upper(esa.SALES_ORDER_TYPE) NOT like '%RET%'  then 'Sale'
                end,' ') as reason
                from `data-light-house-prod.EDW.VW_EBS_SAS_ALL_LOC_DATA_NEW` ESA
                where 1 = 1 AND billing_date  ='2022-08-10'
                GROUP BY BR_CD,
                document_no,
                TRX_DATE1      ,
                replace(CUSTOMER_NUMBER,'.0',''),
                CUSTOMER_NAME,
                ESA.UNIT_SELLING_PRICE,
                SALES_ORDER_TYPE,
                address_1,
                address_2,
                address_3,
                CHANNEL,
                ITEM_CODE,
                DESCRIPTION
                ) A
                -----where bill_no='139923' and upper(description) like '%LUMARK%'
                ORDER BY
            BILL_DT
        '''
        )

Full_Load_IBL_PRODUCTS_results = Full_Load_IBL_PRODUCTS.result() #
Full_Load_IBL_PRODUCTS_df1 = Full_Load_IBL_PRODUCTS_results.to_dataframe()
Full_Load_IBL_PRODUCTS_df =Full_Load_IBL_PRODUCTS_df1.to_dict(orient = 'records')



########################################### Incremental_Load_IBL_PRODUCTS  ###############################################################
Incremental_Load_IBL_PRODUCTS = client.query("""
 select *  from `data-light-house-prod.EDW.IBL_PRODUCTS`
where created_on = (current_date-1) or last_change_date =  (current_date-1); """)

Incremental_Load_IBL_PRODUCTS_results = Incremental_Load_IBL_PRODUCTS.result() #
Incremental_Load_IBL_PRODUCTS_df1 = Incremental_Load_IBL_PRODUCTS_results.to_dataframe()
Incremental_Load_IBL_PRODUCTS_df = Incremental_Load_IBL_PRODUCTS_df1.to_dict(orient = 'records')

########################################### Full_Load_IBL_BUSINESS_LINE ###############################################################
Full_Load_IBL_BUSINESS_LINE = client.query("""
  select * from `data-light-house-prod.EDW.IBL_BUSINESS_LINE`; """)

Full_Load_IBL_BUSINESS_LINE_results = Full_Load_IBL_BUSINESS_LINE.result() #
Full_Load_IBL_BUSINESS_LINE_df = Full_Load_IBL_BUSINESS_LINE_results.to_dataframe()
Full_Load_IBL_BUSINESS_LINE_df = Full_Load_IBL_BUSINESS_LINE_df.to_dict(
    orient='records')


# print(df.head())
app = FastAPI()
@app.get("/Full_Load_IBL_Branches")
async def root():
    return branches_df

@app.get("/Full_Load_IBL_CUSTOMER")
async def root():
    return Full_Load_IBL_CUSTOMER_df

@app.get("/Incremental_Load_IBL_CUSTOMER")
async def root():
    return Incremental_Load_IBL_CUSTOMER_df

@app.get("/Full_Load_IBL_PRODUCTS")
async def root():
    return Full_Load_IBL_PRODUCTS_df

@app.get("/Incremental_Load_IBL_PRODUCTS")
async def root():
    return Incremental_Load_IBL_PRODUCTS_df

@app.get("/Full_Load_IBL_BUSINESS_LINE")
async def root():
    return Full_Load_IBL_BUSINESS_LINE_df

# if __name__ == "__main__":
#     uvicorn.run(app,host="192.168.130.81",port=9001)

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=9001)
