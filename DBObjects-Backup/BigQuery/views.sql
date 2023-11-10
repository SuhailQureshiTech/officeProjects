
create or replace view `data-light-house-prod`.EDW.ALL_BUSINESS_LINE_DATA AS
SELECT * FROM (
SELECT business_line_code,business_line_desc 
FROM `data-light-house-prod.EDW.IBL_BUSINESS_LINE_DESC` 
UNION ALL
select CAST(FLEX_VALUE_MEANING AS STRING),DESCRIPTION from `data-light-house-prod.EDW.EBS_BUSLINE_LINE_DATA`);

create or replace view `data-light-house-prod`.EDW.current_year_sale as 
SELECT is2.branch_id AS ORG_ID,
    is2.branch_description AS ORG_DESCRIPTION,

   is2.billing_date AS INVOICE_DATE,
   is2.document_no AS INVOICE_NUMBER,
   is2.booker_id AS BOOKER_ID,
   is2.booker_name AS BOOKER_NAME,
   is2.supplier_id AS SUPPLIER_ID,
   is2.supplier_name AS SUPPLIER_NAME,

    is2.business_line_id AS BUSINESS_LINE_ID,
    is2.business_line_description AS BUSINESS_LINE,
    is2.channel AS CHANNEL,

   is2.customer_id AS CUSTOMER_ID,
   is2.customer_name AS CUSTOMER_NAME,
   is2.customer_number AS CUSTOMER_NUMBER,
   concat(coalesce(is2.address_1,''), ' ', coalesce(is2.address_2,''), ' ',coalesce(is2.address_3,'')) AS Customer_Address,
   is2.sales_order_type AS SALES_ORDER_TYPE,

    is2.item_code AS ITEM_CODE,
    is2.item_description AS ITEM_DESC,
   is2.unit_selling_price AS UNIT_SELLING_PRICE,
   
   -- extract(YEAR from is2.billing_date )Year,
   -- FORMAT_DATETIME("%B", is2.Billing_date) as month_name,

    sum(is2.sold_qty) AS SOLD_QTY,
   sum(is2.bonus_qty) AS BONUS_QTY,
   sum(is2.claimable_discount) AS CLAIMABLE_DISCOUNT,
   sum(is2.unclaimable_discount) AS UNCLAIMABLE_DISCOUNT,
   sum(is2.tax_recoverable) AS TAX_RECOVERABLE,
    sum(is2.gross_amount) AS GROSS,
  is2.reason_code AS REASON_CODE
   FROM `data-light-house-prod.EDW.VW_IBL_SALES` is2
  WHERE 1=1
  and is2.billing_date >='2023-02-01' 
  AND is2.company_code = '6300'
  group by 
  is2.branch_id ,
    is2.branch_description ,
    is2.business_line_id ,
    is2.business_line_description ,
    is2.channel ,
    is2.item_code,
    is2.item_description,
       is2.billing_date,
   is2.document_no ,
   is2.booker_id ,
   is2.booker_name ,
   is2.supplier_id ,
   is2.supplier_name ,is2.customer_id,
      is2.customer_name,
   is2.customer_number ,
   concat(coalesce(is2.address_1,''), ' ', coalesce(is2.address_2,''), ' ',coalesce(is2.address_3,'')) ,
   is2.sales_order_type,
    is2.item_code ,
    is2.item_description ,is2.reason_code,
       is2.unit_selling_price
      --  ,extract(YEAR from is2.billing_date ) ,
      --  ,FORMAT_DATETIME("%B", is2.Billing_date) 
;

create or replace view `data-light-house-prod`.EDW.DSHB_PJP_SALES as 
SELECT 
pjp_code,
pjp_name,
item_code,
item_description,
branch_id,
branch_description,
billing_date,
booker_id,
booker_name,
supplier_id,
supplier_name,
business_line_id,
business_line_description,
channel,
customer_number,
customer_name,
reason_code,
company_code,
---,sales_order_no,salesflo_order_no,
sum(sold_qty)sold_qty,
sum(bonus_qty)bonus_qty,
sum(claimable_discount)claimable_discount,
sum(unclaimable_discount)unclaimable_discount,
sum(tax_recoverable)tax_recoverable,
sum(net_amount)net_amount,
sum(gross_amount)gross_amount,
sum(total_discount)total_discount
 FROM `data-light-house-prod.EDW.VW_PJP_SALE`
 group by
pjp_code,
pjp_name,
item_code,
item_description,
branch_id,
branch_description,
billing_date,
booker_id,
booker_name,
supplier_id,
supplier_name,
business_line_id,
business_line_description,
channel,
customer_number,
customer_name,
reason_code,company_code;

create or replace view `data-light-house-prod`.EDW.EBS_INVOICE_ORDER_VW as 
SELECT sdm.customer_name AS institution,
    sdm.branch_id,
    sdm.branch_description AS branch_name,
    'IBL Operations (Pvt.) Ltd.' AS distributor,
    sdm.channel AS ins_type,
    NULL AS product,
    cast(cast(sdm.item_code as INT64) as string) item_code,
    sdm.item_description AS sku,
    sdm.unit_selling_price AS selling_price,
    sdm.claimable_discount,
    sdm.unclaimable_discount AS un_claimable_discount,
    sdm.claimable_discount + sdm.unclaimable_discount AS inst_discount,
    format_datetime('%B',  sdm.billing_date) AS Month,
    sdm.sales_order_no AS order_ref_no,
    NULL AS date_of_order,
    sdm.sold_qty AS order_quantity,
    sdm.bonus_qty AS foc,
    sdm.sold_qty AS total_qty,
    sdm.gross_amount AS sales_vlaue,
    sdm.billing_date AS invoice_date_ibl,
    sdm.document_no AS invoice_no_ibl,
    sdm.tax_recoverable,
    NULL AS customer_trx_id
   FROM `data-light-house-prod.EDW.VW_IBL_SALES` sdm
  WHERE 1 = 1 and billing_date>='2022-01-01'  AND sdm.company_code = '6300' 
  AND sdm.channel = 'Institution - Pharma' and item_code not like 'F%'
  and CUSTOMER_NUMBER not in (
'2000193189',
'2000187218',
'2000187418',
'2000187234',
'2000187260',
'2000187434',
'2000187249',
'2000187256',
'2000187683',
'2000187257',
'2000187248',
'2000187354',
'2000187250',
'2000187842',
'2000187243',
'2000187306',
'2000187216',
'2000187215',
'2000187771',
'2000141200',
'2000187193',
'2000187205',
'2000187162',
'2000187037',
'2000187222',
'2000187217',
'2000187662',
'2000187255',
'2000187210',
'2000187285'
)
   UNION ALL
  SELECT sdm.customer_name AS institution,
    sdm.branch_id,
    sdm.branch_description AS branch_name,
    'IBL Operations (Pvt.) Ltd.' AS distributor,
    sdm.channel AS ins_type,
    NULL AS product,
    sdm.item_code ,
    sdm.item_description AS sku,
    sdm.unit_selling_price AS selling_price,
    sdm.claimable_discount,
    sdm.unclaimable_discount AS un_claimable_discount,
    sdm.claimable_discount + sdm.unclaimable_discount AS inst_discount,
    format_datetime('%B',  sdm.billing_date) AS Month,
    sdm.sales_order_no AS order_ref_no,
    NULL AS date_of_order,
    sdm.sold_qty AS order_quantity,
    sdm.bonus_qty AS foc,
    sdm.sold_qty AS total_qty,
    sdm.gross_amount AS sales_vlaue,
    sdm.billing_date AS invoice_date_ibl,
    sdm.document_no AS invoice_no_ibl,
    sdm.tax_recoverable,
    NULL AS customer_trx_id
   FROM `data-light-house-prod.EDW.VW_IBL_SALES` sdm
  WHERE 1 = 1 and billing_date>='2022-01-01'  AND sdm.company_code = '6300' 
  AND sdm.channel = 'Institution - Pharma' and item_code not LIKE  '0%'
   and CUSTOMER_NUMBER not in (
'2000193189',
'2000187218',
'2000187418',
'2000187234',
'2000187260',
'2000187434',
'2000187249',
'2000187256',
'2000187683',
'2000187257',
'2000187248',
'2000187354',
'2000187250',
'2000187842',
'2000187243',
'2000187306',
'2000187216',
'2000187215',
'2000187771',
'2000141200',
'2000187193',
'2000187205',
'2000187162',
'2000187037',
'2000187222',
'2000187217',
'2000187662',
'2000187255',
'2000187210',
'2000187285'
);

create or replace view `data-light-house-prod`.EDW.FRANCHISE_SALE_POSTGRE as 
select 
fsal.order_no,invoice_date,invoice_number invoice_no
,channel,ibl_distributor_code,distributor_customer_code distributor_customer_no,ibl_customer_code ibl_customer_no
,ibl_customer_name customer_name,distributor_item_code,ibl_item_code,ibl_item_description item_description,sold_qty qty_sold
,gross_amount,reason,discount
,bonus_qty,company_code,data_loading_date current_dates,address
 from  `data-light-house-prod.EDW.FRANCHISE_SALES`  as fsal
where 1=1;

create or replace view `data-light-house-prod`.EDW.FRANCHISE_SALES_MONTH as 
select * from(
select 
EXTRACT(YEAR FROM invoice_date)year_,
EXTRACT(MONTH FROM invoice_date)month_num
,FORMAT_DATETIME("%B", DATETIME(invoice_date)) as month_name
---FORMAT_DATETIME("%B", DATETIME(invoice_date))month_
,count(*)cnt
from `data-light-house-prod.EDW.FRANCHISE_SALES` 
---where invoice_date>'2022-10-01' 
group by EXTRACT(MONTH FROM invoice_date),FORMAT_DATETIME("%B", DATETIME(invoice_date)),EXTRACT(YEAR FROM invoice_date)
---order by EXTRACT(MONTH FROM invoice_date) 
) order by month_num;

create or replace view `data-light-house-prod`.EDW.IBL_CUSTOMERS_CORDINATES as
select * from (
 select distinct customer_number,customer_name,cast(b.store_latitude as float64) as store_latitude
,cast(b.store_longitude as float64) as store_longitude
  from `data-light-house-prod.EDW.IBL_CUSTOMER` a
  ,`data-light-house-prod.EDW.IBL_SALESFLO_ORDERS` b
 WHERE a.date>='2019-07-01' and b.delivery_date>="2019-07-01"
 and b.sap_store_code=a.customer_number
 )customer_data
  where customer_data.store_latitude is not null;
  
create or replace view  `data-light-house-prod`.EDW.IBL_ITEM as
select distinct item_code,item_desc from (
select 
distinct cast(cast(itm.matnr as int) as string) item_code,itm.matnr_desc item_desc
 from `data-light-house-prod.EDW.MATNR_WITH_COMPANY_BUSLINE_STOCK` as itm
 where itm.matnr   like '000%'
union all
select 
distinct cast(itm.matnr as string)item_code,itm.matnr_desc item_desc
 from `data-light-house-prod.EDW.MATNR_WITH_COMPANY_BUSLINE_STOCK` as itm
 where itm.matnr not  like '000%'
);

create or replace view `data-light-house-prod`.EDW.IBL_PRODUCTS_DATA as
select 
 distinct substr(matnr,9,20) item_code,matnr_desc item_desc,null sales_organization
 from `EDW.MATNR_WITH_COMPANY_BUSLINE_STOCK`
where 1=1;

create or replace view `data-light-house-prod`.EDW.MARKITT_GRN_DSH as 
SELECT
  pstngdate bill_date,
  matdoc bill_no,
  NULL user_name,
  itm.barcode bar_code,
  cast(material as string) sap_item_code,
  itm.BarCode_Desc BarCode_Desc
,grn.qtyinune quantity, 
  0 rate,
0 amount,
0 item_discount_amount,
0 discount_amount,
0 item_GST_amount,
0 net_amount,
itm.category_code category_code,
itm.category category,
itm.brand_code brand_code,
itm.brand brand,
'Sap' data_flag,
'GRN' data_type
FROM
  `data-light-house-prod.EDW.MARKITT_GRN` as grn
LEFT OUTER JOIN  `data-light-house-prod.EDW.VW_MARKITT_ITEMS_LIST`  as itm ON (CAST(itm.SAP_ITEM_CODE as string) =cast(grn.material as string))
 WHERE  1=1  and grn.mvt in ('101','102','161');
 


`data-light-house-prod`.EDW.MARKITT_ITEMS_DSH
`data-light-house-prod`.EDW.MARKITT_PO_DSH
`data-light-house-prod`.EDW.MARKITT_SALES_DSH
`data-light-house-prod`.EDW.markitt_sales_utility
`data-light-house-prod`.EDW.MARKITT_STOCK_OPQTY_DSH
`data-light-house-prod`.EDW.OUT_DATED_STOCK
`data-light-house-prod`.EDW.VW_BRANCH_DATA
`data-light-house-prod`.EDW.vw_branch_wise_sale_comparison
`data-light-house-prod`.EDW.VW_Branch_Wise_Sales_Report
`data-light-house-prod`.EDW.vw_branch_wise_sales_report_hier
`data-light-house-prod`.EDW.VW_EBS_SAS_ALL_LOC_DATA_NEW
`data-light-house-prod`.EDW.VW_EBS_SAS_GROUP_LOC_DATA_NEW

create or replace view `data-light-house-prod`.EDW.VW_EBS_SAS_HC_ALL_LOC_DATA_NEW as 
select
   branch_id ,
   branch_description ,
   document_no ,
   billing_date,
   format_date("%d-%b-%y", billing_date)trx_date1,
   customer_number,
   customer_name ,
   substr(item_code, 9, 30)item_code,
   item_description ,
   cast(unit_selling_price as string)unit_selling_price,
   ifnull(sold_qty, 0) sold_qty,
   ifnull(bonus_qty, 0)bonus_qty,
   ifnull(claimable_discount, 0) + ifnull(unclaimable_discount, 0)discount,
   channel ,
   address_1 address_1,
   address_2 address_2,
   address_3 address_3,
   branch_description org_desc,
   company_code ,
   gross_amount net_amt ,
   ifnull(gross_amount, 0) + ifnull(claimable_discount, 0) + ifnull(unclaimable_discount, 0)gross_value,
   case
      when
         sold_qty <> 0 
         and 
         (
            ifnull(claimable_discount, 0) + ifnull(unclaimable_discount, 0)
         )
         <> 0 
      then
( ((sold_qty*unit_selling_price) + (ifnull(claimable_discount, 0) + ifnull(unclaimable_discount, 0))) / sold_qty ) 
      else
         0 
   end
   discounted_rate , sales_order_type ,'OPS'DATA_FLAG
from `data-light-house-prod.EDW.VW_IBL_SALES` st
  WHERE 1=1 
    and st.billing_date>='2022-01-01'
    and (ST.COMPANY_CODE='6100' and st.business_line_id IN ('N19','N16') 
        or st.company_code='6300' and st.business_line_id  not IN ('N19','N16') 
    )  
      AND st.channel<>'Distributor - A'
union all
SELECT
  branch_code  branch_id ,
  branch_desc  branch_description ,
  invoice_number document_no ,
  invoice_date billing_date,
  format_date("%d-%b-%y", invoice_date)trx_date1,
   ref_customer_number customer_number,
   ibl_customer_name customer_name ,
   ibl_item_code item_code,
   sap_item_desc item_description ,
   cast(0 as string) unit_selling_price,
    ifnull(sold_qty, 0) sold_qty,
   ifnull(bonus_qty, 0)bonus_qty,
   ifnull(discount, 0) discount,
    channel ,
   ifnull(add1,'') address_1,
   ifnull(add2,'') address_2,
   ifnull(add3,'') address_3,
    branch_desc org_desc,
     company_code ,
   gross_amount net_amt ,
   ifnull(gross_amount, 0) + ifnull(discount, 0) gross_value,
   case
      when
         sold_qty <> 0 
         and 
         (
            ifnull(discount, 0)         )
         <> 0 
      then
( ((gross_amount) + (ifnull(discount, 0) )) / sold_qty ) 
      else
         0 
   end
   discounted_rate ,reason sales_order_type,'SD'DATA_FLAG
FROM `EDW.VW_FRANCHISE_SALES_DATA`;


`data-light-house-prod`.EDW.VW_ECC_S4HANA_EXEC_DATA
`data-light-house-prod`.EDW.vw_ecchanadata
`data-light-house-prod`.EDW.VW_FRANCHISE_ADDRESS
`data-light-house-prod`.EDW.VW_FRANCHISE_SALES_DATA
`data-light-house-prod`.EDW.VW_FRANCHISE_SAP_CUSTOMERS
`data-light-house-prod`.EDW.VW_FRANCHISE_SAP_LOCATIONS
`data-light-house-prod`.EDW.VW_IBL_BOOKING_EXECUTION
`data-light-house-prod`.EDW.VW_IBL_BOOKING_EXECUTION_6200
`data-light-house-prod`.EDW.VW_IBL_BUSINESS_LINE_TARGETS
`data-light-house-prod`.EDW.VW_IBL_LIVV_STOCK
`data-light-house-prod`.EDW.VW_IBL_PAKOLA_STOCK
`data-light-house-prod`.EDW.VW_IBL_PEPSI_STOCK
`data-light-house-prod`.EDW.VW_IBL_PRODUCTIVITY
`data-light-house-prod`.EDW.VW_IBL_SALES
`data-light-house-prod`.EDW.VW_IBL_SALES2
`data-light-house-prod`.EDW.vw_ibl_sales_backdated
`data-light-house-prod`.EDW.VW_IBL_SALES_HC
`data-light-house-prod`.EDW.VW_IBL_STOCK
`data-light-house-prod`.EDW.VW_IBL_STOCK_HC
`data-light-house-prod`.EDW.VW_IBL_STOCK_test
`data-light-house-prod`.EDW.VW_LIVVEL_SALES
`data-light-house-prod`.EDW.VW_LOGISTIC_SALES
`data-light-house-prod`.EDW.VW_MARKITT_ERROR_LOG_LIST
create or replace view `data-light-house-prod`.EDW.VW_MARKITT_ITEMS_LIST
create or replace view `data-light-house-prod`.EDW.VW_MARKITT_PURCHASE
create or replace view `data-light-house-prod`.EDW.VW_MARKITT_SAL_PO_GRN_DATA
create or replace view `data-light-house-prod`.EDW.VW_NATIONAL_SALES_REPORT
create or replace view `data-light-house-prod`.EDW.VW_NATIONAL_SALES_REPORT_Including_Previous
create or replace view `data-light-house-prod`.EDW.VW_ORG
create or replace view `data-light-house-prod`.EDW.VW_PAKOLA_SALES
create or replace view `data-light-house-prod`.EDW.VW_PEPSICO_SALES
create or replace view `data-light-house-prod`.EDW.VW_PJP_SALE
create or replace view `data-light-house-prod`.EDW.vw_s4hanadata
create or replace view `data-light-house-prod`.EDW.VW_SAS_MREP_DATA
create or replace view `data-light-house-prod`.EDW.VW_SEARLE_SALES
create or replace view `data-light-house-prod`.EDW.VW_SEARLE_SALES_BKUP_FINAL
create or replace view `data-light-house-prod`.EDW.ZMARKIT_ITEM_DSH