-- 6300,6100
SELECT 
company_code,bill_type_description,billing_type,sales_order_type
,sum(sold_qty) sold_qty,sum(gross_amount)gross_amount,sum(claimable_discount)claimable_discount
,sum(unclaimable_discount)unclaimable_discount,sum(tax_recoverable)tax_recoverable
FROM (
SELECT a.item_category,
    a.org_id as branch_id,
    a.org_desc as branch_description,
    a.trx_date as billing_date,
    concat(
    		business_line_id, to_char(trx_date,'Mon-YY')
    	) billing_date1,
--    FORMAT_DATE("%b-%y",trx_date) billing_date2,
   to_char(trx_date,'Mon-YY') billing_date2, 	
    a.trx_number as document_no,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.business_line business_line_description,
    a.channel,
    a.customer_id,
    a.customer_number,
    a.customer_name,
    a.sales_order_type,
    a.inventory_item_id,
    a.item_code,
    a.description as item_description,
    a.unit_selling_price,
    a.sold_qty,
    a.bonus_qty,
    a.claimable_discount,a.unclaimable_discount,
    case 
    when a.sales_order_type in ('UB Cancel.Invoice') then  a.tax_recoverable*-1 
    when  UPPER(sales_order_type) like 'OPS%CANC%INVOICE%' then  a.tax_recoverable*-1 
    else a.tax_recoverable 
    end tax_recoverable,
    CASE WHEN ITEM_CATEGORY IN ('ZFCL','ZFOU','ZBRC','ZBRU') THEN TAX_RECOVERABLE 	
	ELSE 		
	CASE WHEN billing_type IN ('ZUCC','ZURB') THEN
		((SOLD_QTY* UNIT_SELLING_PRICE )-TOTAL_DISCOUNT*-1)+TAX_RECOVERABLE
	ELSE
	((SOLD_QTY* UNIT_SELLING_PRICE )-ABS(TOTAL_DISCOUNT) )+TAX_RECOVERABLE	
	END	
	END net_amount,
        CASE
            WHEN a.item_category IN ('ZFCL', 'ZFOU', 'ZBRC', 'ZBRU') THEN 0
            ELSE
            CASE
                WHEN a.billing_type = 'ZORC' AND a.billing_type <> 'ZUB1' THEN a.sold_qty * a.unit_selling_price
                ELSE a.sold_qty * a.unit_selling_price
            END
        END AS gross_amount,
    a.total_discount,
    a.customer_trx_id as customer_num,
    a.reason_code,
    a.bill_type_desc as bill_type_description,
    a.company_code,
    a.billing_type,
    a.sales_order_no,
    a.add1 as address_1,
    a.add2 as address_2,
    a.add3  as address_3,
    a.salesflo_order_no,
    a.return_reason_code
    ,CASE
      WHEN a.return_reason_code='Assigned by the System (Internal)'  then 'N'
        WHEN a.return_reason_code='Delivery date too late'  then 'Y'
        WHEN a.return_reason_code='Poor quality'  then 'N'
        WHEN a.return_reason_code='Too expensive'  then 'N'
        WHEN a.return_reason_code='Competitor better'  then 'N'
        WHEN a.return_reason_code='Guarantee'  then 'N'
        WHEN a.return_reason_code='Unreasonable request'  then 'N'
        WHEN a.return_reason_code='Cust.to receive replacement'  then 'N'
        WHEN a.return_reason_code='No Cash'  then 'Y'
        WHEN a.return_reason_code='Transaction is being checked'  then 'N'
        WHEN a.return_reason_code='Bill return due to wrong TP'  then 'N'
        WHEN a.return_reason_code='Out of section bill'  then 'N'
        WHEN a.return_reason_code='Order not placed'  then 'N'
        WHEN a.return_reason_code='Batch issue'  then 'Y'
        WHEN a.return_reason_code='Shop closed'  then 'Y'
        WHEN a.return_reason_code='Incorrect order'  then 'N'
        WHEN a.return_reason_code='Without DSL'  then 'N'
        WHEN a.return_reason_code='Late delivery'  then 'Y'
        WHEN a.return_reason_code='Return due to wrong patch'  then 'N'
        WHEN a.return_reason_code='Discount issue'  then 'N'
        WHEN a.return_reason_code='Cancelled By Dealer / Customer'  then 'N'
        WHEN a.return_reason_code='Selection of Wrong Product'  then 'N'
        WHEN a.return_reason_code='Shortage of Stock'  then 'N'
        WHEN a.return_reason_code='Unsellable Stock'  then 'N'
        WHEN a.return_reason_code='Trial Order'  then 'N'
       ELSE a.return_reason_code end as execution_status
   FROM ( SELECT psdd.item_category,
            psdd.ORG_ID AS org_id,
            psdd.ORG_DESC AS org_desc,
            psdd.TRX_DATE AS trx_date,
            psdd.TRX_NUMBER AS trx_number,
            psdd.booker_id,
            psdd.booker_name,
            psdd.supplier_id,
            psdd.supplier_name,
            trim(psdd.business_line_id) AS business_line_id,
            psdd.BUSINESS_LINE AS business_line,
            COALESCE(psdd.channel, 'Blank') AS channel,
            psdd.customer_id,
            psdd.customer_number,
            psdd.customer_name,
            psdd.sales_order_type,
            psdd.inventory_item_id,
            psdd.item_code,
            psdd.DESCRIPTION AS description,
            psdd.unit_selling_price,
            psdd.sold_qty,
            psdd.bonus_qty,
            -- psdd.claimable_discount,
            -- psdd.unclaimable_discount,
               CASE
            WHEN billing_type = 'ZORC' THEN claimable_discount * -1
            ELSE claimable_discount
        END AS claimable_discount,
        CASE
            WHEN billing_type= 'ZORC' THEN unclaimable_discount * -1
            ELSE unclaimable_discount
        END AS unclaimable_discount,
            psdd.tax_recoverable,
            psdd.gross_amount AS net_amount,		
					CASE WHEN billing_type IN ('ZORC') THEN
	claimable_discount*-1
	ELSE 
	claimable_discount
	END+CASE WHEN billing_type IN ('ZORC') THEN
	unclaimable_discount*-1
	ELSE
	unclaimable_discount
	END TOTAL_DISCOUNT,
	-- psdd.unclaimable_discount + psdd.claimable_discount AS total_discount,            
            psdd.customer_number AS customer_trx_id,
            --psdd.reason_code,
            case 
            when company_code in ('6100','6300') and UPPER(sales_order_type) like '%CANCE%' then 'Cancel' 
            when company_code in ('6100','6300') and UPPER(sales_order_type) like '%RETU%' then 'Return'
            when company_code in ('6100','6300') and UPPER(sales_order_type) not like '%RETU%'  and UPPER(sales_order_type) not like '%CANCE%' then 'Sales'
            else psdd.reason_code
            end reason_code,
            psdd.BILL_TYPE_DESC AS bill_type_desc,
            psdd.company_code,
            psdd.billing_type,
            psdd.sales_order_no,
            psdd.ADD1 AS add1,
            psdd.ADD2 AS add2,
            psdd.ADD3 AS add3,
            psdd.SALES_ORDER_NO salesflo_order_no,
            psdd.return_reason_code
           FROM PHNX_SALES_DATA  psdd
          WHERE 1 = 1
--          AND TRX_NUMBER='8400209582' 
          AND psdd.TRX_DATE>='2019-07-01'
           AND (psdd.company_code IN('6100', '6300')) 
           AND (psdd.billing_type IN('ZOPC', 'ZOCC', 'ZUBC', 'ZUCC', 'ZORE', 'ZORC', 'ZURB', 'ZUB1', 'ZNES', 'ZNEC')))a
           ) GROUP BY company_code,bill_type_description,billing_type,sales_order_type
              ORDER BY company_code
           ;
           
--           union all

SELECT 
company_code,bill_type_description,billing_type,sales_order_type,document_no,item_code,item_description
,sum(sold_qty) sold_qty,sum(gross_amount)gross_amount,sum(claimable_discount)claimable_discount
,sum(unclaimable_discount)unclaimable_discount,sum(tax_recoverable)tax_recoverable
FROM (
    SELECT a.item_category,
    a.org_id as branch_id,
    a.org_desc as branch_description,
    a.trx_date as billing_date,
    concat(
    		business_line_id, to_char(trx_date,'Mon-YY')
    	) billing_date1,
   to_char(trx_date,'Mon-YY') billing_date2, 	
    a.trx_number as document_no,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.business_line business_line_description,
    a.channel,
    a.customer_id,
    a.customer_number,
    a.customer_name,
    a.sales_order_type,
    a.inventory_item_id,
    a.item_code,
    a.description as item_description,
    a.unit_selling_price,
    a.sold_qty,
    a.bonus_qty,
    a.claimable_discount,a.unclaimable_discount,
    --Tax Recoverable
    case 
    when a.sales_order_type in   ('Cancel IBL Log Billl') then  a.tax_recoverable*-1 
    else a.tax_recoverable 
    end tax_recoverable,
    CASE WHEN ITEM_CATEGORY IN ('ZFCL','ZFOU','ZBRC','ZBRU','ZTAI') THEN TAX_RECOVERABLE 	
	ELSE 		
	CASE WHEN billing_type IN ('ZUCC','ZURB','ZIBC') THEN
		((SOLD_QTY* UNIT_SELLING_PRICE )-TOTAL_DISCOUNT*-1)+TAX_RECOVERABLE
	ELSE
	((SOLD_QTY* UNIT_SELLING_PRICE )-ABS(TOTAL_DISCOUNT) )+TAX_RECOVERABLE	
	END	
	END net_amount,
        CASE
            WHEN a.item_category IN ('ZFCL', 'ZFOU', 'ZBRC', 'ZBRU') THEN 0
            ELSE
            CASE
                WHEN a.billing_type = 'ZORC' AND a.billing_type <> 'ZUB1' THEN a.sold_qty * a.unit_selling_price
                ELSE a.sold_qty * a.unit_selling_price
            END
        END AS gross_amount,
    a.total_discount,
    a.customer_trx_id as customer_num,
    a.reason_code,
    a.bill_type_desc as bill_type_description,
    a.company_code,
    a.billing_type,
    a.sales_order_no,
    a.add1 as address_1,
    a.add2 as address_2,
    a.add3  as address_3,
    a.salesflo_order_no,
    a.return_reason_code
   , NULL as execution_status
   FROM ( SELECT psdd.item_category,
            psdd.org_id AS org_id,
            psdd.org_desc AS org_desc,
            psdd.trx_date AS trx_date,
            psdd.trx_number AS trx_number,
            psdd.booker_id,
            psdd.booker_name,
            psdd.supplier_id,
            psdd.supplier_name,
            trim(psdd.business_line_id) AS business_line_id,
            psdd.BUSINESS_LINE AS business_line,
            COALESCE(psdd.channel, 'Blank') AS channel,
            psdd.customer_id,
            psdd.customer_number,
            psdd.customer_name,
            psdd.sales_order_type,
            psdd.inventory_item_id,
            psdd.item_code,
            psdd.DESCRIPTION AS description,
            psdd.unit_selling_price,
            psdd.sold_qty,
            psdd.bonus_qty,
               CASE
            WHEN UPPER(sales_order_type) LIKE '%CANCE%CRED%' THEN claimable_discount * -1
            ELSE claimable_discount
        END AS claimable_discount,
        CASE
            WHEN UPPER(sales_order_type) LIKE '%CANCE%CRED%' THEN unclaimable_discount * -1
            ELSE unclaimable_discount
        END AS unclaimable_discount,
            psdd.tax_recoverable,
            psdd.gross_amount AS net_amount,		
		CASE WHEN  UPPER(sales_order_type) LIKE '%CANCE%CRED%' THEN 	claimable_discount*-1
	ELSE 
	claimable_discount
	END+CASE WHEN  UPPER(sales_order_type) LIKE '%CANCE%CRED%' THEN
	unclaimable_discount*-1
	ELSE
	unclaimable_discount
	END TOTAL_DISCOUNT,
            psdd.customer_number AS customer_trx_id,
            case 
            when company_code in ('6200') and UPPER(sales_order_type) like '%CANCE%' then 'Cancel' 
            when company_code in ('6200') and UPPER(sales_order_type) like '%RETU%' then 'Return'
            when company_code in ('6200') and UPPER(sales_order_type) not like '%RETU%'  
            and UPPER(sales_order_type) not like '%CANCE%' then    'Sales'
            else psdd.reason_code
            end reason_code,
            psdd.BILL_TYPE_DESC AS bill_type_desc,
            psdd.company_code,
            psdd.billing_type,
            psdd.sales_order_no,
            psdd.ADD1 AS add1,
            psdd.ADD2 AS add2,
            psdd.ADD3 AS add3,
            psdd.SALES_ORDER_NO salesflo_order_no,
            psdd.return_reason_code
           FROM PHNX_SALES_DATA_LOG  psdd
          WHERE 1 = 1 AND trx_date>='2022-10-01' 
         ---  AND trx_date='2023-03-01'
          AND (psdd.company_code IN('6200')) 
          AND (psdd.billing_type IN( 'ZIUP','ZIBB','ZLRB','ZSII','ZCMR','ZCUP','ZSIC','ZLB2','ZIBC')
                     )) a) 
                    GROUP BY company_code,bill_type_description,billing_type,sales_order_type,document_no,item_code,item_description
              ORDER BY company_code
                     ;
                     
                    
                    SELECT DISTINCT TRX_NUMBER 
                    FROM PHNX_SALES_DATA_LOG WHERE TRX_NUMBER ='4030002352';