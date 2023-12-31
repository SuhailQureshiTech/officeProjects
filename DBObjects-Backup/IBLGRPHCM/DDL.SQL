-- IBLGRPHCM.EBS_INVOICE_DATA_TBL definition

CREATE TABLE "EBS_INVOICE_DATA_TBL" 
   (	"BILL_DT" DATE, 
	"ITEM_CODE" VARCHAR2(100), 
	"PROD_NM" VARCHAR2(200), 
	"ORG_SAP_ID" VARCHAR2(10), 
	"ORG_DESC" VARCHAR2(200), 
	"CHANNEL" VARCHAR2(100), 
	"DATA_FLAG" VARCHAR2(100), 
	"SOLD_QTY" NUMBER(*,0), 
	"GROSS" NUMBER(18,2), 
	"UNIT_SELLING_PRICE" NUMBER(18,2)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "IBLGRPHCM" ;


-- IBLGRPHCM.EBS_INVOICE_DATA_TBL1 definition

CREATE TABLE "EBS_INVOICE_DATA_TBL1" 
   (	"BILL_DT" DATE, 
	"ITEM_CODE" VARCHAR2(100), 
	"PROD_NM" VARCHAR2(200), 
	"ORG_SAP_ID" VARCHAR2(10), 
	"ORG_DESC" VARCHAR2(200), 
	"CHANNEL" VARCHAR2(100), 
	"DATA_FLAG" VARCHAR2(100), 
	"SOLD_QTY" NUMBER(*,0), 
	"GROSS" NUMBER(18,2), 
	"UNIT_SELLING_PRICE" NUMBER(18,2)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "IBLGRPHCM" ;


-- IBLGRPHCM.EBS_INVOICE_ORDER definition

CREATE TABLE "EBS_INVOICE_ORDER" 
   (	"INSTITUTION" VARCHAR2(250 CHAR), 
	"BRANCH_ID" VARCHAR2(250 CHAR), 
	"BRANCH_NAME" VARCHAR2(250 CHAR), 
	"DISTRIBUTOR" VARCHAR2(250 CHAR), 
	"INS_TYPE" VARCHAR2(250 CHAR), 
	"ITEM_CODE" VARCHAR2(250 CHAR), 
	"SKU" VARCHAR2(250 CHAR), 
	"SELLING_PRICE" FLOAT(126), 
	"CLAIMABLE_DISCOUNT" FLOAT(126), 
	"UN_CLAIMABLE_DISCOUNT" FLOAT(126), 
	"INST_DISCOUNT" FLOAT(126), 
	"MONTH" VARCHAR2(15 CHAR), 
	"ORDER_REF_NO" VARCHAR2(15 CHAR), 
	"DATE_OF_ORDER" DATE, 
	"ORDER_QUANTITY" FLOAT(126), 
	"FOC" FLOAT(126), 
	"TOTAL_QTY" FLOAT(126), 
	"SALES_VLAUE" FLOAT(126), 
	"NET_SALE_VALUE" FLOAT(126), 
	"INVOICE_DATE_IBL" DATE, 
	"INVOICE_NO_IBL" VARCHAR2(50 CHAR), 
	"TAX_RECOVERABLE" FLOAT(126), 
	"CUSTOMER_TRX_ID" VARCHAR2(25 CHAR)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "IBLGRPHCM" ;


-- IBLGRPHCM.GROUPS definition

CREATE TABLE "GROUPS" 
   (	"GRP_ID" VARCHAR2(10), 
	"GRP_DSCP" VARCHAR2(75), 
	"BU" VARCHAR2(20), 
	"AD" VARCHAR2(25)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "IBLGRPHCM" ;


-- IBLGRPHCM.PRODUCTSSS definition

CREATE TABLE "PRODUCTSSS" 
   (	"ITEM_COD" VARCHAR2(10), 
	"PRODUCT_NM" VARCHAR2(75)
   ) SEGMENT CREATION DEFERRED 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  TABLESPACE "IBLGRPHCM" ;


-- IBLGRPHCM.PRODUCT_LIVE2 definition

CREATE TABLE "PRODUCT_LIVE2" 
   (	"PROD_ID" VARCHAR2(10), 
	"GRP_ID" VARCHAR2(6), 
	"CTG" VARCHAR2(10), 
	"NPROD_ID" VARCHAR2(15), 
	"PROD_NM" VARCHAR2(200), 
	"EFP" NUMBER, 
	"TP" NUMBER, 
	"CP" NUMBER, 
	"SAP_ITEM_CODE" VARCHAR2(15), 
	"ACT_DEACT" VARCHAR2(2), 
	"SAS_PROD_ID" VARCHAR2(15)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "IBLGRPHCM" ;


-- IBLGRPHCM.PRODUCT_NUTRITION definition

CREATE TABLE "PRODUCT_NUTRITION" 
   (	"PROD_ID" VARCHAR2(10), 
	"GRP_ID" VARCHAR2(6), 
	"CTG" VARCHAR2(10), 
	"NPROD_ID" VARCHAR2(15), 
	"PROD_NM" VARCHAR2(200), 
	"EFP" NUMBER, 
	"TP" NUMBER, 
	"CP" NUMBER, 
	"SAP_ITEM_CODE" VARCHAR2(15), 
	"ACT_DEACT" VARCHAR2(2), 
	"SAS_PROD_ID" VARCHAR2(15)
   ) SEGMENT CREATION DEFERRED 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  TABLESPACE "IBLGRPHCM" ;


-- IBLGRPHCM.USERS definition

CREATE TABLE "USERS" 
   (	"USER_ID" VARCHAR2(10), 
	"USER_NM" VARCHAR2(120), 
	"USER_PW" VARCHAR2(35)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "IBLGRPHCM" ;