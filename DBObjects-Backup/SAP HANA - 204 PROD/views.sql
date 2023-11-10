CREATE VIEW ETL.ADRC_VIEW AS
SELECT
CLIENT MANDT ,ADDRNUMBER,DATE_FROM ,DATE_TO ,STR_SUPPL1 ,STR_SUPPL2 ,STR_SUPPL3 
from SAPABAP1.adrc a  WHERE CLIENT=300;

CREATE VIEW ETL.BSEG_DETAILS AS
SELECT
	b.MANDT
	,a.BUDAT 
	, b.BUKRS
	, b.BELNR
	, b.GJAHR
	, BUZEI
	, BUZID
	, AUGDT
	, AUGCP
	, AUGBL
	, BSCHL
	, KOART
	, UMSKZ
	, UMSKS
	, ZUMSK
	, SHKZG
	, GSBER
	, PARGB
	, MWSKZ
	, QSSKZ
	, DMBTR
	, WRBTR
	, KZBTR
	, PSWBT
	, PSWSL
	, TXBHW
	, TXBFW
	, MWSTS
	, WMWST
	, HWBAS
	, FWBAS
	, HWZUZ
	, FWZUZ
	, SHZUZ
	, STEKZ
	, MWART
	, TXGRP
	, KTOSL
	, QSSHB
	, KURSR
	, GBETR
	, BDIFF
	, BDIF2
	, VALUT
	, ZUONR
	, SGTXT
	, ZINKZ
	, VBUND
	, BEWAR
	, ALTKT
	, VORGN
	, FDLEV
	, FDGRP
	, FDWBT
	, FDTAG
	, FKONT
	, KOKRS
	, KOSTL
	, PROJN
	, AUFNR
	, VBELN
	, VBEL2
	, POSN2
	, ETEN2
	, ANLN1
	, ANLN2
	, ANBWA
	, BZDAT
	, PERNR
	, XUMSW
	, XHRES
	, XKRES
	, XOPVW
	, XCPDD
	, XSKST
	, XSAUF
	, XSPRO
	, XSERG
	, XFAKT
	, XUMAN
	, XANET
	, XSKRL
	, XINVE
	, XPANZ
	, XAUTO
	, XNCOP
	, XZAHL
	, SAKNR
	, HKONT
	, KUNNR
	, LIFNR
	, FILKD
	, XBILK
	, GVTYP
	, HZUON
	, ZFBDT
	, ZTERM
	, ZBD1T
	, ZBD2T
	, ZBD3T
	, ZBD1P
	, ZBD2P
	, SKFBT
	, SKNTO
	, WSKTO
	, ZLSCH
	, ZLSPR
	, ZBFIX
	, HBKID
	, BVTYP
	, NEBTR
	, MWSK1
	, DMBT1
	, WRBT1
	, MWSK2
	, DMBT2
	, WRBT2
	, MWSK3
	, DMBT3
	, WRBT3
	, REBZG
	, REBZJ
	, REBZZ
	, REBZT
	, ZOLLT
	, ZOLLD
	, LZBKZ
	, LANDL
	, DIEKZ
	, SAMNR
	, ABPER
	, VRSKZ
	, VRSDT
	, DISBN
	, DISBJ
	, DISBZ
	, WVERW
	, ANFBN
	, ANFBJ
	, ANFBU
	, ANFAE
	, BLNBT
	, BLNKZ
	, BLNPZ
	, MSCHL
	, MANSP
	, MADAT
	, MANST
	, MABER
	, ESRNR
	, ESRRE
	, ESRPZ
	, KLIBT
	, QSZNR
	, QBSHB
	, QSFBT
	, NAVHW
	, NAVFW
	, MATNR
	, WERKS
	, MENGE
	, MEINS
	, ERFMG
	, ERFME
	, BPMNG
	, BPRME
	, EBELN
	, EBELP
	, ZEKKN
	, ELIKZ
	, VPRSV
	, PEINH
	, BWKEY
	, BWTAR
	, BUSTW
	, REWRT
	, REWWR
	, BONFB
	, BUALT
	, PSALT
	, NPREI
	, TBTKZ
	, SPGRP
	, SPGRM
	, SPGRT
	, SPGRG
	, SPGRV
	, SPGRQ
	, STCEG
	, EGBLD
	, EGLLD
	, RSTGR
	, RYACQ
	, RPACQ
	, RDIFF
	, RDIF2
	, PRCTR
	, XHKOM
	, VNAME
	, RECID
	, EGRUP
	, VPTNR
	, VERTT
	, VERTN
	, VBEWA
	, DEPOT
	, TXJCD
	, IMKEY
	, DABRZ
	, POPTS
	, FIPOS
	, KSTRG
	, NPLNR
	, AUFPL
	, APLZL
	, PROJK
	, PAOBJNR
	, PASUBNR
	, SPGRS
	, SPGRC
	, BTYPE
	, ETYPE
	, XEGDR
	, LNRAN
	, HRKFT
	, DMBE2
	, DMBE3
	, DMB21
	, DMB22
	, DMB23
	, DMB31
	, DMB32
	, DMB33
	, MWST2
	, MWST3
	, NAVH2
	, NAVH3
	, SKNT2
	, SKNT3
	, BDIF3
	, RDIF3
	, HWMET
	, GLUPM
	, XRAGL
	, UZAWE
	, LOKKT
	, FISTL
	, GEBER
	, STBUK
	, TXBH2
	, TXBH3
	, PPRCT
	, XREF1
	, XREF2
	, KBLNR
	, KBLPOS
	, STTAX
	, FKBER
	, OBZEI
	, XNEGP
	, RFZEI
	, CCBTC
	, KKBER
	, EMPFB
	, XREF3
	, DTWS1
	, DTWS2
	, DTWS3
	, DTWS4
	, GRICD
	, GRIRG
	, GITYP
	, XPYPR
	, KIDNO
	, ABSBT
	, IDXSP
	, LINFV
	, KONTT
	, KONTL
	, TXDAT
	, AGZEI
	, PYCUR
	, PYAMT
	, BUPLA
	, SECCO
	, LSTAR
	, CESSION_KZ
	, PRZNR
	, PPDIFF
	, PPDIF2
	, PPDIF3
	, PENLC1
	, PENLC2
	, PENLC3
	, PENFC
	, PENDAYS
	, b.PENRC
	, GRANT_NBR
	, SCTAX
	, FKBER_LONG
	, GMVKZ
	, SRTYPE
	, INTRENO
	, MEASURE
	, AUGGJ
	, PPA_EX_IND
	, DOCLN
	, SEGMENT
	, PSEGMENT
	, PFKBER
	, HKTID
	, KSTAR
	, XLGCLR
	, TAXPS
	, PAYS_PROV
	, PAYS_TRAN
	, MNDID
	, XFRGE_BSEG
	, PGEBER
	, PGRANT_NBR
	, BUDGET_PD
	, PBUDGET_PD
	, PEROP_BEG
	, PEROP_END
	, FASTPAY
	, IGNR_IVREF
	, FMFGUS_KEY
	, FMXDOCNR
	, FMXYEAR
	, FMXDOCLN
	, FMXZEKKN
	, PRODPER
	, SQUAN
	, J_1TPBUPL
FROM
	SAPABAP1.BSEG b
	LEFT OUTER JOIN SAPABAP1.bkpf a ON (a.mandt=b.MANDT  AND  a.BUKRS=b.BUKRS AND a.BELNR=b.BELNR AND a.GJAHR=b.GJAHR)
	 WHERE 1=1 AND a.MANDT =300 AND b.BUKRS  IN (1900,1600,1500,1100,1200,1000);

CREATE VIEW ETL.CONDITION_TYPE_TEXT AS
SELECT "MANDT" , "SPRAS" , "KVEWE" , "KAPPL" , "KSCHL" , "VTEXT" FROM SAPABAP1.T685T WHERE 1=1 AND MANDT =300 AND SPRAS ='E';

CREATE VIEW ETL.CUSTOMER_LOCATIONS AS
SELECT 
KNA1.KUNNR CUSTOMER_CODE,KNA1.SORTL MAPPING_CUSTOMER_CODE,  KNA1.NAME1 CUSTOMER_NAME,KNA1.ADRNR,KNVV.VKORG COMPANY,KNVV.VKBUR LOCATION,TVKBT.BEZEI LOCATION_DESC
 FROM SAPABAP1.KNA1
 INNER JOIN SAPABAP1.KNVV ON (KNVV.MANDT=KNA1.MANDT AND KNVV.KUNNR=KNA1.KUNNR)
 INNER JOIN SAPABAP1.TVKBT ON (TVKBT.MANDT=KNA1.MANDT AND TVKBT.VKBUR=KNVV.VKBUR AND TVKBT.SPRAS='E')
 
 WHERE 1=1 AND KNA1.MANDT=300
 AND KNVV.VKORG IN (6100,6300);

CREATE VIEW ETL.CUSTOMER_WTIH_ADDRESS AS
SELECT 
KNA1.KUNNR ,KNA1 .NAME1 ,KNA1 .ADRNR ,ADRC.STR_SUPPL1 ADD1,ADRC.STR_SUPPL2 ADD2,ADRC.STR_SUPPL3 ADD3
FROM SAPABAP1.KNA1 KNA1 
LEFT OUTER JOIN SAPABAP1.ADRC ADRC ON (ADRC.ADDRNUMBER=KNA1.ADRNR)
WHERE KNA1.MANDT=300;

CREATE VIEW ETL.DUPLICATE_MARKITT_ITEMS AS
SELECT "MI2"."NORMT" , "MI2"."MATNR" , "MI2"."MAKTX" , "MI2"."ERNAM" , "MI2"."MSTAE" , "MI2"."ERSDA" FROM MARKITT_ITEMS mi2  WHERE 1=1 AND NORMT  IN (
SELECT NORMT  
FROM MARKITT_ITEMS mi 
GROUP BY NORMT 
HAVING count(*)>1 
ORDER BY NORMT 
) ORDER BY NORMT;

CREATE VIEW ETL.ECC_BILL_EXECUTION_DATA AS
SELECT
	'ECC' CONN_STRING,
	a.mandt client,
	A.BUKRS company,
	COMP.BUTXT COMPANY_DESC,
	A.CPUDT document_date,
	A.USNAM user_id,
	A.BLART document_type,
	B.LTEXT document_desc,
	C.WERKS plant,PLNT.NAME1 PLANT_DESC,
	a.budat transaction_date,
	COUNT( DISTINCT A.BELNR ) AS record_count
FROM
	SAPABAP1.BKPF AS A
INNER JOIN SAPABAP1.T003T AS B ON
	A.BLART = B.BLART
	AND a.mandt = b.mandt
INNER JOIN SAPABAP1.BSEG AS C ON
	(A.BELNR = C.BELNR
		AND C.GJAHR = A.GJAHR
		AND A.BUKRS = C.BUKRS
		AND a.mandt = c.mandt )
LEFT OUTER JOIN SAPABAP1.t001 AS COMP ON
	(COMP.MANDT = A.MANDT
		AND COMP.BUKRS = A.BUKRS)
LEFT OUTER JOIN SAPABAP1.t001W AS PLNT ON
	(PLNT.MANDT = C.MANDT
		AND PLNT.WERKS = C.WERKS)		
WHERE
	1 = 1
--	AND A.CPUDT BETWEEN  '20230711'	AND  '20230712'
	AND a.mandt = 300
	AND B.SPRAS = 'E'
	--rough
--	AND A.USNAM = 'IMRAN.KAREEM'
--	AND B.LTEXT = 'Goods Issue/Delivery'
--	AND A.CPUDT = '20230620'
GROUP BY
	A.BUKRS,
	CPUDT,
	USNAM,
	A.BLART,
	B.LTEXT,
	C.WERKS,
	a.budat,
	a.mandt,COMP.BUTXT,PLNT.NAME1
	UNION ALL 
	  SELECT
   'ECC' conn_string
   ,300 client
   ,a.bukrs company
   ,COMP.BUTXT COMPANY_DESC
  , A.AEDAT document_date 
   , A.ERNAM user_id 
   ,A.BSART document_type
   , C.BATXT document_desc
   , B.WERKS  plant
   ,PLNT.NAME1 PLANT_DESC
  ,A.AEDAT transaction_date
   ,  COUNT( DISTINCT A.EBELN ) AS record_count 
   FROM SAPABAP1.EKKO AS A
  INNER JOIN SAPABAP1.EKPO AS B ON (A.EBELN = B.EBELN AND B.MANDT=A.MANDT )
  INNER JOIN SAPABAP1.T161T AS C ON (A.BSART = C.BSART  AND C.MANDT=A.MANDT AND A.SPRAS=C.SPRAS)
LEFT OUTER JOIN SAPABAP1.t001 AS COMP ON
	(COMP.MANDT = A.MANDT
		AND COMP.BUKRS = A.BUKRS)
LEFT OUTER JOIN SAPABAP1.t001W AS PLNT ON
	(PLNT.MANDT = a.MANDT
		AND PLNT.WERKS = b.WERKS)			
  WHERE 
  1=1
  AND A.MANDT=300
AND A.SPRAS='E' AND C.SPRAS='E'
  GROUP BY A.AEDAT , A.ERNAM ,A.BSART, A.BUKRS , B.WERKS , C.BATXT,a.bukrs
,COMP.BUTXT,PLNT.NAME1
UNION ALL 
SELECT
'ECC' conn_string,
300 client,	
A.BUKRS company,
COMP.BUTXT COMPANY_DESC,
A.ERDAT document_date,
A.ERNAM user_id,
A.AUART document_type ,
B.TXT document_desc,
A.WERKS plant,
PLNT.NAME1 PLANT_DESC,
A.ERDAT transaction_date,
COUNT( DISTINCT AUFNR ) AS RECORD_COUNT	
FROM
SAPABAP1.AUFK AS A
INNER JOIN SAPABAP1.T003P AS B ON
	(A.AUART = B.AUART AND A.MANDT=B.CLIENT )
LEFT OUTER JOIN SAPABAP1.t001 AS COMP ON (COMP.MANDT = A.MANDT AND COMP.BUKRS = A.BUKRS)
LEFT OUTER JOIN SAPABAP1.t001W AS PLNT ON 	(PLNT.MANDT = a.MANDT
		AND PLNT.WERKS = A.WERKS)		
WHERE
	1 = 1
	AND B.SPRAS = 'E'
GROUP BY
	A.BUKRS,
	A.ERDAT ,
	A.ERNAM ,
	A.AUART ,
	A.WERKS ,
	B.TXT,COMP.BUTXT,PLNT.NAME1;

CREATE VIEW ETL.GROUP_SALES_DATA_VIEW AS
SELECT a.item_category,
    a.org_id,
    a.org_desc,
    a.billing_date,
    a.document_no,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.busline_desc,
    a.channel_desc,
    a.customer_code AS customer_id,
    a.customer_code AS customer_number,
    a.customer_name,
    a.billing_type_text AS sales_order_type,
    a.item_code AS inventory_item_id,
    a.item_code,
    a.item_desc AS description,
    a.unit_selling_price,
    sum(a.quantity) AS quantity,
    COALESCE(sum(a.claim_bonus_quantity), 0) + COALESCE(sum(a.unclaim_bonus_quantity), 0) AS bonus_qty,
    sum(a.claimable_discount) AS claimable_discount,
    sum(a.unclaimable_discount) AS unclaimable_discount,
    sum(a.tax_recoverable) AS tax_recoverable,
    sum(a.gross_amount) AS gross_amount,
        CASE
            WHEN (upper(a.billing_type_text) LIKE '%RETURN%' OR  upper(a.billing_type_text) LIKE '%.RET.%' ) THEN 'RETURN'
            ELSE 'SALES'
        END AS reason_code,
    a.billing_type,
    a.bill_type_desc,
    a.company_code,
    a."ADRNR",
    a.add1,
    a.add2,
    a.add3,
    a.sales_order_no,
    a.bstnk,
    a.item_no,
    a.augru,
    a.return_reason_code,
    sum(a.amount) AS amount
    ,a.VTWEG_DIST_CHANNEL,a.VTWEG_DIST_CHNL_DESC 
   FROM ( 
SELECT '300' AS mandt,
            psd.company_code,
            psd.org_id,
            loc."BEZEI" AS org_desc,
            psd.sales_order_no,
            psd.sales_order_type,
            psd.document_no,
            psd.billing_type_text,
            psd.billing_date,
            psd.billing_type,
            psd.booker_id,
            booker_name."ENAME" AS booker_name,
            psd.supplier_id,
            supplier_name."ENAME" AS supplier_name,
            psd.business_line_id,
            busline."BEZEI" AS busline_desc,
            psd.customer_code,
            cust."NAME1" AS customer_name,
            psd.channel,
            ---chnl."KTEXT" AS channel_desc,
            psd.VTWEG_DIST_CHNL_DESC  AS channel_desc,
            psd.material_code AS item_code,
            psd.item_desc,
            psd.item_category_text,
            psd.item_category,
            psd.unit,
            psd.billing_cancelled,
                CASE
                        WHEN ( 
                              upper(psd.billing_type_text) like '%CANCEL%'   
                              OR upper(psd.billing_type_text) LIKE '%RETURN%' 
                              OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) AND  upper(psd.billing_type_text) LIKE '%CANCEL%MEMO%'  THEN psd.amount * -1
                    ELSE psd.amount
                END AS amount,
                CASE
                    WHEN upper(psd.item_category_text) like '%BONUS%' THEN 0
                    ELSE
                    CASE
                        WHEN ( 
                              upper(psd.billing_type_text) like '%CANCEL%'   
                              OR upper(psd.billing_type_text) LIKE '%RETURN%' 
                              OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) AND  upper(psd.billing_type_text) LIKE '%CANCEL%MEMO%'   THEN psd.quantity * -1
                        ELSE psd.quantity
                    END
                END AS quantity,
            psd.unit_selling_price,
                CASE
                    WHEN ( 
                              upper(psd.billing_type_text) LIKE '%RETURN%' 
                              OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) THEN 'RETURN'
                    ELSE 'SALES'
                END AS reason_code,
                CASE
                    WHEN ( 
                              upper(psd.billing_type_text) like '%CANCEL%'   
                              OR upper(psd.billing_type_text) LIKE '%RETURN%' 
                              OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) AND  upper(psd.billing_type_text) LIKE '%CANCEL%MEMO%' 
                              AND psd.claim_bonus_quantity > 0 THEN psd.claim_bonus_quantity * -1
                    ELSE psd.claim_bonus_quantity
                END AS claim_bonus_quantity,
                CASE
                    WHEN ( 
                              upper(psd.billing_type_text) like '%CANCEL%'   
                              OR upper(psd.billing_type_text) LIKE '%RETURN%' 
                              OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) AND  upper(psd.billing_type_text) LIKE '%CANCEL%MEMO%'    AND psd.unclaim_bonus_quantity > 0 THEN psd.unclaim_bonus_quantity * -1
                    ELSE psd.unclaim_bonus_quantity
                END AS unclaim_bonus_quantity,
                CASE
                    WHEN ( 
                              upper(psd.billing_type_text) like '%CANCEL%'   
                              OR upper(psd.billing_type_text) LIKE '%RETURN%' 
                              OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) AND  upper(psd.billing_type_text) LIKE '%CANCEL%MEMO%' 
                    AND psd.claimable_discount > 0 THEN psd.claimable_discount * -1
                    ELSE psd.claimable_discount
                END AS claimable_discount,
                CASE
                    WHEN ( 
                              upper(psd.billing_type_text) like '%CANCEL%'   
                              OR upper(psd.billing_type_text) LIKE '%RETURN%' 
                              OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) AND  upper(psd.billing_type_text) LIKE '%CANCEL%MEMO%' 
                    AND psd.unclaimable_discount > 0 THEN psd.unclaimable_discount  * -1
                    ELSE psd.unclaimable_discount
                END AS unclaimable_discount,
           CASE WHEN 
           ( 
                              upper(psd.billing_type_text) like '%CANCEL%'   
                              OR upper(psd.billing_type_text) LIKE '%RETURN%' 
                              OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) AND  upper(psd.billing_type_text) NOT LIKE '%CANCEL%MEMO%'  THEN  psd.tax_recoverable*-1
                             ELSE            psd.tax_recoverable END AS tax_recoverable,
           
           
            CASE  WHEN ( 
                         upper(psd.billing_type_text) like '%CANCEL%'   
                         OR upper(psd.billing_type_text) LIKE '%RETURN%' 
                         OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) AND  upper(psd.billing_type_text) LIKE '%CANCEL%MEMO%'  THEN psd.gross_amount*-1 ELSE  psd.gross_amount END gross_amount,
                CASE
                    WHEN psd.payment_type  IN ('ZE01','Z000') THEN 'CASH'                   ELSE 'CREDIT'
                END AS bill_type_desc,
               CASE  WHEN ( 
                          upper(psd.billing_type_text) LIKE '%RETURN%' 
                         OR  upper(psd.billing_type_text) LIKE '%.RET.%' 
                             ) THEN  'Return' ELSE  'Sales' end AS data_flag,
            cust."ADRNR",
            adrc."STR_SUPPL1" AS add1,
            adrc."STR_SUPPL2" AS add2,
            adrc."STR_SUPPL3" AS add3,
            psd.bstnk,
            psd.item_no,
            psd.augru,            
             CASE WHEN  tvaut.BEZEI IS NULL THEN TVAGT.BEZEI  ELSE tvaut.BEZEI  END AS return_reason_code
             ,psd.VTWEG_DIST_CHANNEL ,psd.VTWEG_DIST_CHNL_DESC 
           FROM GROUP_SALES_DETAIL_DATA psd
             LEFT JOIN SAPABAP1."TVKBT" loc ON loc."MANDT"  = '300'  AND loc."VKBUR"  = psd.org_id  AND loc."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."TVM1T" busline ON busline."MANDT"  = '300'  AND busline."MVGR1"  = psd.business_line_id  AND busline."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."T151T" chnl ON chnl."MANDT"  = '300'  AND chnl."KDGRP"  = psd.channel  AND chnl."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."KNA1" cust ON cust."MANDT"  = '300'  AND cust."KUNNR"  = psd.customer_code 
             LEFT JOIN SAPABAP1."PA0001" booker_name ON booker_name."MANDT"  = '300'  AND booker_name."PERNR"  = psd.booker_id  AND booker_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."PA0001" supplier_name ON supplier_name."MANDT"  = '300'  AND supplier_name."PERNR"  = psd.supplier_id  AND supplier_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."ADRC" adrc ON adrc."CLIENT"  = '300'  AND adrc."ADDRNUMBER"  = cust."ADRNR"  AND adrc."DATE_TO"  = '99991231' 
             LEFT JOIN SAPABAP1.tvaut tvaut ON tvaut.augru  = psd.augru AND tvaut.SPRAS = 'E' AND tvaut.MANDT ='300' 
             LEFT JOIN SAPABAP1.TVAGT TVAGT ON TVAGT.ABGRU  = psd.augru AND TVAGT.SPRAS = 'E' AND TVAGT.MANDT ='300'              
             WHERE 1 = 1 
         -- AND psd.billing_date >= '2021-07-03'
          --AND psd.company_code IN('6100' , '6300')
--          AND psd.billing_type_text  NOT IN ('IBL UB Billing Retur'
--          	, 'OPS-Sales Returns' , 'IBL UB Billing Retur')
          	AND psd.document_no IS NOT NULL
          	          	) a
          	          	WHERE 1=1
          	          	--a.document_no='1015002196'
  GROUP BY a.item_category, a.org_id, a.org_desc, a.billing_date, a.document_no, 
  a.booker_id, a.booker_name, a.supplier_id, a.supplier_name, 
  a.business_line_id, a.busline_desc, a.channel_desc, a.customer_code, 
  a.customer_name, a.billing_type_text, a.item_code, a.item_desc, a.unit_selling_price, (
        CASE
	        WHEN
            (
            	upper(a.billing_type_text) LIKE '%RETURN%' OR  upper(a.billing_type_text) LIKE '%.RET.%' 
            )  THEN 'RETURN' 
            ELSE 'SALES' 
        END)        
        , a.billing_type, a.bill_type_desc, a.company_code, a.ADRNR, a.add1, a.add2, 
        a.add3, a.sales_order_no, a.bstnk, a.item_no, a.augru, a.return_reason_code
       ,VTWEG_DIST_CHANNEL,VTWEG_DIST_CHNL_DESC;

CREATE VIEW ETL.GROUP_SALES_VIEW AS
SELECT a.item_category,
    a.org_id,
    a.org_desc,
    a.trx_date,
    a.trx_number,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.business_line,
    a.channel,
    a.customer_id,
    a.customer_number,
    a.customer_name,
    a.sales_order_type,
    a.inventory_item_id,
    a.item_code,
    a.description,
    a.unit_selling_price,
        CASE
            WHEN upper(a.sales_order_type   ) LIKE  '%CANCEL%MEMO%'    AND a.sold_qty < 0  THEN a.sold_qty * '-1'  
            ELSE a.sold_qty
        END AS sold_qty,
    a.bonus_qty,
        CASE
            WHEN 1 = 1 AND upper(a.sales_order_type   ) LIKE  '%CANCEL%MEMO%'    AND a.claimable_discount >= 0  THEN a.claimable_discount * -1
            WHEN a.billing_type    = 'ZORC'    THEN a.claimable_discount * -1 
            WHEN a.company_code    = '6100'    AND a.billing_type    = 'ZUBC'    AND a.claimable_discount > 0  THEN a.claimable_discount * -1  
            ELSE a.claimable_discount
        END AS claimable_discount,
        CASE
            WHEN 1 = 1 AND upper(a.sales_order_type   ) like '%CANCEL%MEMO%'    AND a.unclaimable_discount >= 0  THEN a.unclaimable_discount * -1
            WHEN a.billing_type    = 'ZORC'    THEN a.unclaimable_discount * -1 
            WHEN a.company_code    = '6100'    AND a.billing_type    = 'ZUBC'    AND a.unclaimable_discount > 0  THEN a.unclaimable_discount * -1
            ELSE a.unclaimable_discount
        END AS unclaimable_discount,
    a.tax_recoverable,
        CASE
            WHEN a.item_category   IN  ('ZFCL'  , 'ZFOU'  , 'ZBRC'  , 'ZBRU') THEN a.tax_recoverable
            ELSE
            CASE
                WHEN a.billing_type    IN   ('ZUCC'   , 'ZURB') THEN a.sold_qty * a.unit_selling_price - a.total_discount * -1   + a.tax_recoverable
                ELSE a.sold_qty * a.unit_selling_price - abs(a.total_discount) + a.tax_recoverable
            END
        END AS net_amount,
        CASE
            WHEN a.item_category    IN ('ZFCL'  , 'ZFOU'  , 'ZBRC'   , 'ZBRU') THEN 0 
            ELSE
            CASE
                WHEN a.billing_type    = 'ZORC'    THEN a.sold_qty * a.unit_selling_price * -1 
                ELSE a.sold_qty * a.unit_selling_price
            END
        END AS gross_amount,
    a.total_discount,
    a.customer_trx_id,
    a.reason_code,
    a.bill_type_desc,
    a.company_code,
    a.billing_type,
    a.sales_order_no,
    a.add1,
    a.add2,
    a.add3,
    a.bstnk,
    a.item_no,
    a.return_reason_code
   FROM ( SELECT psdd.item_category,
            psdd.org_id,
            psdd.org_desc,
            psdd.billing_date AS trx_date,
            psdd.document_no AS trx_number,
            psdd.booker_id AS booker_id,
            psdd.booker_name,
            psdd.supplier_id AS supplier_id,
            psdd.supplier_name,
            trim(psdd.business_line_id   ) AS business_line_id,
            psdd.busline_desc AS business_line,
            COALESCE(psdd.channel_desc, 'Blank') AS channel,
            psdd.customer_id,
            psdd.customer_number,
            psdd.customer_name,
            psdd.sales_order_type,
            psdd.inventory_item_id,
            psdd.item_code,
            psdd.description,
            psdd.unit_selling_price,
            psdd.quantity AS sold_qty,
            psdd.bonus_qty,
            psdd.claimable_discount,
            psdd.unclaimable_discount,
                CASE
                    WHEN (psdd.sales_order_type    LIKE '%CANCEL%'    OR psdd.sales_order_type    LIKE  '%RETUR%'   ) 
                    AND upper(psdd.sales_order_type   ) NOT LIKE  '%CANCEL%MEMO%'    AND psdd.tax_recoverable > 0  THEN psdd.tax_recoverable * -1
                    WHEN upper(psdd.sales_order_type   ) LIKE  '%CANCEL%MEMO%'    AND psdd.tax_recoverable < 0  THEN psdd.tax_recoverable * -1  
                    ELSE psdd.tax_recoverable
                END AS tax_recoverable,
            psdd.gross_amount AS net_amount,
            psdd.unclaimable_discount + psdd.claimable_discount AS total_discount,
            psdd.document_no AS customer_trx_id,
            psdd.reason_code,
            psdd.bill_type_desc,
            psdd.company_code,
            psdd.billing_type,
            psdd.sales_order_no,
            psdd.add1,
            psdd.add2,
            psdd.add3,
            psdd.bstnk,
            psdd.item_no,
            psdd.return_reason_code
           FROM GROUP_SALES_DATA_VIEW  psdd
          WHERE 1 = 1 AND          psdd.company_code    IN  ('6100'   , '6300')       
          AND psdd.billing_type IN  ('ZOPC'  , 'ZOCC'   , 'ZUBC'  , 'ZUCC'   , 'ZORE'
          , 'ZORC'   , 'ZURB'  , 'ZUB1'  , 'ZNES'  , 'ZNEC')
          ) a;

CREATE VIEW ETL.IBL_SALE_DISCOUNT_MIS_VW_BO AS
SELECT a.item_category,
    a.org_id,
    a.org_desc,
    a.trx_date,
    a.trx_number,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.business_line,
    a.channel,
    a.customer_id,
    a.customer_number,
    a.customer_name,
    a.sales_order_type,
    a.inventory_item_id,
    a.item_code,
    a.description,
    a.unit_selling_price,
    a.sold_qty,
    a.bonus_qty,
        CASE
            WHEN a.billing_type = 'ZORC' THEN a.claimable_discount * -1
            ELSE a.claimable_discount
        END AS claimable_discount,
        CASE
            WHEN a.billing_type = 'ZORC' THEN a.unclaimable_discount * -1
            ELSE a.unclaimable_discount
        END AS unclaimable_discount,
    a.tax_recoverable,
        CASE
            WHEN a.item_category IN  ('ZFCL', 'ZFOU', 'ZBRC', 'ZBRU') THEN a.tax_recoverable
            ELSE
            CASE
                WHEN a.billing_type IN  ('ZUCC', 'ZURB') THEN a.sold_qty * a.unit_selling_price - a.total_discount * -1 + a.tax_recoverable
                ELSE a.sold_qty * a.unit_selling_price - abs(a.total_discount) + a.tax_recoverable
            END
        END AS net_amount,
        CASE
            WHEN a.item_category IN  ('ZFCL', 'ZFOU', 'ZBRC', 'ZBRU') THEN 0
            ELSE
            CASE
                WHEN a.billing_type = 'ZORC' AND a.billing_type <> 'ZUB1' THEN a.sold_qty * a.unit_selling_price
                ELSE a.sold_qty * a.unit_selling_price
            END
        END AS gross_amount,
    a.total_discount,
    a.customer_trx_id,
    a.reason_code,
    a.bill_type_desc,
    a.company_code,
    a.billing_type,
    a.sales_order_no,
    a.add1,
    a.add2,
    a.add3
   FROM (
SELECT psdd.item_category,
            psdd.org_id,
            psdd.org_desc,
            psdd.trx_date,
            psdd.trx_number,
            psdd.booker_id,
            psdd.booker_name,
            psdd.supplier_id,
            psdd.supplier_name,
            trim(psdd.business_line_id) AS business_line_id,
            psdd.business_line,
            COALESCE(psdd.channel, 'Blank') AS channel,
            psdd.customer_id,
            psdd.customer_number,
            psdd.customer_name,
            psdd.sales_order_type,
            psdd.inventory_item_id,
            psdd.item_code,
            psdd.description,
            psdd.unit_selling_price,
            psdd.sold_qty,
            psdd.bonus_qty,
            psdd.claimable_discount,
            psdd.unclaimable_discount,
            psdd.tax_recoverable,
            psdd.gross_amount AS net_amount,
            psdd.unclaimable_discount + psdd.claimable_discount AS total_discount,
            psdd.customer_number AS customer_trx_id,
            psdd.reason_code,
            psdd.bill_type_desc,
            psdd.company_code,
            psdd.billing_type,
            psdd.sales_order_no,
            psdd.add1,
            psdd.add2,
            psdd.add3
           FROM PHNX_SALES_DATA psdd
          WHERE 1 = 1
          AND psdd.company_code IN  ('6100', '6300')
          AND psdd.billing_type IN  ('ZOPC', 'ZOCC', 'ZUBC', 'ZUCC', 'ZORE', 'ZORC', 'ZURB', 'ZUB1', 'ZNES', 'ZNEC'))a;

CREATE VIEW ETL.KONV_KSCHL_TEXT AS
SELECT 
  
  
A.MANDT,A.KSCHL,B.VTEXT,A.KPOSN
  
 FROM SAPABAP1.konv A 
LEFT OUTER JOIN SAPABAP1.T685T B
	ON(B.MANDT=A.MANDT AND B.KSCHL=A.KSCHL AND B.SPRAS='E')
WHERE 1=1 AND A.MANDT=300;

CREATE VIEW ETL.MARKITT_ERROR_LOG AS
SELECT
	zv.MANDT ,
	zv.BILLNO markitt_document_number,
	CAST(zv.BILLDATE AS DATE)BILLDATE ,
	SAP_ORDER ,
	vbap.VBELN sap_order_number ,
	mi.NORMT barcode,
	vbap.MATNR item_code,
	vbap.ARKTX item_desc ,
	vbap.KWMENG order_qty,
	vbap.NETWR net_value,
	vbap.POSNR item_no,
	vbap.AUFNR order_number,
	vbrp.VBELN invoice_number,
		CASE
		WHEN ifnull(vbap.KLMENG, 0)= 0 THEN 'Insufficient Stock'
		ELSE zv.ERROR_LOG4
	END LOG_REASON,
	'SAP_ERROR_LOG' DATA_FLAG
FROM
	SAPABAP1.ZMARKIT_VBAK zv
INNER JOIN SAPABAP1.VBAP vbap ON
	(vbap.VBELN = zv.SAP_ORDER)
LEFT OUTER JOIN SAPABAP1.vbrp vbrp ON
	(vbrp.AUBEL = vbap.VBELN
		AND vbrp.POSNR = vbap.POSNR)
LEFT OUTER JOIN MARKITT_ITEMS mi ON
	(mi.MATNR = vbap.MATNR)
WHERE
	1 = 1
	AND zv.MANDT = 300
		AND VBRP.vbeln IS NULL
UNION ALL
SELECT
	MANDT ,
	BILLNO MARKITT_DOCUMENT_NUMBER,
	CAST(BILLDATE AS date)BILLDATE,
	NULL SAP_ORDER,
	NULL SAP_ORDER_NUMBER,
	MATERIAL BARCODE,
	NULL ITEM_CODE,
	NULL ITEM_DESC,
	QUANTITY ORDER_QTY,
	AMOUNT NET_VALUE,
	SERIALNO ITEM_NO,
	NULL ORDER_NUMBER,
	NULL INVOICE_NUMBER,
	'Unmapped Barcode/Blocked Barcode' LOG_REASON,
	'INTG_ERROR_LOG' DATA_FLAG
FROM
	sapabap1.ZMARKIT_ITEM_LOG zil
WHERE
	MANDT = 300;

CREATE VIEW ETL.MARKITT_ITEMS AS
SELECT 
a.NORMT,a.MATNR ,b.MAKTX ,a.ERNAM ,a.MSTAE ,a.ERSDA 
 FROM SAPABAP1."MARA" a
INNER JOIN  SAPABAP1."MAKT" b ON a.MATNR=b.MATNR
INNER JOIN SAPABAP1."MARC" c ON a.MATNR=c.MATNR
where  b.SPRAS='E'
AND  a.MANDT='300' 
AND a.MSTAE<> 'Z1' 
AND a.NORMT<>'' AND c.WERKS='3300';

CREATE VIEW ETL.MATNR_WITH_COMPANY_BUSLIEN AS
SELECT 
distinct
 MARA.MANDT,MARA.MATNR,MAKT.MAKTX MATNR_DESC,  MARA.NORMT MAPPING_CODE,MVKE.VKORG COMPANY,MVKE.MVGR1 BUSLINE_ID,TVM1T.BEZEI BUSLINE_DESC
FROM SAPABAP1.MARA 
 LEFT outer JOIN SAPABAP1.MVKE ON (MVKE.MANDT=MARA.MANDT AND MVKE.MATNR=MARA.MATNR AND MVKE.VKORG IN (6100,6300,6200))
LEFT OUTER JOIN SAPABAP1.MAKT ON (MAKT.MANDT=MARA.MANDT AND MAKT.MATNR=MARA.MATNR AND MAKT.SPRAS='E')
LEFT OUTER JOIN SAPABAP1.TVM1T ON (TVM1T.MANDT=MARA.MANDT AND TVM1T.MVGR1=MVKE.MVGR1 AND TVM1T.SPRAS='E')
WHERE 1=1 AND MARA.MANDT=300;

CREATE VIEW ETL.OCT_ZMARKIT AS
SELECT "Z"."MANDT" , "Z"."BILLNO" , "Z"."BRANCH" , "Z"."BILLDATE" , "Z"."SERIALNO" , "Z"."MATERIAL" , "Z"."QUANTITY" , "Z"."RATE" , "Z"."AMOUNT" , "Z"."ITEMDISCOUNTAMOUNT" , "Z"."NETAMOUNT" , "Z"."ITEMGSTAMOUNT" , "Z"."ZMODE" , "Z"."FLAG" FROM SAPABAP1.ZMARKIT z 
WHERE BILLDATE BETWEEN '20221101' AND '20221130';

CREATE VIEW ETL.OCT_ZMARKIT_ITEM AS
SELECT "ZI"."MANDT" , "ZI"."BILLNO" , "ZI"."SERIALNO" , "ZI"."BRANCH" , "ZI"."BILLDATE" , "ZI"."MATERIAL" , "ZI"."QUANTITY" , "ZI"."RATE" , "ZI"."AMOUNT" , "ZI"."ITEMDISCOUNTAMOUNT" , "ZI"."NETAMOUNT" , "ZI"."ITEMGSTAMOUNT" , "ZI"."ZMODE" , "ZI"."FLAG" 
FROM SAPABAP1.ZMARKIT_ITEM zi  
WHERE BILLDATE  BETWEEN '20221001' AND '20221130';

CREATE VIEW ETL.PHNX_LOGISTIC_SALES_DATA_VIEW AS
SELECT a.item_category,
    a.org_id,
    a.org_desc,
    a.billing_date,
    a.document_no,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.busline_desc,
    a.channel_desc,
    a.customer_code AS customer_id,
    a.customer_code AS customer_number,
    a.customer_name,
    a.billing_type_text AS sales_order_type,
    a.item_code AS inventory_item_id,
    a.item_code,
    a.item_desc AS description,
    a.unit_selling_price,
    sum(a.quantity) AS quantity,
    COALESCE(sum(a.claim_bonus_quantity), 0) + COALESCE(sum(a.unclaim_bonus_quantity), 0) AS bonus_qty,
    sum(a.claimable_discount) AS claimable_discount,
    sum(a.unclaimable_discount) AS unclaimable_discount,
    sum(a.tax_recoverable) AS tax_recoverable,
    sum(a.gross_amount) AS gross_amount,
        CASE
            WHEN UPPER(a.billing_type_text) LIKE '%RET%' THEN 'RETURN'
            ELSE 'SALES'
        END AS reason_code,
    a.billing_type,
    a.bill_type_desc,
    a.company_code,
    a."ADRNR",
    a.add1,
    a.add2,
    a.add3,
    a.sales_order_no,
    a.bstnk,
    a.item_no,
    a.augru,
    a.return_reason_code,
    sum(a.amount) AS amount
   FROM ( SELECT '300' AS mandt,
            psd.company_code,
            psd.org_id,
            loc."BEZEI" AS org_desc,
            psd.sales_order_no,
            psd.sales_order_type,
            psd.document_no,
            psd.billing_type_text,
            psd.billing_date,
            psd.billing_type,
            psd.booker_id,
            booker_name."ENAME" AS booker_name,
            psd.supplier_id,
            supplier_name."ENAME" AS supplier_name,
            psd.business_line_id,
            busline."BEZEI" AS busline_desc,
            psd.customer_code,
            cust."NAME1" AS customer_name,
            psd.channel,
            chnl."KTEXT" AS channel_desc,
            psd.material_code AS item_code,
            psd.item_desc,
            psd.item_category_text,
            psd.item_category,
            psd.unit,
            psd.billing_cancelled,
                CASE
                    WHEN UPPER(psd.billing_type_text) LIKE  '%CANCEL%' THEN psd.amount * -1
                    ELSE psd.amount
                END AS amount,
                CASE
                    WHEN upper(psd.item_category_text) like '%BONUS%' THEN 0
                    ELSE
                    CASE
                        WHEN upper(psd.billing_type_text) like '%CANCEL%' 
                        ---AND psd.billing_type <> 'ZUB1' 
                        AND psd.quantity > 0 THEN psd.quantity * -1
                        ELSE psd.quantity
                    END
                END AS quantity,
            psd.unit_selling_price,
                CASE
                    WHEN psd.billing_type_text LIKE  '% RET %' THEN 'RETURN'
                    ELSE 'SALES'
                END AS reason_code,
                CASE
                    WHEN upper(psd.billing_type_text) LIKE  '%CANCEL%'
                    AND psd.claim_bonus_quantity > 0 THEN psd.claim_bonus_quantity * -1
                    ELSE psd.claim_bonus_quantity
                END AS claim_bonus_quantity,
                CASE
                    WHEN upper(psd.billing_type_text) LIKE '%CANCEL%'
                    AND psd.unclaim_bonus_quantity > 0 THEN psd.unclaim_bonus_quantity * -1
                    ELSE psd.unclaim_bonus_quantity
                END AS unclaim_bonus_quantity,
                CASE								
                    WHEN psd.billing_type_text IN ('IBL LOG Sales Inv')
                    AND psd.claimable_discount > 0 THEN psd.claimable_discount * -1
                    ELSE psd.claimable_discount
                END AS claimable_discount,
                CASE
                    WHEN psd.billing_type_text IN  ('IBL LOG Sales Inv')
                    AND psd.unclaimable_discount > 0 THEN psd.unclaimable_discount  * -1
                    ELSE psd.unclaimable_discount
                END AS unclaimable_discount,
            psd.tax_recoverable,
            psd.gross_amount,
                CASE
                    WHEN psd.payment_type = 'Z000' THEN 'CASH'                   ELSE 'CREDIT'
                END AS bill_type_desc,
            'Sales' AS data_flag,
            cust."ADRNR",
            adrc."STR_SUPPL1" AS add1,
            adrc."STR_SUPPL2" AS add2,
            adrc."STR_SUPPL3" AS add3,
            psd.bstnk,
            psd.item_no,
            psd.augru,
             CASE WHEN  tvaut.BEZEI IS NULL THEN TVAGT.BEZEI  ELSE tvaut.BEZEI  END AS return_reason_code
           FROM PHNX_SALES_DETAIL_DATA_LOG psd
             LEFT JOIN SAPABAP1."TVKBT" loc ON loc."MANDT"  = '300'  AND loc."VKBUR"  = psd.org_id  AND loc."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."TVM1T" busline ON busline."MANDT"  = '300'  AND busline."MVGR1"  = psd.business_line_id  AND busline."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."T151T" chnl ON chnl."MANDT"  = '300'  AND chnl."KDGRP"  = psd.channel  AND chnl."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."KNA1" cust ON cust."MANDT"  = '300'  AND cust."KUNNR"  = psd.customer_code 
             LEFT JOIN SAPABAP1."PA0001" booker_name ON booker_name."MANDT"  = '300'  AND booker_name."PERNR"  = psd.booker_id  AND booker_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."PA0001" supplier_name ON supplier_name."MANDT"  = '300'  AND supplier_name."PERNR"  = psd.supplier_id  AND supplier_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."ADRC" adrc ON adrc."CLIENT"  = '300'  AND adrc."ADDRNUMBER"  = cust."ADRNR"  AND adrc."DATE_TO"  = '99991231' 
             LEFT JOIN SAPABAP1.tvaut tvaut ON tvaut.augru  = psd.augru AND tvaut.SPRAS = 'E' AND tvaut.MANDT ='300' 
             LEFT JOIN SAPABAP1.TVAGT TVAGT ON TVAGT.ABGRU  = psd.augru AND TVAGT.SPRAS = 'E' AND TVAGT.MANDT ='300'              
             WHERE 1 = 1 
          AND psd.billing_date >= '2021-07-03'
          AND psd.company_code IN('6200')
          AND psd.billing_type_text  NOT IN ('LOGs Ret Billing')
          	AND psd.document_no IS NOT NULL
        UNION ALL
         SELECT '300'  AS mandt,
            psd.company_code,
            psd.org_id,
            loc."BEZEI" AS org_desc,
            psd.sales_order_no,
            psd.sales_order_type,
            psd.document_no,
            psd.billing_type_text,
            psd.billing_date,
            psd.billing_type,
            psd.booker_id,
            booker_name."ENAME"  AS booker_name,
            psd.supplier_id,
            supplier_name."ENAME"  AS supplier_name,
            psd.business_line_id,
            busline."BEZEI" AS busline_desc,
            psd.customer_code,
            cust."NAME1" AS customer_name,
            psd.channel,
            chnl."KTEXT" AS channel_desc,
            psd.material_code AS item_code,
            psd.item_desc,
            psd.item_category_text,
            psd.item_category,
            psd.unit,
            psd.billing_cancelled,
                CASE
                    WHEN UPPER(psd.billing_type_text) LIKE  '%CANCE%'  THEN psd.amount * -1
                    ELSE psd.amount
                END AS amount,
                CASE
                    WHEN upper(psd.item_category_text) LIKE  '%BONUS%'  THEN 0
                    ELSE
                    CASE
                        WHEN psd.quantity > 0 THEN psd.quantity * -1
                        ELSE psd.quantity
                    END
                END AS quantity,
            psd.unit_selling_price,
                CASE
                    WHEN psd.billing_type_text  = 'LOGs Ret Billing'  THEN 'RETURN' 
                    ELSE 'SALES' 
                END AS reason_code,
            psd.claim_bonus_quantity,
            psd.unclaim_bonus_quantity,
                CASE
                    WHEN psd.billing_type_text  NOT IN  ('IBL LOG Sales Inv') 
                    AND psd.claimable_discount < 0 THEN psd.claimable_discount * -1
                    ELSE psd.claimable_discount
                END AS claimable_discount,
                CASE
                    WHEN psd.billing_type_text  NOT IN  ('IBL LOG Sales Inv') 
                       AND psd.unclaimable_discount < 0 THEN psd.unclaimable_discount * -1
                    ELSE psd.unclaimable_discount
                END AS unclaimable_discount,
                CASE
                    WHEN psd.tax_recoverable > 0 THEN psd.tax_recoverable * -1
                    ELSE psd.tax_recoverable
                END AS tax_recoverable,
            psd.gross_amount,
                CASE
                    WHEN psd.payment_type  = 'Z000'  THEN 'CASH' 
                    ELSE 'CREDIT' 
                END AS bill_type_desc,
            'Return'  AS data_flag,
            cust.ADRNR,
            adrc."STR_SUPPL1" AS add1,
            adrc."STR_SUPPL2" AS add2,
            adrc."STR_SUPPL3" AS add3,
            psd.bstnk,
            psd.item_no,
            psd.augru,
--            tvaut.bezei AS return_reason_code
                         CASE WHEN  tvaut.BEZEI IS NULL THEN TVAGT.BEZEI  ELSE tvaut.BEZEI  END AS return_reason_code
           FROM PHNX_SALES_DETAIL_DATA_LOG psd
             LEFT JOIN SAPABAP1."TVKBT" loc ON loc."MANDT"  = '300'  AND loc."VKBUR"  = psd.org_id  AND loc."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."TVM1T" busline ON busline."MANDT"  = '300'  AND busline."MVGR1"  = psd.business_line_id  AND busline."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."T151T" chnl ON chnl."MANDT"  = '300'  AND chnl."KDGRP"  = psd.channel  AND chnl."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."KNA1" cust ON cust."MANDT"  = '300'  AND cust."KUNNR"  = psd.customer_code 
             LEFT JOIN SAPABAP1."PA0001" booker_name ON booker_name."MANDT"  = '300'  AND booker_name."PERNR"  = psd.booker_id  AND booker_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."PA0001" supplier_name ON supplier_name."MANDT"  = '300'  AND supplier_name."PERNR"  = psd.supplier_id  AND supplier_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."ADRC" adrc ON adrc."CLIENT"  = '300'  AND adrc."ADDRNUMBER"  = cust."ADRNR"  AND adrc."DATE_TO"  = '99991231' 
             LEFT JOIN SAPABAP1.tvaut tvaut ON tvaut.augru  = psd.augru AND tvaut.SPRAS = 'E' AND tvaut.MANDT ='300' 
             LEFT JOIN SAPABAP1.TVAGT TVAGT ON TVAGT.ABGRU  = psd.augru AND TVAGT.SPRAS = 'E' AND TVAGT.MANDT ='300' 
          WHERE 1 = 1 AND psd.billing_type_text  IN  ('LOGs Ret Billing') AND psd.document_no  <> '' 
          	AND psd.billing_date >= '2021-07-03' AND psd.company_code  IN ('6200')
          	) a
  GROUP BY a.item_category, a.org_id, a.org_desc, a.billing_date, a.document_no, 
  a.booker_id, a.booker_name, a.supplier_id, a.supplier_name, 
  a.business_line_id, a.busline_desc, a.channel_desc, a.customer_code, 
  a.customer_name, a.billing_type_text, a.item_code, a.item_desc, a.unit_selling_price, (
        CASE
            WHEN a.billing_type_text  = 'IBL UB Billing Retur'  THEN 'RETURN' 
            ELSE 'SALES' 
        END), a.billing_type, a.bill_type_desc, a.company_code, a.ADRNR, a.add1, a.add2, 
        a.add3, a.sales_order_no, a.bstnk, a.item_no, a.augru, a.return_reason_code;

CREATE VIEW ETL.PHNX_LOGISTIC_SALES_VIEW AS
SELECT a.item_category,
    a.org_id,
    a.org_desc,
    a.trx_date,
    a.trx_number,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.business_line,
    a.channel,
    a.customer_id,
    a.customer_number,
    a.customer_name,
    a.sales_order_type,
    a.inventory_item_id,
    a.item_code,
    a.description,
    a.unit_selling_price,
        CASE
            WHEN upper(a.sales_order_type) LIKE  '%CANCEL%MEMO%'    AND a.sold_qty < 0  THEN a.sold_qty * '-1'  
            ELSE a.sold_qty
        END AS sold_qty,
    a.bonus_qty,
        CASE
            WHEN 1 = 1 AND upper(a.sales_order_type) LIKE  '%CANCEL%MEMO%'    AND a.claimable_discount >= 0  THEN a.claimable_discount * -1
            WHEN upper(a.sales_order_type) LIKE '% CANCE %'    THEN a.claimable_discount * -1 
            WHEN a.company_code    = '6200'    AND a.SALES_ORDER_TYPE    IN ('IBL LOG Sales Inv')  AND a.claimable_discount > 0  THEN a.claimable_discount * -1  
            ELSE a.claimable_discount
        END AS claimable_discount,
        CASE
            WHEN 1 = 1 AND upper(a.sales_order_type ) like '%CANCEL%MEMO%'    AND a.unclaimable_discount >= 0  THEN a.unclaimable_discount * -1
            WHEN a.billing_type    = 'ZORC'    THEN a.unclaimable_discount * -1 
            WHEN a.company_code    = '6200'   AND a.sales_order_type    IN ('IBL LOG Sales Inv')   AND a.unclaimable_discount > 0  THEN a.unclaimable_discount * -1
            ELSE a.unclaimable_discount
        END AS unclaimable_discount,
    a.tax_recoverable,
        CASE
            WHEN a.item_category   IN  ('ZFCL'  , 'ZFOU'  , 'ZBRC'  , 'ZBRU') THEN a.tax_recoverable
            ELSE
            CASE
                WHEN upper(a.SALES_ORDER_TYPE) LIKE '% CANCE %' OR  upper(a.SALES_ORDER_TYPE) LIKE '% RET %'  THEN a.sold_qty * a.unit_selling_price - a.total_discount * -1   + a.tax_recoverable
                ELSE a.sold_qty * a.unit_selling_price - abs(a.total_discount) + a.tax_recoverable
            END
        END AS net_amount,        
        CASE
            WHEN a.item_category    IN ('ZFCL'  , 'ZFOU'  , 'ZBRC'   , 'ZBRU') THEN 0 
            ELSE
            CASE
                WHEN upper(a.SALES_ORDER_TYPE) LIKE '%CANCE%MEMO%'    THEN a.sold_qty * a.unit_selling_price * -1 
                ELSE a.sold_qty * a.unit_selling_price
            END
        END AS gross_amount,
    a.total_discount,
    a.customer_trx_id,
    a.reason_code,
    a.bill_type_desc,
    a.company_code,
    a.billing_type,
    a.sales_order_no,
    a.add1,
    a.add2,
    a.add3,
    a.bstnk,
    a.item_no,
    a.return_reason_code
   FROM ( SELECT psdd.item_category,
            psdd.org_id,
            psdd.org_desc,
            psdd.billing_date AS trx_date,
            psdd.document_no AS trx_number,
            psdd.booker_id AS booker_id,
            psdd.booker_name,
            psdd.supplier_id AS supplier_id,
            psdd.supplier_name,
            trim(psdd.business_line_id   ) AS business_line_id,
            psdd.busline_desc AS business_line,
            COALESCE(psdd.channel_desc, 'Blank') AS channel,
            psdd.customer_id,
            psdd.customer_number,
            psdd.customer_name,
            psdd.sales_order_type,
            psdd.inventory_item_id,
            psdd.item_code,
            psdd.description,
            psdd.unit_selling_price,
            psdd.quantity AS sold_qty,
            psdd.bonus_qty,
            psdd.claimable_discount,
            psdd.unclaimable_discount,
                CASE
                    WHEN (psdd.sales_order_type    LIKE '%CANCEL%'    OR psdd.sales_order_type    LIKE  '%RETUR%'   ) 
                    AND upper(psdd.sales_order_type   ) NOT LIKE  '%CANCEL%MEMO%'    AND psdd.tax_recoverable > 0  THEN psdd.tax_recoverable * -1
                    WHEN upper(psdd.sales_order_type   ) LIKE  '%CANCEL%MEMO%'    AND psdd.tax_recoverable < 0  THEN psdd.tax_recoverable * -1  
                    ELSE psdd.tax_recoverable
                END AS tax_recoverable,
            psdd.gross_amount AS net_amount,
            psdd.unclaimable_discount + psdd.claimable_discount AS total_discount,
            psdd.document_no AS customer_trx_id,
            psdd.reason_code,
            psdd.bill_type_desc,
            psdd.company_code,
            psdd.billing_type,
            psdd.sales_order_no,
            psdd.add1,
            psdd.add2,
            psdd.add3,
            psdd.bstnk,
            psdd.item_no,
            psdd.return_reason_code
           FROM /*PHNX_SALES_DATA_VIEW*/ PHNX_LOGISTIC_SALES_DATA_VIEW psdd
          WHERE 1 = 1 AND          psdd.company_code    IN  ('6200')       
          AND psdd.billing_type IN  ( 'ZIUP','ZIBB','ZLRB','ZSII','ZCMR','ZCUP','ZSIC','ZLB2','ZIBC')
          ) a;

CREATE VIEW ETL.PHNX_SALE_DATA_VW AS
SELECT "PSD"."ITEM_CATEGORY" , "PSD"."ORG_ID" , "PSD"."ORG_DESC" , "PSD"."TRX_DATE" , "PSD"."TRX_NUMBER" , "PSD"."BOOKER_ID" , "PSD"."BOOKER_NAME" , "PSD"."SUPPLIER_ID" , "PSD"."SUPPLIER_NAME" , "PSD"."BUSINESS_LINE_ID" , "PSD"."BUSINESS_LINE" , "PSD"."CHANNEL" , "PSD"."CUSTOMER_ID" , "PSD"."CUSTOMER_NUMBER" , "PSD"."CUSTOMER_NAME" , "PSD"."SALES_ORDER_TYPE" , "PSD"."INVENTORY_ITEM_ID" , "PSD"."ITEM_CODE" , "PSD"."DESCRIPTION" , "PSD"."UNIT_SELLING_PRICE" , "PSD"."SOLD_QTY" , "PSD"."BONUS_QTY" , "PSD"."CLAIMABLE_DISCOUNT" , "PSD"."UNCLAIMABLE_DISCOUNT" , "PSD"."TAX_RECOVERABLE" , "PSD"."NET_AMOUNT" , "PSD"."GROSS_AMOUNT" , "PSD"."TOTAL_DISCOUNT" , "PSD"."CUSTOMER_TRX_ID" , "PSD"."REASON_CODE" , "PSD"."BILL_TYPE_DESC" , "PSD"."COMPANY_CODE" , "PSD"."BILLING_TYPE" , "PSD"."SALES_ORDER_NO" , "PSD"."ADD1" , "PSD"."ADD2" , "PSD"."ADD3" , "PSD"."BSTNK" , "PSD"."ITEM_NO" , "PSD"."RETURN_REASON_CODE" FROM PHNX_SALES_DATA psd 
WHERE TRX_DATE ='2022-04-11';

CREATE VIEW ETL.PHNX_SALES_DATA_VIEW AS
SELECT a.item_category,
    a.org_id,
    a.org_desc,
    a.billing_date,
    a.document_no,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.busline_desc,
    a.channel_desc,
    a.customer_code AS customer_id,
    a.customer_code AS customer_number,
    a.customer_name,
    a.billing_type_text AS sales_order_type,
    a.item_code AS inventory_item_id,
    a.item_code,
    a.item_desc AS description,
    a.unit_selling_price,
    sum(a.quantity) AS quantity,
    COALESCE(sum(a.claim_bonus_quantity), 0) + COALESCE(sum(a.unclaim_bonus_quantity), 0) AS bonus_qty,
    sum(a.claimable_discount) AS claimable_discount,
    sum(a.unclaimable_discount) AS unclaimable_discount,
    sum(a.tax_recoverable) AS tax_recoverable,
    sum(a.gross_amount) AS gross_amount,
        CASE
            WHEN a.billing_type_text = 'IBL UB Billing Retur' THEN 'RETURN'
            ELSE 'SALES'
        END AS reason_code,
    a.billing_type,
    a.bill_type_desc,
    a.company_code,
    a."ADRNR",
    a.add1,
    a.add2,
    a.add3,
    a.sales_order_no,
    a.bstnk,
    a.item_no,
    a.augru,
    a.return_reason_code,
    sum(a.amount) AS amount
   FROM ( SELECT '300' AS mandt,
            psd.company_code,
            psd.org_id,
            loc."BEZEI" AS org_desc,
            psd.sales_order_no,
            psd.sales_order_type,
            psd.document_no,
            psd.billing_type_text,
            psd.billing_date,
            psd.billing_type,
            psd.booker_id,
            booker_name."ENAME" AS booker_name,
            psd.supplier_id,
            supplier_name."ENAME" AS supplier_name,
            psd.business_line_id,
            busline."BEZEI" AS busline_desc,
            psd.customer_code,
            cust."NAME1" AS customer_name,
            psd.channel,
            chnl."KTEXT" AS channel_desc,
            psd.material_code AS item_code,
            psd.item_desc,
            psd.item_category_text,
            psd.item_category,
            psd.unit,
            psd.billing_cancelled,
                CASE
                    WHEN psd.billing_type = 'ZOCC' THEN psd.amount * -1
                    ELSE psd.amount
                END AS amount,
                CASE
                    WHEN upper(psd.item_category_text) like '%BONUS%' THEN 0
                    ELSE
                    CASE
                        WHEN upper(psd.billing_type_text) like '%CANCEL%' AND psd.billing_type <> 'ZUB1' AND psd.quantity > 0 THEN psd.quantity * -1
                        ELSE psd.quantity
                    END
                END AS quantity,
            psd.unit_selling_price,
                CASE
                    WHEN psd.billing_type_text = 'OPS-Sales Returns' THEN 'RETURN'
                    ELSE 'SALES'
                END AS reason_code,
                CASE
                    WHEN upper(psd.billing_type_text) LIKE  '%CANCEL%'
                    AND psd.claim_bonus_quantity > 0 THEN psd.claim_bonus_quantity * -1
                    ELSE psd.claim_bonus_quantity
                END AS claim_bonus_quantity,
                CASE
                    WHEN upper(psd.billing_type_text) LIKE '%CANCEL%'
                    AND psd.unclaim_bonus_quantity > 0 THEN psd.unclaim_bonus_quantity * -1
                    ELSE psd.unclaim_bonus_quantity
                END AS unclaim_bonus_quantity,
                CASE
                    WHEN psd.billing_type_text IN ('OPS Sales Tax Cash','UB Sales Tax Cash', 'Bill Near Exp Sales')
                    AND psd.claimable_discount > 0 THEN psd.claimable_discount * -1
                    ELSE psd.claimable_discount
                END AS claimable_discount,
                CASE
                    WHEN psd.billing_type_text IN  ('OPS Sales Tax Cash', 'UB Sales Tax Cash', 'Bill Near Exp Sales')
                    AND psd.unclaimable_discount > 0 THEN psd.unclaimable_discount  * -1
                    ELSE psd.unclaimable_discount
                END AS unclaimable_discount,
            psd.tax_recoverable,
            psd.gross_amount,
                CASE
                    WHEN psd.payment_type = 'Z000' THEN 'CASH'                   ELSE 'CREDIT'
                END AS bill_type_desc,
            'Sales' AS data_flag,
            cust."ADRNR",
            adrc."STR_SUPPL1" AS add1,
            adrc."STR_SUPPL2" AS add2,
            adrc."STR_SUPPL3" AS add3,
            psd.bstnk,
            psd.item_no,
            psd.augru,
             CASE WHEN  tvaut.BEZEI IS NULL THEN TVAGT.BEZEI  ELSE tvaut.BEZEI  END AS return_reason_code
           FROM PHNX_SALES_DETAIL_DATA psd
             LEFT JOIN SAPABAP1."TVKBT" loc ON loc."MANDT"  = '300'  AND loc."VKBUR"  = psd.org_id  AND loc."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."TVM1T" busline ON busline."MANDT"  = '300'  AND busline."MVGR1"  = psd.business_line_id  AND busline."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."T151T" chnl ON chnl."MANDT"  = '300'  AND chnl."KDGRP"  = psd.channel  AND chnl."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."KNA1" cust ON cust."MANDT"  = '300'  AND cust."KUNNR"  = psd.customer_code 
             LEFT JOIN SAPABAP1."PA0001" booker_name ON booker_name."MANDT"  = '300'  AND booker_name."PERNR"  = psd.booker_id  AND booker_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."PA0001" supplier_name ON supplier_name."MANDT"  = '300'  AND supplier_name."PERNR"  = psd.supplier_id  AND supplier_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."ADRC" adrc ON adrc."CLIENT"  = '300'  AND adrc."ADDRNUMBER"  = cust."ADRNR"  AND adrc."DATE_TO"  = '99991231' 
             LEFT JOIN SAPABAP1.tvaut tvaut ON tvaut.augru  = psd.augru AND tvaut.SPRAS = 'E' AND tvaut.MANDT ='300' 
             LEFT JOIN SAPABAP1.TVAGT TVAGT ON TVAGT.ABGRU  = psd.augru AND TVAGT.SPRAS = 'E' AND TVAGT.MANDT ='300'              
             WHERE 1 = 1 
          AND psd.billing_date >= '2021-07-03'
          AND psd.company_code IN('6100' , '6300')
          AND psd.billing_type_text  NOT IN ('IBL UB Billing Retur'
          	, 'OPS-Sales Returns' , 'IBL UB Billing Retur')
          	AND psd.document_no IS NOT NULL
        UNION ALL
         SELECT '300'  AS mandt,
            psd.company_code,
            psd.org_id,
            loc."BEZEI" AS org_desc,
            psd.sales_order_no,
            psd.sales_order_type,
            psd.document_no,
            psd.billing_type_text,
            psd.billing_date,
            psd.billing_type,
            psd.booker_id,
            booker_name."ENAME"  AS booker_name,
            psd.supplier_id,
            supplier_name."ENAME"  AS supplier_name,
            psd.business_line_id,
            busline."BEZEI" AS busline_desc,
            psd.customer_code,
            cust."NAME1" AS customer_name,
            psd.channel,
            chnl."KTEXT" AS channel_desc,
            psd.material_code AS item_code,
            psd.item_desc,
            psd.item_category_text,
            psd.item_category,
            psd.unit,
            psd.billing_cancelled,
                CASE
                    WHEN psd.billing_type  = 'ZOCC'  THEN psd.amount * -1
                    ELSE psd.amount
                END AS amount,
                CASE
                    WHEN upper(psd.item_category_text) LIKE  '%BONUS%'  THEN 0
                    ELSE
                    CASE
                        WHEN psd.quantity > 0 THEN psd.quantity * -1
                        ELSE psd.quantity
                    END
                END AS quantity,
            psd.unit_selling_price,
                CASE
                    WHEN psd.billing_type_text  = 'OPS-Sales Returns'  THEN 'RETURN' 
                    ELSE 'SALES' 
                END AS reason_code,
            psd.claim_bonus_quantity,
            psd.unclaim_bonus_quantity,
                CASE
                    WHEN psd.billing_type_text  NOT IN  ('OPS Sales Tax Cash' 
                    , 'UB Sales Tax Cash' , 'Bill Near Exp Sales') 
                    AND psd.claimable_discount < 0 THEN psd.claimable_discount * -1
                    ELSE psd.claimable_discount
                END AS claimable_discount,
                CASE
                    WHEN psd.billing_type_text  NOT IN  ('OPS Sales Tax Cash' , 'UB Sales Tax Cash' , 'Bill Near Exp Sales') 
                       AND psd.unclaimable_discount < 0 THEN psd.unclaimable_discount * -1
                    ELSE psd.unclaimable_discount
                END AS unclaimable_discount,
                CASE
                    WHEN psd.tax_recoverable > 0 THEN psd.tax_recoverable * -1
                    ELSE psd.tax_recoverable
                END AS tax_recoverable,
            psd.gross_amount,
                CASE
                    WHEN psd.payment_type  = 'Z000'  THEN 'CASH' 
                    ELSE 'CREDIT' 
                END AS bill_type_desc,
            'Return'  AS data_flag,
            cust.ADRNR,
            adrc."STR_SUPPL1" AS add1,
            adrc."STR_SUPPL2" AS add2,
            adrc."STR_SUPPL3" AS add3,
            psd.bstnk,
            psd.item_no,
            psd.augru,
--            tvaut.bezei AS return_reason_code
                         CASE WHEN  tvaut.BEZEI IS NULL THEN TVAGT.BEZEI  ELSE tvaut.BEZEI  END AS return_reason_code
           FROM PHNX_SALES_DETAIL_DATA psd
             LEFT JOIN SAPABAP1."TVKBT" loc ON loc."MANDT"  = '300'  AND loc."VKBUR"  = psd.org_id  AND loc."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."TVM1T" busline ON busline."MANDT"  = '300'  AND busline."MVGR1"  = psd.business_line_id  AND busline."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."T151T" chnl ON chnl."MANDT"  = '300'  AND chnl."KDGRP"  = psd.channel  AND chnl."SPRAS"  = 'E' 
             LEFT JOIN SAPABAP1."KNA1" cust ON cust."MANDT"  = '300'  AND cust."KUNNR"  = psd.customer_code 
             LEFT JOIN SAPABAP1."PA0001" booker_name ON booker_name."MANDT"  = '300'  AND booker_name."PERNR"  = psd.booker_id  AND booker_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."PA0001" supplier_name ON supplier_name."MANDT"  = '300'  AND supplier_name."PERNR"  = psd.supplier_id  AND supplier_name."ENDDA"  = '99991231' 
             LEFT JOIN SAPABAP1."ADRC" adrc ON adrc."CLIENT"  = '300'  AND adrc."ADDRNUMBER"  = cust."ADRNR"  AND adrc."DATE_TO"  = '99991231' 
             LEFT JOIN SAPABAP1.tvaut tvaut ON tvaut.augru  = psd.augru AND tvaut.SPRAS = 'E' AND tvaut.MANDT ='300' 
             LEFT JOIN SAPABAP1.TVAGT TVAGT ON TVAGT.ABGRU  = psd.augru AND TVAGT.SPRAS = 'E' AND TVAGT.MANDT ='300' 
          WHERE 1 = 1 AND psd.billing_type_text  IN  ('IBL UB Billing Retur' , 'OPS-Sales Returns' 
          	, 'IBL UB Billing Retur') AND psd.document_no  <> '' 
          	AND psd.billing_date >= '2021-07-03' AND psd.company_code  IN ('6100' , '6300')
          	) a
  GROUP BY a.item_category, a.org_id, a.org_desc, a.billing_date, a.document_no, 
  a.booker_id, a.booker_name, a.supplier_id, a.supplier_name, 
  a.business_line_id, a.busline_desc, a.channel_desc, a.customer_code, 
  a.customer_name, a.billing_type_text, a.item_code, a.item_desc, a.unit_selling_price, (
        CASE
            WHEN a.billing_type_text  = 'IBL UB Billing Retur'  THEN 'RETURN' 
            ELSE 'SALES' 
        END), a.billing_type, a.bill_type_desc, a.company_code, a.ADRNR, a.add1, a.add2, 
        a.add3, a.sales_order_no, a.bstnk, a.item_no, a.augru, a.return_reason_code;

CREATE VIEW ETL.PHNX_SALES_DATASET AS
select
	 CLIENT,
	 COMPANY,
	 BILLING_DATE,
	 BILLING_DOC,
	 BILLING_TYPE,
	 BILL_TYPE_DESC ,
	 SALES_ORDER_NUMBER,
	 SALES_ORDER_TYPE,
	 BILLING_CATEGORY,
	 BILLING_CATEGORY_DESC ,
	 SD_DOCUMENT_CATEGORY,
	 SD_DOCUMENT_CATEGORY_DESC,
	 SALES_ORGANIZATION,
	 SALES_ORGANIZATION_DESC,
	 BOOKER_ID,
	 BOOKER_NAME ,
	 SUPPLIER_ID,
	 SUPPLIER_NAME,
	 SALES_ORDER_LINE,
	 BUSLINE_ID,
	 BUSLINE_DESC,
	 CUSTOMER_NUMBER,
	 CUSTOMER_NAME ,
	 PAYER ,
	 CHANNEL_ID,
	 CHANNEL ,
	 INVENTORY_ITEM_ID ,
	 ITEM_CODE ,
	 MAPPING_ITEM_CODE,
	 ITEM_DESC ,
	 ITEM_CATEGORY,
	 ITEM_CATEGORY_DESC,
	 CASE WHEN UPPER(ITEM_CATEGORY_DESC) NOT LIKE '%BONUS%' 
THEN SOLD_QTY 
ELSE 0 
END SOLD_QTY,
	 CASE WHEN UPPER(ITEM_CATEGORY_DESC) NOT LIKE '%BONUS%' 
THEN 0 
ELSE BONUS_QTY 
END BONUS_QTY,
	 UNIT_SELLING_PRICE,
	 --CLAIMABLE_DISC,
CASE WHEN DATA_FLAG='DISC' 
AND DISC_FLAG IN ('ZCDP',
	'ZCDV') 
THEN CLAIMABLE_DISC 
ELSE 0 
END CLAIMABLE_DISC,
	 CASE WHEN DATA_FLAG='DISC' 
AND DISC_FLAG IN ('ZUDP',
	'ZUDV') 
THEN CLAIMABLE_DISC 
ELSE 0 
END UN_CLAIMABLE_DISC,
	 -- ZUDP & ZUDV
DATA_FLAG,
	 DISC_FLAG,
	 REF_COLS,
	 VBAK_AUART,
	 KNUMV ,
	 VBRP_FKIMG,
	 VBRP_PSTYV 
from ( SELECT
	 VBRK.MANDT CLIENT ,
	VBRK.BUKRS COMPANY ,
	VBRK.FKDAT BILLING_DATE ,
	VBRK.VBELN BILLING_DOC ,
	VBRK.FKART Billing_Type ,
	TVFKT.VTEXT BILL_TYPE_DESC ,
	VBRP.AUBEL Sales_ORDER_NUMBER ,
	SALES_ORD.BEZEI SALES_ORDER_TYPE ,
	VBRK.FKTYP BILLING_CATEGORY ,
	BCD.DDTEXT Billing_category_DESC ,
	VBRK.VBTYP SD_document_category ,
	SDCD.DDTEXT SD_document_category_DESC ,
	VBRK.VKORG Sales_Organization ,
	SOD.VTEXT Sales_Organization_DESC ,
	BOOKER.PERNR BOOKER_ID,
	BOOKER_NAME.VNAMC BOOKER_NAME ,
	SUPPLIER.PERNR SUPPLIER_ID,
	SUPPLIER_NAME.VNAMC SUPPLIER_NAME ,
	SALES_ORDER_LINE.POSNR SALES_ORDER_LINE ,
	VBRP.MVGR1 BUSLINE_ID,
	BUSLINE.BEZEI BUSLINE_DESC ,
	VBRK.KUNAG CUSTOMER_NUMBER,
	CUST.NAME1 CUSTOMER_NAME,
	VBRK.KUNRG Payer ,
	VBRK.KDGRP CHANNEL_ID,
	CHNL.KTEXT CHANNEL ,
	VBRP.MATNR INVENTORY_ITEM_ID ,
	VBRP.MATNR ITEM_CODE ,
	ITM.NORMT MAPPING_ITEM_CODE ,
	VBRP.ARKTX ITEM_DESC ,
	VBRP.PSTYV item_category ,
	ITMC.VTEXT item_category_desc ,
	VBRP.FKIMG SOLD_QTY ,
	VBRP.FKIMG BONUS_QTY ,
	TP.KBETR UNIT_SELLING_PRICE --,CLMDISC.KBETR
--,Z_GET_CLAIM_UN_DISCOUNT('C',SALES_ORDER_LINE.POSNR, VBRK.KNUMV)
 ,
	0 CLAIMABLE_DISC ,
	'SALES' DATA_FLAG ,
	null disc_flag ,
	'' REF_COLS ,
	VBAK.AUART VBAK_AUART ,
	VBRK.KNUMV ,
	VBRP.FKIMG VBRP_FKIMG ,
	VBRP.PSTYV VBRP_PSTYV 
	FROM SAPABAP1.VBRK AS VBRK 
	INNER JOIN SAPABAP1.VBRP AS VBRP ON (VBRP.MANDT=VBRK.MANDT 
		AND VBRP.VBELN=VBRK.VBELN) 
	LEFT OUTER JOIN SAPABAP1.VBAK AS VBAK ON(VBAK.MANDT=VBRK.MANDT 
		AND VBAK.VBELN=VBRP.AUBEL) --BILL TYPE DESC	

	LEFT OUTER JOIN SAPABAP1.TVFKT AS TVFKT ON (TVFKT.MANDT=VBRK.MANDT 
		AND TVFKT.FKART=VBRK.FKART 
		AND TVFKT.SPRAS='E')--BILL TYPE DESC
 
	LEFT OUTER JOIN SAPABAP1.DD07T AS BCD ON (BCD.DOMVALUE_L=VBRK.FKTYP 
		AND BCD.DDLANGUAGE='E' 
		AND BCD.DOMNAME='FKTYP' ) --BILLING_CATEGORY_DESC
 
	LEFT OUTER JOIN SAPABAP1.DD07T AS SDCD ON (SDCD.DOMVALUE_L=VBRK.VBTYP 
		AND SDCD.DDLANGUAGE='E' 
		AND SDCD.DOMNAME='VBTYP' ) --SD_document_category_DESC
 
	LEFT OUTER JOIN SAPABAP1.TVKOT AS SOD ON (SOD.MANDT=VBRK.MANDT 
		AND SOD.VKORG=VBRK.VKORG 
		AND SOD.SPRAS='E') --Sales_Organization_DESC
 --BOOKER_ID

	LEFT OUTER JOIN SAPABAP1.VBPA AS BOOKER ON(BOOKER.MANDT=VBRK.MANDT 
		AND BOOKER.VBELN=VBRP.AUBEL 
		AND BOOKER.PARVW='BK')--BOOKER ID
 --BOOKER NAME

	LEFT OUTER JOIN SAPABAP1.PA0002 AS BOOKER_NAME ON(BOOKER_NAME.MANDT=VBRK.MANDT 
		AND BOOKER_NAME.PERNR=BOOKER.PERNR)--BOOKER NAME
 --SUPPLIER ID

	LEFT OUTER JOIN SAPABAP1.VBPA AS SUPPLIER ON(SUPPLIER.MANDT=VBRK.MANDT 
		AND SUPPLIER.VBELN=VBRP.AUBEL 
		AND SUPPLIER.PARVW='ZS')--SUPPLIER ID
 --SUPPLIER NAME

	LEFT OUTER JOIN SAPABAP1.PA0002 AS SUPPLIER_NAME ON(SUPPLIER_NAME.MANDT=VBRK.MANDT 
		AND SUPPLIER_NAME.PERNR=SUPPLIER.PERNR)--SUPPLIER NAME
 --SALES ORDER LINE

	LEFT OUTER JOIN SAPABAP1.VBAP SALES_ORDER_LINE ON(SALES_ORDER_LINE.MANDT=VBRP.MANDT 
		AND SALES_ORDER_LINE.VBELN=VBRP.AUBEL 
		AND SALES_ORDER_LINE.POSNR=VBRP.POSNR) --BUSLIEN DESC

	LEFT OUTER JOIN SAPABAP1.TVM1T BUSLINE ON(BUSLINE.MANDT=VBRP.MANDT 
		AND BUSLINE.MVGR1=VBRP.MVGR1 
		AND BUSLINE.SPRAS='E') --CUSTOMER

	LEFT OUTER JOIN SAPABAP1.KNA1 CUST ON(CUST.MANDT=VBRK.MANDT 
		AND CUST.KUNNR=VBRK.KUNAG ) --CHANNEL

	LEFT OUTER JOIN SAPABAP1.T151T CHNL ON(CHNL.MANDT=VBRK.MANDT 
		AND CHNL.KDGRP=VBRK.KDGRP 
		AND CHNL.SPRAS='E') --SALES ORDER TYPE

	LEFT OUTER JOIN SAPABAP1.TVAKT SALES_ORD ON(SALES_ORD.MANDT=VBAK.MANDT 
		AND SALES_ORD.AUART=VBAK.AUART 
		AND SALES_ORD.SPRAS='E') --ITEM

	LEFT OUTER JOIN SAPABAP1.MARA ITM ON(ITM.MANDT=VBRP.MANDT 
		AND ITM.MATNR=VBRP.MATNR ) --Unit Selling Price

	LEFT OUTER JOIN SAPABAP1.KONV AS TP ON(TP.MANDT=VBRK.MANDT 
		AND TP.KNUMV=VBRK.KNUMV 
		AND TP.KPOSN=SALES_ORDER_LINE.POSNR 
		AND TP.KSCHL='ZTRP') --,VBRK.KNUMV
 --Item Category Desc

	LEFT OUTER JOIN SAPABAP1.TVAPT AS ITMC ON(VBRP.MANDT=ITMC.MANDT 
		AND ITMC.PSTYV=VBRP.PSTYV 
		AND ITMC.SPRAS='E') --,VBRP.PSTYV VBRP_PSTYV
 
	WHERE 1=1 
	AND VBRK.MANDT=300 
	AND VBRK.BUKRS IN (6100,6300) --- ROUGH
--AND VBRK.VBELN='2400001308'--CANCEL INVOICE
--AND VBRK.VBELN='2300020625'
--ORDER BY VBRK.VBELN
 ---UNION ALL CLAIMABLE / UNCLAIMABLE
 
	UNION ALL SELECT
	 VBRK.MANDT CLIENT ,
	VBRK.BUKRS COMPANY ,
	VBRK.FKDAT BILLING_DATE ,
	VBRK.VBELN BILLING_DOC ,
	VBRK.FKART Billing_Type ,
	TVFKT.VTEXT BILL_TYPE_DESC ,
	VBRP.AUBEL Sales_ORDER_NUMBER ,
	SALES_ORD.BEZEI SALES_ORDER_TYPE ,
	VBRK.FKTYP BILLING_CATEGORY ,
	BCD.DDTEXT Billing_category_DESC ,
	VBRK.VBTYP SD_document_category ,
	SDCD.DDTEXT SD_document_category_DESC ,
	VBRK.VKORG Sales_Organization ,
	SOD.VTEXT Sales_Organization_DESC ,
	BOOKER.PERNR BOOKER_ID,
	BOOKER_NAME.VNAMC BOOKER_NAME ,
	SUPPLIER.PERNR SUPPLIER_ID,
	SUPPLIER_NAME.VNAMC SUPPLIER_NAME ,
	SALES_ORDER_LINE.POSNR SALES_ORDER_LINE ,
	VBRP.MVGR1 BUSLINE_ID,
	BUSLINE.BEZEI BUSLINE_DESC ,
	VBRK.KUNAG CUSTOMER_NUMBER,
	CUST.NAME1 CUSTOMER_NAME,
	VBRK.KUNRG Payer ,
	VBRK.KDGRP CHANNEL_ID,
	CHNL.KTEXT CHANNEL ,
	VBRP.MATNR INVENTORY_ITEM_ID ,
	VBRP.MATNR ITEM_CODE ,
	ITM.NORMT MAPPING_ITEM_CODE ,
	VBRP.ARKTX ITEM_DESC ,
	VBRP.PSTYV item_category ,
	ITMC.VTEXT item_category_desc ,
	0 SOLD_QTY ,
	0 BONUS_QTY ,
	TP.KBETR UNIT_SELLING_PRICE ,
	CLMDISC.KBETR CLAIMABLE_DISC ,
	'DISC' DATA_FLAG ,
	CLMDISC.KSCHL disc_flag --,CLMDISC.KBETR
--,Z_GET_CLAIM_UN_DISCOUNT('C',SALES_ORDER_LINE.POSNR, VBRK.KNUMV)
 ,
	'' REF_COLS ,
	VBAK.AUART VBAK_AUART ,
	VBRK.KNUMV ,
	VBRP.FKIMG VBRP_FKIMG ,
	VBRP.PSTYV VBRP_PSTYV 
	FROM SAPABAP1.VBRK AS VBRK 
	INNER JOIN SAPABAP1.VBRP AS VBRP ON (VBRP.MANDT=VBRK.MANDT 
		AND VBRP.VBELN=VBRK.VBELN) 
	LEFT OUTER JOIN SAPABAP1.VBAK AS VBAK ON(VBAK.MANDT=VBRK.MANDT 
		AND VBAK.VBELN=VBRP.AUBEL) --BILL TYPE DESC	

	LEFT OUTER JOIN SAPABAP1.TVFKT AS TVFKT ON (TVFKT.MANDT=VBRK.MANDT 
		AND TVFKT.FKART=VBRK.FKART 
		AND TVFKT.SPRAS='E')--BILL TYPE DESC
 
	LEFT OUTER JOIN SAPABAP1.DD07T AS BCD ON (BCD.DOMVALUE_L=VBRK.FKTYP 
		AND BCD.DDLANGUAGE='E' 
		AND BCD.DOMNAME='FKTYP' ) --BILLING_CATEGORY_DESC
 
	LEFT OUTER JOIN SAPABAP1.DD07T AS SDCD ON (SDCD.DOMVALUE_L=VBRK.VBTYP 
		AND SDCD.DDLANGUAGE='E' 
		AND SDCD.DOMNAME='VBTYP' ) --SD_document_category_DESC
 
	LEFT OUTER JOIN SAPABAP1.TVKOT AS SOD ON (SOD.MANDT=VBRK.MANDT 
		AND SOD.VKORG=VBRK.VKORG 
		AND SOD.SPRAS='E') --Sales_Organization_DESC
 --BOOKER_ID

	LEFT OUTER JOIN SAPABAP1.VBPA AS BOOKER ON(BOOKER.MANDT=VBRK.MANDT 
		AND BOOKER.VBELN=VBRP.AUBEL 
		AND BOOKER.PARVW='BK')--BOOKER ID
 --BOOKER NAME

	LEFT OUTER JOIN SAPABAP1.PA0002 AS BOOKER_NAME ON(BOOKER_NAME.MANDT=VBRK.MANDT 
		AND BOOKER_NAME.PERNR=BOOKER.PERNR)--BOOKER NAME
 --SUPPLIER ID

	LEFT OUTER JOIN SAPABAP1.VBPA AS SUPPLIER ON(SUPPLIER.MANDT=VBRK.MANDT 
		AND SUPPLIER.VBELN=VBRP.AUBEL 
		AND SUPPLIER.PARVW='ZS')--SUPPLIER ID
 --SUPPLIER NAME

	LEFT OUTER JOIN SAPABAP1.PA0002 AS SUPPLIER_NAME ON(SUPPLIER_NAME.MANDT=VBRK.MANDT 
		AND SUPPLIER_NAME.PERNR=SUPPLIER.PERNR)--SUPPLIER NAME
 --SALES ORDER LINE

	LEFT OUTER JOIN SAPABAP1.VBAP SALES_ORDER_LINE ON(SALES_ORDER_LINE.MANDT=VBRP.MANDT 
		AND SALES_ORDER_LINE.VBELN=VBRP.AUBEL 
		AND SALES_ORDER_LINE.POSNR=VBRP.POSNR) --BUSLIEN DESC

	LEFT OUTER JOIN SAPABAP1.TVM1T BUSLINE ON(BUSLINE.MANDT=VBRP.MANDT 
		AND BUSLINE.MVGR1=VBRP.MVGR1 
		AND BUSLINE.SPRAS='E') --CUSTOMER

	LEFT OUTER JOIN SAPABAP1.KNA1 CUST ON(CUST.MANDT=VBRK.MANDT 
		AND CUST.KUNNR=VBRK.KUNAG ) --CHANNEL

	LEFT OUTER JOIN SAPABAP1.T151T CHNL ON(CHNL.MANDT=VBRK.MANDT 
		AND CHNL.KDGRP=VBRK.KDGRP 
		AND CHNL.SPRAS='E') --SALES ORDER TYPE

	LEFT OUTER JOIN SAPABAP1.TVAKT SALES_ORD ON(SALES_ORD.MANDT=VBAK.MANDT 
		AND SALES_ORD.AUART=VBAK.AUART 
		AND SALES_ORD.SPRAS='E') --ITEM

	LEFT OUTER JOIN SAPABAP1.MARA ITM ON(ITM.MANDT=VBRP.MANDT 
		AND ITM.MATNR=VBRP.MATNR ) --Unit Selling Price

	LEFT OUTER JOIN SAPABAP1.KONV AS TP ON(TP.MANDT=VBRK.MANDT 
		AND TP.KNUMV=VBRK.KNUMV 
		AND TP.KPOSN=SALES_ORDER_LINE.POSNR 
		AND TP.KSCHL='ZTRP') --,VBRK.KNUMV
 --CLAIMABLE DISCOUNT

	LEFT OUTER JOIN SAPABAP1.KONV AS CLMDISC ON(CLMDISC.MANDT=VBRK.MANDT 
		AND CLMDISC.KNUMV=VBRK.KNUMV 
		AND CLMDISC.KPOSN=SALES_ORDER_LINE.POSNR 
		AND CLMDISC.KSCHL IN ('ZCDP',
	'ZCDV',
	'ZUDP' ,
	 'ZUDV')) --Item Category Desc

	LEFT OUTER JOIN SAPABAP1.TVAPT AS ITMC ON(VBRP.MANDT=ITMC.MANDT 
		AND ITMC.PSTYV=VBRP.PSTYV 
		AND ITMC.SPRAS='E') --,VBRP.PSTYV VBRP_PSTYV
 
	WHERE 1=1 
	AND VBRK.MANDT=300 
	AND VBRK.BUKRS IN (6100,6300) 
	AND UPPER(ITMC.VTEXT) NOT LIKE '%BONUS%' --- ROUGH
--AND VBRK.VBELN='2400001308'--CANCEL INVOICE
--AND VBRK.VBELN='2300020625'
--ORDER BY VBRK.VBELN
 ) 
WHERE 1=1 
--AND BILLING_DATE BETWEEN '20210801' 
--AND '20210831' 
order by BILLING_DOC,
	SALES_ORDER_LINE;

CREATE VIEW ETL.PHNX_SALES_DETAILS AS
SELECT
	 * 
from ( SELECT
	 VBRK.MANDT CLIENT ,
	 VBRK.BUKRS COMPANY ,
	 VBRK.FKDAT BILLING_DATE ,
	 VBRK.VBELN BILLING_DOC ,
	 VBRK.FKART Billing_Type ,
	 TVFKT.VTEXT BILL_TYPE_DESC ,
	 VBRP.AUBEL Sales_ORDER_NUMBER ,
	 SALES_ORD.BEZEI SALES_ORDER_TYPE ,
	 VBRK.FKTYP BILLING_CATEGORY ,
	 BCD.DDTEXT Billing_category_DESC ,
	 VBRK.VBTYP SD_document_category ,
	 SDCD.DDTEXT SD_document_category_DESC ,
	 VBRK.VKORG Sales_Organization ,
	 SOD.VTEXT Sales_Organization_DESC ,
	 BOOKER.PERNR BOOKER_ID,
	 BOOKER_NAME.VNAMC BOOKER_NAME ,
	 SUPPLIER.PERNR SUPPLIER_ID,
	 SUPPLIER_NAME.VNAMC SUPPLIER_NAME ,
	 SALES_ORDER_LINE.POSNR SALES_ORDER_LINE ,
	 VBRP.MVGR1 BUSLINE_ID,
	 BUSLINE.BEZEI BUSLINE_DESC ,
	 VBRK.KUNAG CUSTOMER_NUMBER,
	 CUST.NAME1 CUSTOMER_NAME,
	 VBRK.KUNRG Payer ,
	 VBRK.KDGRP CHANNEL_ID,
	 CHNL.KTEXT CHANNEL ,
	 VBRP.MATNR INVENTORY_ITEM_ID ,
	 VBRP.MATNR ITEM_CODE ,
	 ITM.NORMT MAPPING_ITEM_CODE ,
	 VBRP.ARKTX ITEM_DESC ,
	 VBRP.PSTYV item_category ,
	 ITMC.VTEXT item_category_desc ,
	 VBRP.FKIMG SOLD_QTY ,
	 VBRP.FKIMG BONUS_QTY ,
	 TP.KBETR UNIT_SELLING_PRICE --,CLMDISC.KBETR
--,Z_GET_CLAIM_UN_DISCOUNT('C',SALES_ORDER_LINE.POSNR, VBRK.KNUMV)
 ,
	 0 CLAIMABLE_DISC ,
	 'SALES' DATA_FLAG ,
	 null disc_flag ,
	 vbrk.FKSTO,
	 '' REF_COLS ,
	 VBAK.AUART VBAK_AUART ,
	 VBRK.KNUMV ,
	 VBRP.FKIMG VBRP_FKIMG ,
	 VBRP.PSTYV VBRP_PSTYV ,
	 VBRP.POSNR VBRP_POSNR ,
	CASE WHEN VBRK.ZTERM='Z000' THEN 'CASH' ELSE 'CREDIT' END  PAYMENT_TYPE--,ZTERM_TXT.TEXT1 ZTERM_TEXT
 
	FROM SAPABAP1.VBRK AS VBRK 
	INNER JOIN SAPABAP1.VBRP AS VBRP ON (VBRP.MANDT=VBRK.MANDT 
		AND VBRP.VBELN=VBRK.VBELN) 
	LEFT OUTER JOIN SAPABAP1.VBAK AS VBAK ON(VBAK.MANDT=VBRK.MANDT 
		AND VBAK.VBELN=VBRP.AUBEL) --BILL TYPE DESC	
 
	LEFT OUTER JOIN SAPABAP1.TVFKT AS TVFKT ON (TVFKT.MANDT=VBRK.MANDT 
		AND TVFKT.FKART=VBRK.FKART 
		AND TVFKT.SPRAS='E')--BILL TYPE DESC
 
	LEFT OUTER JOIN SAPABAP1.DD07T AS BCD ON (BCD.DOMVALUE_L=VBRK.FKTYP 
		AND BCD.DDLANGUAGE='E' 
		AND BCD.DOMNAME='FKTYP' ) --BILLING_CATEGORY_DESC
 
	LEFT OUTER JOIN SAPABAP1.DD07T AS SDCD ON (SDCD.DOMVALUE_L=VBRK.VBTYP 
		AND SDCD.DDLANGUAGE='E' 
		AND SDCD.DOMNAME='VBTYP' ) --SD_document_category_DESC
 
	LEFT OUTER JOIN SAPABAP1.TVKOT AS SOD ON (SOD.MANDT=VBRK.MANDT 
		AND SOD.VKORG=VBRK.VKORG 
		AND SOD.SPRAS='E') --Sales_Organization_DESC
 --BOOKER_ID
 
	LEFT OUTER JOIN SAPABAP1.VBPA AS BOOKER ON(BOOKER.MANDT=VBRK.MANDT 
		AND BOOKER.VBELN=VBRP.AUBEL 
		AND BOOKER.PARVW='BK')--BOOKER ID
 --BOOKER NAME
 
	LEFT OUTER JOIN SAPABAP1.PA0002 AS BOOKER_NAME ON(BOOKER_NAME.MANDT=VBRK.MANDT 
		AND BOOKER_NAME.PERNR=BOOKER.PERNR)--BOOKER NAME
 --SUPPLIER ID
 
	LEFT OUTER JOIN SAPABAP1.VBPA AS SUPPLIER ON(SUPPLIER.MANDT=VBRK.MANDT 
		AND SUPPLIER.VBELN=VBRP.AUBEL 
		AND SUPPLIER.PARVW='ZS')--SUPPLIER ID
 --SUPPLIER NAME
 
	LEFT OUTER JOIN SAPABAP1.PA0002 AS SUPPLIER_NAME ON(SUPPLIER_NAME.MANDT=VBRK.MANDT 
		AND SUPPLIER_NAME.PERNR=SUPPLIER.PERNR)--SUPPLIER NAME
 --SALES ORDER LINE
 
	LEFT OUTER JOIN SAPABAP1.VBAP SALES_ORDER_LINE ON(SALES_ORDER_LINE.MANDT=VBRP.MANDT 
		AND SALES_ORDER_LINE.VBELN=VBRP.AUBEL 
		AND SALES_ORDER_LINE.POSNR=VBRP.POSNR) --sales order line
 
	LEFT OUTER JOIN SAPABAP1.TVM1T BUSLINE ON(BUSLINE.MANDT=VBRP.MANDT 
		AND BUSLINE.MVGR1=VBRP.MVGR1 
		AND BUSLINE.SPRAS='E') --customer
 
	LEFT OUTER JOIN SAPABAP1.KNA1 CUST ON(CUST.MANDT=VBRK.MANDT 
		AND CUST.KUNNR=VBRK.KUNAG ) --CHANNEL
 
	LEFT OUTER JOIN SAPABAP1.T151T CHNL ON(CHNL.MANDT=VBRK.MANDT 
		AND CHNL.KDGRP=VBRK.KDGRP 
		AND CHNL.SPRAS='E') --channel
 
	LEFT OUTER JOIN SAPABAP1.TVAKT SALES_ORD ON(SALES_ORD.MANDT=VBAK.MANDT 
		AND SALES_ORD.AUART=VBAK.AUART 
		AND SALES_ORD.SPRAS='E') --ITEM
 
	LEFT OUTER JOIN SAPABAP1.MARA ITM ON(ITM.MANDT=VBRP.MANDT 
		AND ITM.MATNR=VBRP.MATNR ) --Unit Selling Price
 
	LEFT OUTER JOIN SAPABAP1.KONV AS TP ON(TP.MANDT=VBRK.MANDT 
		AND TP.KNUMV=VBRK.KNUMV 
		AND TP.KPOSN=SALES_ORDER_LINE.POSNR 
		AND TP.KSCHL='ZTRP') --,VBRK.KNUMV
 --Item Category Desc
 
	LEFT OUTER JOIN SAPABAP1.TVAPT AS ITMC ON(VBRP.MANDT=ITMC.MANDT 
		AND ITMC.PSTYV=VBRP.PSTYV 
		AND ITMC.SPRAS='E') --,VBRP.PSTYV VBRP_PSTYV
 --ZTERM TEXT
/* 
	LEFT OUTER JOIN SAPABAP1.T052U AS ZTERM_TXT ON (ZTERM_TXT.MANDT=VBRP.MANDT 
		AND ZTERM_TXT.SPRAS='E' 
		AND ZTERM_TXT.ZTERM=VBRK.ZTERM) */ 
	WHERE 1=1 
	AND VBRK.MANDT=300 
	--AND VBRK.BUKRS=6300--- ROUGH
--AND VBRK.VBELN='2400001308'--CANCEL INVOICE
--AND VBRK.VBELN='2300020625'
--ORDER BY VBRK.VBELN
 ---UNION ALL CLAIMABLE / UNCLAIMABLE
 
 

	UNION ALL SELECT
	 VBRK.MANDT CLIENT ,
	 VBRK.BUKRS COMPANY ,
	 VBRK.FKDAT BILLING_DATE ,
	 VBRK.VBELN BILLING_DOC ,
	 VBRK.FKART Billing_Type ,
	 TVFKT.VTEXT BILL_TYPE_DESC ,
	 VBRP.AUBEL Sales_ORDER_NUMBER ,
	 SALES_ORD.BEZEI SALES_ORDER_TYPE ,
	 VBRK.FKTYP BILLING_CATEGORY ,
	 BCD.DDTEXT Billing_category_DESC ,
	 VBRK.VBTYP SD_document_category ,
	 SDCD.DDTEXT SD_document_category_DESC ,
	 VBRK.VKORG Sales_Organization ,
	 SOD.VTEXT Sales_Organization_DESC ,
	 BOOKER.PERNR BOOKER_ID,
	 BOOKER_NAME.VNAMC BOOKER_NAME ,
	 SUPPLIER.PERNR SUPPLIER_ID,
	 SUPPLIER_NAME.VNAMC SUPPLIER_NAME ,
	 SALES_ORDER_LINE.POSNR SALES_ORDER_LINE ,
	 VBRP.MVGR1 BUSLINE_ID,
	 BUSLINE.BEZEI BUSLINE_DESC ,
	 VBRK.KUNAG CUSTOMER_NUMBER,
	 CUST.NAME1 CUSTOMER_NAME,
	 VBRK.KUNRG Payer ,
	 VBRK.KDGRP CHANNEL_ID,
	 CHNL.KTEXT CHANNEL ,
	 VBRP.MATNR INVENTORY_ITEM_ID ,
	 VBRP.MATNR ITEM_CODE ,
	 ITM.NORMT MAPPING_ITEM_CODE ,
	 VBRP.ARKTX ITEM_DESC ,
	 VBRP.PSTYV item_category ,
	 ITMC.VTEXT item_category_desc ,
	 0 SOLD_QTY ,
	 0 BONUS_QTY ,
	 TP.KBETR UNIT_SELLING_PRICE ,
	 CLMDISC.KBETR CLAIMABLE_DISC ,
	 'DISC' DATA_FLAG ,
	 CLMDISC.KSCHL disc_flag,
	 --,CLMDISC.KBETR
--,Z_GET_CLAIM_UN_DISCOUNT('C',SALES_ORDER_LINE.POSNR, VBRK.KNUMV)
 vbrk.FKSTO,
	 '' REF_COLS ,
	 VBAK.AUART VBAK_AUART ,
	 VBRK.KNUMV ,
	 VBRP.FKIMG VBRP_FKIMG ,
	 VBRP.PSTYV VBRP_PSTYV ,
	 VBRP.POSNR VBRP_POSNR ,
	CASE WHEN VBRK.ZTERM='Z000' THEN 'CASH' ELSE 'CREDIT' END  PAYMENT_TYPE--,ZTERM_TXT.TEXT1 ZTERM_TEXT

 
	FROM SAPABAP1.VBRK AS VBRK 
	INNER JOIN SAPABAP1.VBRP AS VBRP ON (VBRP.MANDT=VBRK.MANDT 
		AND VBRP.VBELN=VBRK.VBELN) 
	LEFT OUTER JOIN SAPABAP1.VBAK AS VBAK ON(VBAK.MANDT=VBRK.MANDT 
		AND VBAK.VBELN=VBRP.AUBEL) --BILL TYPE DESC	
 
	LEFT OUTER JOIN SAPABAP1.TVFKT AS TVFKT ON (TVFKT.MANDT=VBRK.MANDT 
		AND TVFKT.FKART=VBRK.FKART 
		AND TVFKT.SPRAS='E')--BILL TYPE DESC
 
	LEFT OUTER JOIN SAPABAP1.DD07T AS BCD ON (BCD.DOMVALUE_L=VBRK.FKTYP 
		AND BCD.DDLANGUAGE='E' 
		AND BCD.DOMNAME='FKTYP' ) --BILLING_CATEGORY_DESC
 
	LEFT OUTER JOIN SAPABAP1.DD07T AS SDCD ON (SDCD.DOMVALUE_L=VBRK.VBTYP 
		AND SDCD.DDLANGUAGE='E' 
		AND SDCD.DOMNAME='VBTYP' ) --SD_document_category_DESC
 
	LEFT OUTER JOIN SAPABAP1.TVKOT AS SOD ON (SOD.MANDT=VBRK.MANDT 
		AND SOD.VKORG=VBRK.VKORG 
		AND SOD.SPRAS='E') --Sales_Organization_DESC
 --BOOKER_ID
 
	LEFT OUTER JOIN SAPABAP1.VBPA AS BOOKER ON(BOOKER.MANDT=VBRK.MANDT 
		AND BOOKER.VBELN=VBRP.AUBEL 
		AND BOOKER.PARVW='BK')--BOOKER ID
 --BOOKER NAME
 
	LEFT OUTER JOIN SAPABAP1.PA0002 AS BOOKER_NAME ON(BOOKER_NAME.MANDT=VBRK.MANDT 
		AND BOOKER_NAME.PERNR=BOOKER.PERNR)--BOOKER NAME
 --SUPPLIER ID
 
	LEFT OUTER JOIN SAPABAP1.VBPA AS SUPPLIER ON(SUPPLIER.MANDT=VBRK.MANDT 
		AND SUPPLIER.VBELN=VBRP.AUBEL 
		AND SUPPLIER.PARVW='ZS')--SUPPLIER ID
 --SUPPLIER NAME
 
	LEFT OUTER JOIN SAPABAP1.PA0002 AS SUPPLIER_NAME ON(SUPPLIER_NAME.MANDT=VBRK.MANDT 
		AND SUPPLIER_NAME.PERNR=SUPPLIER.PERNR)--SUPPLIER NAME
 --SALES ORDER LINE
 
	LEFT OUTER JOIN SAPABAP1.VBAP SALES_ORDER_LINE ON(SALES_ORDER_LINE.MANDT=VBRP.MANDT 
		AND SALES_ORDER_LINE.VBELN=VBRP.AUBEL 
		AND SALES_ORDER_LINE.POSNR=VBRP.POSNR) --BUSLIEN DESC
 
	LEFT OUTER JOIN SAPABAP1.TVM1T BUSLINE ON(BUSLINE.MANDT=VBRP.MANDT 
		AND BUSLINE.MVGR1=VBRP.MVGR1 
		AND BUSLINE.SPRAS='E') --Busline
 
	LEFT OUTER JOIN SAPABAP1.KNA1 CUST ON(CUST.MANDT=VBRK.MANDT 
		AND CUST.KUNNR=VBRK.KUNAG ) --customer
 
	LEFT OUTER JOIN SAPABAP1.T151T CHNL ON(CHNL.MANDT=VBRK.MANDT 
		AND CHNL.KDGRP=VBRK.KDGRP 
		AND CHNL.SPRAS='E') --SALES ORDER TYPE
 
	LEFT OUTER JOIN SAPABAP1.TVAKT SALES_ORD ON(SALES_ORD.MANDT=VBAK.MANDT 
		AND SALES_ORD.AUART=VBAK.AUART 
		AND SALES_ORD.SPRAS='E') --ITEM
 
	LEFT OUTER JOIN SAPABAP1.MARA ITM ON(ITM.MANDT=VBRP.MANDT 
		AND ITM.MATNR=VBRP.MATNR ) --Unit Selling Price
 
	LEFT OUTER JOIN SAPABAP1.KONV AS TP ON(TP.MANDT=VBRK.MANDT 
		AND TP.KNUMV=VBRK.KNUMV 
		AND TP.KPOSN=SALES_ORDER_LINE.POSNR 
		AND TP.KSCHL='ZTRP') --,VBRK.KNUMV

 --CLAIMABLE DISCOUNT
 
	LEFT OUTER JOIN SAPABAP1.KONV AS CLMDISC ON(CLMDISC.MANDT=VBRK.MANDT 
		AND CLMDISC.KNUMV=VBRK.KNUMV 
		AND CLMDISC.KPOSN=SALES_ORDER_LINE.POSNR 
		AND CLMDISC.KSCHL IN ('ZCDP',
	 'ZCDV',
	 'ZUDP' ,
	 'ZUDV')) --Item Category Desc
 
	LEFT OUTER JOIN SAPABAP1.TVAPT AS ITMC ON(VBRP.MANDT=ITMC.MANDT 
		AND ITMC.PSTYV=VBRP.PSTYV 
		AND ITMC.SPRAS='E') --,VBRP.PSTYV VBRP_PSTYV
 --ZTERM TEXT
	WHERE 1=1 
	AND VBRK.MANDT=300 
	--AND VBRK.BUKRS=6300 
	AND UPPER(ITMC.VTEXT) NOT LIKE '%BONUS%' --- ROUGH
--AND VBRK.VBELN='2400001308'--CANCEL INVOICE
--AND VBRK.VBELN='2300020625'
--ORDER BY VBRK.VBELN




 );

CREATE VIEW ETL.PHNX_SALES_VIEW AS
SELECT a.item_category,
    a.org_id,
    a.org_desc,
    a.trx_date,
    a.trx_number,
    a.booker_id,
    a.booker_name,
    a.supplier_id,
    a.supplier_name,
    a.business_line_id,
    a.business_line,
    a.channel,
    a.customer_id,
    a.customer_number,
    a.customer_name,
    a.sales_order_type,
    a.inventory_item_id,
    a.item_code,
    a.description,
    a.unit_selling_price,
        CASE
            WHEN upper(a.sales_order_type   ) LIKE  '%CANCEL%MEMO%'    AND a.sold_qty < 0  THEN a.sold_qty * '-1'  
            ELSE a.sold_qty
        END AS sold_qty,
    a.bonus_qty,
        CASE
            WHEN 1 = 1 AND upper(a.sales_order_type   ) LIKE  '%CANCEL%MEMO%'    AND a.claimable_discount >= 0  THEN a.claimable_discount * -1
            WHEN a.billing_type    = 'ZORC'    THEN a.claimable_discount * -1 
            WHEN a.company_code    = '6100'    AND a.billing_type    = 'ZUBC'    AND a.claimable_discount > 0  THEN a.claimable_discount * -1  
            ELSE a.claimable_discount
        END AS claimable_discount,
        CASE
            WHEN 1 = 1 AND upper(a.sales_order_type   ) like '%CANCEL%MEMO%'    AND a.unclaimable_discount >= 0  THEN a.unclaimable_discount * -1
            WHEN a.billing_type    = 'ZORC'    THEN a.unclaimable_discount * -1 
            WHEN a.company_code    = '6100'    AND a.billing_type    = 'ZUBC'    AND a.unclaimable_discount > 0  THEN a.unclaimable_discount * -1
            ELSE a.unclaimable_discount
        END AS unclaimable_discount,
    a.tax_recoverable,
        CASE
            WHEN a.item_category   IN  ('ZFCL'  , 'ZFOU'  , 'ZBRC'  , 'ZBRU') THEN a.tax_recoverable
            ELSE
            CASE
                WHEN a.billing_type    IN   ('ZUCC'   , 'ZURB') THEN a.sold_qty * a.unit_selling_price - a.total_discount * -1   + a.tax_recoverable
                ELSE a.sold_qty * a.unit_selling_price - abs(a.total_discount) + a.tax_recoverable
            END
        END AS net_amount,
        CASE
            WHEN a.item_category    IN ('ZFCL'  , 'ZFOU'  , 'ZBRC'   , 'ZBRU') THEN 0 
            ELSE
            CASE
                WHEN a.billing_type    = 'ZORC'    THEN a.sold_qty * a.unit_selling_price * -1 
                ELSE a.sold_qty * a.unit_selling_price
            END
        END AS gross_amount,
    a.total_discount,
    a.customer_trx_id,
    a.reason_code,
    a.bill_type_desc,
    a.company_code,
    a.billing_type,
    a.sales_order_no,
    a.add1,
    a.add2,
    a.add3,
    a.bstnk,
    a.item_no,
    a.return_reason_code
   FROM ( SELECT psdd.item_category,
            psdd.org_id,
            psdd.org_desc,
            psdd.billing_date AS trx_date,
            psdd.document_no AS trx_number,
            psdd.booker_id AS booker_id,
            psdd.booker_name,
            psdd.supplier_id AS supplier_id,
            psdd.supplier_name,
            trim(psdd.business_line_id   ) AS business_line_id,
            psdd.busline_desc AS business_line,
            COALESCE(psdd.channel_desc, 'Blank') AS channel,
            psdd.customer_id,
            psdd.customer_number,
            psdd.customer_name,
            psdd.sales_order_type,
            psdd.inventory_item_id,
            psdd.item_code,
            psdd.description,
            psdd.unit_selling_price,
            psdd.quantity AS sold_qty,
            psdd.bonus_qty,
            psdd.claimable_discount,
            psdd.unclaimable_discount,
                CASE
                    WHEN (psdd.sales_order_type    LIKE '%CANCEL%'    OR psdd.sales_order_type    LIKE  '%RETUR%'   ) 
                    AND upper(psdd.sales_order_type   ) NOT LIKE  '%CANCEL%MEMO%'    AND psdd.tax_recoverable > 0  THEN psdd.tax_recoverable * -1
                    WHEN upper(psdd.sales_order_type   ) LIKE  '%CANCEL%MEMO%'    AND psdd.tax_recoverable < 0  THEN psdd.tax_recoverable * -1  
                    ELSE psdd.tax_recoverable
                END AS tax_recoverable,
            psdd.gross_amount AS net_amount,
            psdd.unclaimable_discount + psdd.claimable_discount AS total_discount,
            psdd.document_no AS customer_trx_id,
            psdd.reason_code,
            psdd.bill_type_desc,
            psdd.company_code,
            psdd.billing_type,
            psdd.sales_order_no,
            psdd.add1,
            psdd.add2,
            psdd.add3,
            psdd.bstnk,
            psdd.item_no,
            psdd.return_reason_code
           FROM PHNX_SALES_DATA_VIEW psdd
          WHERE 1 = 1 AND          psdd.company_code    IN  ('6100'   , '6300')       
          AND psdd.billing_type IN  ('ZOPC'  , 'ZOCC'   , 'ZUBC'  , 'ZUCC'   , 'ZORE'
          , 'ZORC'   , 'ZURB'  , 'ZUB1'  , 'ZNES'  , 'ZNEC')
          ) a;

CREATE VIEW ETL.PROFITCENTERGROUP_SETLEAF AS
SELECT 
s.MANDT ,s.SETCLASS ,s.SUBCLASS ,s.SETNAME setleaf_setname,sh.SETNAME header_setname,s.VALFROM 
FROM SAPABAP1.SETLEAF s  
LEFT OUTER JOIN SAPABAP1.SETHEADER sh ON (sh.MANDT=s.MANDT AND sh.SETCLASS=s.SETCLASS AND sh.SUBCLASS=s.SUBCLASS )
WHERE 1=1 AND s.MANDT =300
AND s.SETCLASS ='0106';

CREATE VIEW ETL.SAP_EMPLOYEE_DETAILS AS
SELECT 
COMPANY_ID,COMPANY_DESC,EMP_ID,FIRST_NAME,LAST_NAME,EMPLOYEE_NAME,GENDER
,DATE_OF_BIRTH,JOINING_DATE
,DEPT_SECTION_ID,DEPT_SECTION_ID_DESC
FROM (
SELECT 
distinct
1
,B.BUKRS as COMPANY_ID,d.BUTXT Company_Desc
,A.PERNR as EMP_ID
,VORNA as FIRST_NAME
,NACHN as LAST_NAME
,CONCAT(VORNA,NACHN) as EMPLOYEE_NAME
,CASE WHEN GESCH=1 THEN 'Male' ELSE 'Female' END Gender
,TO_VARCHAR(TO_DATE(GBDAT, 'YYYYMMDD'),'YYYY-MM-DD') as DATE_OF_BIRTH
,TO_VARCHAR(TO_DATE(B.BEGDA, 'YYYYMMDD'),'YYYY-MM-DD') as JOINING_DATE
,B.ORGEH  as DEPT_SECTION_ID,C.ORGTX DEPT_SECTION_ID_DESC
FROM SAPABAP1.PA0002 A
LEFT OUTER JOIN SAPABAP1.PA0001  B ON (B.MANDT=A.MANDT AND B.PERNR=A.PERNR)
LEFT OUTER JOIN SAPABAP1.T527X  C ON (C.MANDT=A.MANDT AND C.SPRSL='E' AND C.ORGEH=B.ORGEH)
LEFT OUTER JOIN SAPABAP1.T001 D ON (D.MANDT=A.MANDT AND D.BUKRS=B.BUKRS)
WHERE 1=1 AND A.MANDT =300 
AND A.ENDDA ='99991231'
--AND A.PERNR ='11003001' -- SUHAIL
);

CREATE VIEW ETL.SAP_MARKITT_DATA AS
SELECT "MANDT" , "MARKITT_DOCUMENT_NUMBER" , "BILLDATE" , "SAP_ORDER" , "SAP_ORDER_NUMBER" , "BARCODE" , "ITEM_CODE" , "ITEM_DESC" , "ORDER_QTY" , "NET_VALUE" , "ITEM_NO" , "ORDER_NUMBER" , "INVOICE_NUMBER" FROM (
SELECT
	zv.MANDT ,
	zv.BILLNO markitt_document_number,
	CAST(zv.BILLDATE AS DATE)BILLDATE ,
	SAP_ORDER ,
	vbap.VBELN sap_order_number ,
	mi.NORMT barcode,
	vbap.MATNR item_code,
	vbap.ARKTX item_desc ,
	vbap.KWMENG order_qty,
	vbap.NETWR net_value,
	vbap.POSNR item_no,
	vbap.AUFNR order_number,
	vbrp.VBELN invoice_number
--		CASE
--		WHEN ifnull(vbap.KLMENG, 0)= 0 THEN 'Insufficient Stock'
--		ELSE zv.ERROR_LOG4
--	END LOG_REASON,
--	'SAP_ERROR_LOG' DATA_FLAG
FROM
	SAPABAP1.ZMARKIT_VBAK zv
INNER JOIN SAPABAP1.VBAP vbap ON
	(vbap.VBELN = zv.SAP_ORDER)
LEFT OUTER JOIN SAPABAP1.vbrp vbrp ON
	(vbrp.AUBEL = vbap.VBELN
		AND vbrp.POSNR = vbap.POSNR)
LEFT OUTER JOIN MARKITT_ITEMS mi ON
	(mi.MATNR = vbap.MATNR)
WHERE
	1 = 1
	AND zv.MANDT = 300
		) WHERE 1=1;

CREATE VIEW ETL.USER_AUTH_TCODE_OBJECTS AS
SELECT 
PROFILE,PROFILE_DESC,"ROLE","USER",FIRST_NAME,LAST_NAME,FULL_NAME,START_DATE,END_DATE,COMPANY,COMPANY_DESC,"AUTHORIZATION",T_CODE,"OBJECT",FIELD
, value_,VALUE,VERSION,CHANGED_BY,MODIFICATION_DATE,MODIFICATION_TIME
FROM (
SELECT 
distinct
prf.AGR_NAME ROLE,usr.UNAME USER,usd.name_first first_name,usd.name_last last_name,usd.NAME_TEXTC full_name
,usd.mc_name1 company,usd.company company_desc
--,TO_VARCHAR(TO_DATE(usr.from_dat, 'YYYYMMDD'), 'YYYY-MM-DD')start_date
--,TO_VARCHAR(TO_DATE(usr.to_dat, 'YYYYMMDD'), 'YYYY-MM-DD')end_date
,usr.from_dat start_date, usr.to_dat end_date
,PROFN Profile,prf.PTEXT profile_desc
,a.objct OBJECT,a.aktps version,a.auth AUTHORIZATION,b.von value_
,uobj.NAME T_CODE,uobj.FIELD,uobj.MODIFIER CHANGED_BY
--,TO_VARCHAR(TO_DATE(uobj.MODDATE, 'YYYYMMDD'), 'YYYY-MM-DD')  MODIFICATION_DATE
,uobj.MODDATE MODIFICATION_DATE
,uobj.LOW value
,SUBSTRING(uobj.MODTIME,1,2)||':'||SUBSTRING(uobj.MODTIME,3,2) ||':'||SUBSTRING(uobj.MODTIME,5,2) modification_time   
FROM SAPABAP1.UST10S A
INNER JOIN SAPABAP1.UST12 b ON (a.OBJCT=b.OBJCT AND a.AUTH=b.AUTH AND a.MANDT=b.MANDT)
INNER JOIN SAPABAP1.AGR_PROF prf ON (prf.PROFILE = a.PROFN AND prf.MANDT=a.MANDT )
INNER JOIN SAPABAP1.AGR_USERS usr ON (usr.AGR_NAME=prf.AGR_NAME AND usr.MANDT=prf.MANDT)
INNER JOIN SAPABAP1.USOBT uobj ON (1=1  AND uobj.object=a.objct AND uobj.field=b.field)
LEFT OUTER JOIN SAPABAP1.USER_ADDR usd ON (usd.mandt=a.mandt AND usd.bname=usr.uname )
WHERE 1=1 
AND a.MANDT =300
AND prf.LANGU ='E'
---rough checking
--AND uobj."OBJECT" ='F_BKPF_BUK'  AND uobj.name='F-26'
--AND PROFN='T-IP281783' 
--AND a.AUTH ='T-IP28178300' 
--AND a.OBJCT ='F_BKPF_BUK'
)a;

CREATE VIEW ETL.VBKD_VIEW AS
SELECT VBKD.BSTKD,VBKD.VBELN,VBKD.POSNR,SUBSTRING(VBAP.MATNR,9,30)MATNR ,VBAP.PSTYV ITEM_CATEGORY
FROM SAPABAP1.VBKD VBKD
LEFT OUTER JOIN SAPABAP1.VBAP VBAP ON (VBAP.VBELN=VBKD.VBELN AND VBAP.POSNR=VBKD.POSNR)
WHERE 1=1 AND VBKD.MANDT=300   
--AND BSTKD IN ('D0025ORD13046')
---AND VBKD.VBELN ='3900230652'
  AND VBKD.POSNR<>'000000';

CREATE VIEW ETL.VW_AFVC_AFVV AS
select * from
(
SELECT
	 AFVC.MANDT CLIENT ,
	AUFK.BUKRS Company_Code,
	COMP.BUTXT COMPANY_DESC8,
	AFVC.WERKS PLANT ,
	T001W.NAME2 PLANT_DESC ,
	AFKO.AUFNR ORDER_NUMBER ,
	AUFK.ERDAT Created_DATE,
	AUFK.AUART Order_Type,
	T003P.TXT ORDER_TYPE_DESC,
	 AUFK.ASTNR Order_status,
	AUFK.AUTYP Order_category,
	ORD_CAT.DDTEXT ORDER_CATEGORY_DESC --JEST
,
	 CASE WHEN JEST.INACT='' 
THEN 'COMPLETED' 
ELSE 'IN-PROCESS' 
END ORDER_COMPLETION_STATUS ,
	AFVC.VORNR Operation_Activity_Number ,
	AFVV.SSSBD Latest_start_DATE_Process,
	AFVV.SSSBZ Latest_start_TIME_Process,
	AFVV.SSEDD Latest_FINISH_DATE_EXEC,
	AFVV.SSEDZ Latest_FINISH_TIME_Process ,
	AFVV.LMNGA Confirmed_YIELD ,
	AFVC.LAR01 ACTIVITY1,
	AFVV.BEARZ PROCESSING_TIME,
	AFVC.APLFL Sequence_NUMBER,
	AFVV.VGW01 Standard_Value ,
	(AFVV.VGW01/1000)*AFPO.PGMNG MACHINE_PLAN_HOURS ,
	AFVV.ISM01 Machine_Actual_HOURS ,
	AFVV.VGW02 Standard_Value2 ,
	(AFVV.VGW02/1000)*AFPO.PGMNG LABOUR_PLAN_HOURS ,
	AFVV.ISM02 LABOUR_ACTUAL_HOURS ,
	AFVV.IEDD FINISH_DATE_EXEC ,
	AFVV.IEDZ FINISH_TIME_EXEC ,
	AFVC.GSBER Business_Area,
	AFVC.FUNC_AREA Functional_Area ,
	AFPO.PSMNG Order_QTY ,
	AFPO.WEMNG Quantity_goods_received ,
	AFVC.ARBID WORK_CENTER,
	CRTX.KTEXT WORK_CENTER_DESC,
	AFVC.RUECK ,
	AFKO.PLNBEZ ITEM_CODE,
	 AUFK.KTEXT ITEM_DESC,
	MARA.MATKL MATERIAL_GROUP,
	T023T.WGBEZ MATERIAL_GROUP_DESC ,
	AFVC.AUFPL AUFPL_ROUTING,
	AFVC.APLZL APLZL_GENERAL_COUNTER,
	AFVC.PLNFL SEQUENCE,
	AFVC.SELKZ INDICATOR ,
	AFVC.LTXA1 OPERATIONAL_SHORT_TEXT,
	AFVC.LAR01 ACTIVITY_TYPE1,
	AFVC.LAR02 Activity_Type2,
	AFVC.LAR03 Activity_Type3 ,
	AFVC.LAR04 Activity_Type4,
	AFVC.LAR05 Activity_Type5,
	AFVC.ZERMA STANDARD_CALC,
	AFVC.LOART Wage_Type,
	AFVC.LARNT Activity_Type,
	AFVC.OBJNR OBJECT_NUMBER ,
	AFVC.NPTXTKY InternaL_TXT_NUMBER,
	AFVC.TXJCD Tax_Jurisdiction ,
	AFVV.LMNGA YIELD_CONFIRMED --,AFVV.BEARZ 	Processing_time
,
	AFVV.VGW01 Standard_Value1,
	AFVV.ILE02 ,
	AFVV.BMSCH BASE_QTY,
	AFVV.MGVRG OPERATION_QTY ,
	AFPO.PGMNG Total_planned_order_quantity ,
	AFVV.MEINH,
	AFVV.ILE01 CONFIRMED_ACITIVITY ---
,
	AFVV.ISDD ACTUAL_START_EXEC_DATE,
	AFVV.ISDZ ACTUAL_START_EXEC_TIME ,
	AFVV.FSSAD LAST_START_DATE,
	AFVV.FSSAZ ,
	AFVV.FSELZ ,
	AFVV.ASVRG Operation_scrap ,
	AFVV.XMNGA Total_scrap_quantity ,
	AFVV.BEAZP Unit_processing_time ,
	AFVC.OBJNR ,
	AUFK.PDAT1 Planned_release_date,
	AUFK.PDAT2 PLANNED_COMPLETION_DATE,
	AUFK.PDAT3 PLANNED_CLOSING_DATE ,
	AUFK.IDAT1 RELEASE_DATE,
	AUFK.IDAT2 TECHNICAL_COMPLETION_DATE,
	AUFK.IDAT3 CLOSE_DATE ,
	AUFK.SDATE Start_Date,
	AUFK.PRCTR Profit_center,
	PRCT.LTEXT PROFIT_CENTER_DESC ,
	AUFK.SAKNR GL_ACCOUNT --AFPO
,
	AFPO.POSNR Order_Item_Number,
	AFPO.KDAUF Sales_order_number,
	AFPO.MATNR --,ITEM_DESC.MAKTX ITEM_DESC
,
	AFPO.MEINS UOM ,
	AFPO.PSAMG Scrap_quantity ---,AFPO.PGMNG 	Total_planned_order_quantity
 ,
	AFPO.PAMNG Fixed_quantity_scrap,
	AFPO.CHARG Batch_Number,
	AFPO.LGORT AFPO_LGORT ,
	AFPO.ELIKZ Delivery_Completed_Indicator --AFKO
,
	AFKO.GSTRP Basic_Start_Date,
	AFKO.GLTRP AFKO_Basic_finish_date --REF COLUMNS
,
	AUFK.OBJNR AUFK_OBJNR,
	AFKO.AUFPL AFKO_AUFPL ,
	AFVC.OBJNR AFVC_OBJNR,
	AFVC.AUFPL 
FROM SAPABAP1.AFVC 
LEFT OUTER JOIN SAPABAP1.AFKO ON (AFKO.MANDT=AFVC.MANDT 
	AND AFKO.AUFPL=AFVC.AUFPL) 
LEFT OUTER JOIN SAPABAP1.AUFK ON (AUFK.MANDT=AFKO.MANDT 
	AND AUFK.AUFNR=AFKO.AUFNR ) 
LEFT OUTER JOIN SAPABAP1.CRTX ON (CRTX.MANDT=AFVC.MANDT 
	AND CRTX.OBJID=AFVC.ARBID AND CRTX.SPRAS='E') 
LEFT OUTER JOIN SAPABAP1.AFVV ON (AFVV.MANDT=AFVC.MANDT 
	AND AFVV.AUFPL=AFVC.AUFPL 
	AND AFVV.APLZL=AFVC.APLZL) 
LEFT OUTER JOIN SAPABAP1.T001W ON (T001W.MANDT=AUFK.MANDT 
	AND T001W.WERKS=AUFK.WERKS) 
LEFT OUTER JOIN SAPABAP1.DD07T ORD_CAT ON (ORD_CAT.DOMVALUE_L=AUFK.AUTYP 
	AND ORD_CAT.DDLANGUAGE='E' 
	AND ORD_CAT.DOMNAME='AUFTYP') 
LEFT OUTER JOIN SAPABAP1.T003P ON (T003P.CLIENT=AUFK.MANDT 
	AND T003P.AUART=AUFK.AUART 
	AND T003P.SPRAS='E') 
LEFT OUTER JOIN SAPABAP1.CEPCT PRCT ON (PRCT.MANDT=AUFK.MANDT 
	AND PRCT.PRCTR=AUFK.PRCTR 
	AND PRCT.SPRAS='E') 
LEFT OUTER JOIN SAPABAP1.AFPO ON (AFPO.AUFNR=AUFK.AUFNR 
	AND AFPO.MANDT=AUFK.MANDT) 
LEFT OUTER JOIN SAPABAP1.MAKT ITEM_DESC ON (ITEM_DESC.MANDT=AFPO.MANDT 
	AND ITEM_DESC.MATNR=AFPO.MATNR 
	AND ITEM_DESC.SPRAS='E') 
LEFT OUTER JOIN SAPABAP1.T001 COMP ON (COMP.MANDT=AUFK.MANDT 
	AND COMP.BUKRS=AUFK.BUKRS) 
LEFT OUTER JOIN SAPABAP1.JEST ON (JEST.MANDT=AUFK.MANDT 
	AND JEST.OBJNR=AUFK.OBJNR 
	AND STAT='I0045'/*COMPLETED STATUS MARK*/) 
LEFT OUTER JOIN SAPABAP1.MARA ON (1=1 
	AND MARA.MANDT=AFKO.MANDT 
	AND MARA.MATNR=AFKO.PLNBEZ) 
LEFT OUTER JOIN SAPABAP1.T023T ON (1=1 
	AND T023T.MANDT=MARA.MANDT 
	AND T023T.MATKL=MARA.MATKL)--MATERIAL GROUP DESC
);

CREATE VIEW ETL.VW_AUFK_ORDERS AS
SELECT "CLIENT" , "ORDER_NUMBER" , "ORDER_TYPE" , "ORDER_TYPE_DESC" , "ORDER_CATEGORY" , "ORDER_CATEGORY_DESC" , "REFERENCE_ORDER_NUMBER" , "ENTERED_BY" , "CREATED_DATE" , "LAST_CHANGED_BY" , "CHANGE_DATE_FOR_ORDER_MASTER" , "DESCRIPTION" , "LONG_TEXT_EXISTS" , "COMPANY_CODE" , "COMPANY_DESC" , "PLANT" , "PLANT_DESC" , "BUSINESS_AREA" , "BUSINESS_AREA_DESC" , "CONTROLLING_AREA" , "CONTROL_AREA_DESC" , "COST_COLLECTOR_KEY" , "RESPONSIBLE_COST_CENTER" , "LOCATION" , "LOC_DESC" , "ORDER_STATUS" , "STATUS_REACHED_SO_FAR" , "ORDER_CREATED" , "ORDER_RELEASED" , "ORDER_COMPLETED" , "ORDER_CLOSED" , "PLANNED_RELEASE_DATE" , "PLANNED_COMPLETION_DATE" , "PLANNED_CLOSING_DATE" , "RELEASE_DATE" , "TECHNICAL_COMPLETION_DATE" , "CLOSE_DATE" , "START_DATE" , "PROFIT_CENTER" , "PROFIT_CENTER_DESC" , "GL_ACCOUNT" , "ORDER_ITEM_NUMBER" , "SALES_ORDER_NUMBER" , "MATNR" , "ITEM_DESC" , "UOM" , "SCRAP_QUANTITY" , "TOTAL_PLANNED_ORDER_QUANTITY" , "ORDER_QTY" , "QUANTITY_GOODS_RECEIVED" , "FIXED_QUANTITY_SCRAP" , "VALUE_OF_GOOD_RECEIVED" , "BATCH_NUMBER" , "AFPO_LGORT" , "BASIC_START_DATE" , "AFKO_BASIC_FINISH_DATE" , "AUFK_OBJNR" , "AFKO_AUFPL" from
(
SELECT 

AUFK.MANDT CLIENT,AUFK.AUFNR ORDER_NUMBER,AUFK.AUART ORDER_TYPE,ORD_TYPE.TXT ORDER_TYPE_DESC,AUTYP 	Order_category,ORD_CAT.DDTEXT ORDER_CATEGORY_DESC
,REFNR 	Reference_order_numbeR,ERNAM 	Entered_by,ERDAT 	Created_DATE,AENAM 	Last_changed_by,AEDAT 	Change_date_for_Order_Master
,AUFK.KTEXT 	Description,AUFK.LTEXT 	Long_Text_Exists,AUFK.BUKRS 	Company_Code,COMP.BUTXT COMPANY_DESC ,AUFK.WERKS 	Plant,PLANT.NAME1 PLANT_DESC
, AUFK.GSBER 	Business_Area, IFNULL( BAREA.GTEXT,'N/A') BUSINESS_AREA_DESC,AUFK.KOKRS 	Controlling_Area,CONTROL_AREA.BEZEI CONTROL_AREA_DESC
,CCKEY 	Cost_collector_key,KOSTV 	Responsible_cost_center
,AUFK.STORT 	Location
,LOC.KTEXT LOC_DESC,AUFK.ASTNR 	Order_status
,AUFK.ESTNR 	Status_reached_so_far,AUFK.PHAS0 Order_created,AUFK.PHAS1 ORDER_RELEASED,AUFK.PHAS2 ORDER_COMPLETED,AUFK.PHAS3 ORDER_CLOSED

,AUFK.PDAT1 Planned_release_date,AUFK.PDAT2 PLANNED_COMPLETION_DATE,AUFK.PDAT3 PLANNED_CLOSING_DATE
,AUFK.IDAT1 RELEASE_DATE,AUFK.IDAT2 TECHNICAL_COMPLETION_DATE,AUFK.IDAT3 CLOSE_DATE
,AUFK.SDATE 	Start_Date,AUFK.PRCTR 	Profit_center,PRCT.LTEXT PROFIT_CENTER_DESC
,AUFK.SAKNR GL_ACCOUNT

--AFPO
,AFPO.POSNR 	Order_Item_Number,AFPO.KDAUF 	Sales_order_number,AFPO.MATNR,ITEM_DESC.MAKTX ITEM_DESC,AFPO.MEINS UOM
,AFPO.PSAMG 	Scrap_quantity
,AFPO.PGMNG 	Total_planned_order_quantity
,AFPO.PSMNG 	Order_QTY 
,AFPO.WEMNG 	Quantity_goods_received
,AFPO.PAMNG 	Fixed_quantity_scrap,AFPO.WEWRT VALUE_OF_GOOD_RECEIVED
,AFPO.CHARG 	Batch_Number,AFPO.LGORT AFPO_LGORT

--AFKO
,AFKO.GSTRP Basic_Start_Date,AFKO.GLTRP AFKO_Basic_finish_date

--REF COLUMNS
,AUFK.OBJNR AUFK_OBJNR,AFKO.AUFPL AFKO_AUFPL 

FROM SAPABAP1.AUFK
INNER JOIN SAPABAP1.AFPO ON (AFPO.AUFNR=AUFK.AUFNR AND AFPO.MANDT=AUFK.MANDT)

LEFT OUTER JOIN  SAPABAP1.AFKO ON (AFKO.MANDT=AUFK.MANDT AND AFKO.AUFNR=AUFK.AUFNR)
LEFT OUTER JOIN  SAPABAP1.T003P ORD_TYPE  ON (ORD_TYPE.AUART=AUFK.AUART AND ORD_TYPE.SPRAS='E')
LEFT OUTER JOIN SAPABAP1.DD07T ORD_CAT ON (ORD_CAT.DOMVALUE_L=AUFK.AUTYP AND ORD_CAT.DDLANGUAGE='E' AND ORD_CAT.DOMNAME='AUFTYP')
LEFT OUTER JOIN SAPABAP1.T001 COMP  ON (COMP.BUKRS=AUFK.BUKRS AND COMP.MANDT=AUFK.MANDT )
LEFT OUTER JOIN SAPABAP1.T001W PLANT ON (PLANT.MANDT=AUFK.MANDT AND PLANT.WERKS=AUFK.WERKS)
LEFT OUTER JOIN SAPABAP1.TGSBT  BAREA  ON (BAREA.MANDT=AUFK.MANDT AND BAREA.GSBER=AUFK.GSBER AND BAREA.SPRAS='E') 
LEFT OUTER JOIN SAPABAP1.TKA01 CONTROL_AREA  ON (CONTROL_AREA.MANDT=AUFK.MANDT AND CONTROL_AREA.KOKRS=AUFK.KOKRS) 
LEFT OUTER JOIN SAPABAP1.T499S LOC  ON (LOC.MANDT=AUFK.MANDT AND LOC.WERKS=AUFK.WERKS AND LOC.STAND=AUFK.STORT) 
LEFT OUTER JOIN SAPABAP1.CEPCT   PRCT  ON (PRCT.MANDT=AUFK.MANDT AND PRCT.PRCTR=AUFK.PRCTR AND PRCT.SPRAS='E') 
LEFT OUTER JOIN SAPABAP1.MAKT ITEM_DESC ON (ITEM_DESC.MANDT=AFPO.MANDT AND ITEM_DESC.MATNR=AFPO.MATNR AND ITEM_DESC.SPRAS='E')
 
WHERE 1=1 AND AUFK.MANDT=300
--AND AUFK.ERDAT>='20211001'
--AND AUFK.AUFNR='100008016631'

ORDER BY  AUFK.ERDAT, AUFK.AUFNR
);

CREATE VIEW ETL.VW_CUSTOMERS AS
SELECT
        KNA1.MANDT AS client_number,
        KNA1.KUNNR AS customer_number,
        KNA1.LAND1 AS customer_country_key,
        KNA1.NAME1 AS customer_name,
        KNA1.NAME2 AS customer_name2,
        KNA1.ORT01 AS customer_city,
        KNA1.PSTLZ AS customer_postal_code,
        KNA1.REGIO AS customer_region,
        KNA1.SORTL AS customer_sortfield,
        KNA1.STRAS AS customer_street_houseno,
        KNA1.TELF1 AS customer_telephoneno,
        KNA1.TELFX AS customer_fax,
        KNA1.XCPDK AS customer_onetimeaccount,
        KNA1.ADRNR AS customer_address,
        KNA1.MCOD1 AS customer_searchmatchcode1,
        KNA1.MCOD2 AS customer_searchmatchcode2,
        KNA1.MCOD3 AS customer_searchmatchcode3,
        KNA1.ANRED AS customer_title,
        KNA1.BAHNE AS customer_express_trainstation,
        KNA1.BAHNS AS customer_trainstation,
        KNA1.BBBNR AS customer_internationallocation_1,
        KNA1.BBSNR AS customer_internationallocation_2,
        KNA1.BEGRU AS customer_authorization,
        KNA1.BRSCH AS customer_industry_key,
        KNA1.BUBKZ AS customer_internationallocation_checkdigit,
        CAST(KNA1.ERDAT AS date) AS customer_createdon,
        KNA1.ERNAM AS person_created_the_object,
        KNA1.FAKSD AS customer_central_buildingblock,
        KNA1.FISKN AS customer_accountno_masterrecord_with_fiscaladdress,
        KNA1.KNAZK AS customer_workingtime_calendar,
        KNA1.KNRZA AS customer_accountno_alternateplayer,
        KNA1.KONZS AS customer_groupkey,
        KNA1.KTOKD AS customer_accountgroup,
        KNA1.KUKLA AS customer_classification,
        KNA1.LIFNR AS accountno_vendor,
        KNA1.LIFSD AS customer_central_deliveryblock,
        KNA1.LOEVM AS customer_central_deliveryflag,
        KNA1.NAME3 AS customer_name3,
        KNA1.NAME4 AS customer_name4,
        KNA1.ORT02 AS customer_district,
        KNA1.PFACH AS customer_pobox,
        KNA1.PSTL2 AS customer_pobox_postalcode, 
        KNA1.COUNC AS customer_countrycode,
        KNA1.CITYC AS customer_citycode, 
        KNA1.RPMKR AS regional_market,
        KNA1.SPERR AS customer_central_postingblock,
        KNA1.SPRAS AS customer_languagekey,
        KNA1.STCD1 AS customer_taxno1,
        KNA1.STCD2 AS customer_taxno2,
        KNA1.STKZU AS customer_liableforVAT,
        KNA1.TELBX AS customer_teleboxno,
        KNA1.TELF2 AS customer_telephoneno2, 
        KNA1.TELTX AS customer_teletexno,
        KNA1.TELX1 AS customer_telexno, 
        KNA1.VBUND AS customer_companyid_tradingpartner,
        KNA1.STCEG AS VAT_registrationno,
        KNA1.DEAR1 AS competitor,
        KNA1.DEAR2 AS sales_partner,
        KNA1.DEAR3 AS sales_prospect,
        KNA1.DEAR4 AS customer_type4,
        KNA1.DEAR5 AS default_soldto_partyid,
        KNA1.GFORM AS legal_status,
        KNA1.BRAN1 AS industry_code1,
        KNA1.BRAN2 AS industry_code2,
        KNA1.BRAN3 AS industry_code3,
        KNA1.BRAN4 AS industry_code4,
        KNA1.BRAN5 AS industry_code5,
        KNA1.EKONT AS initial_contact,
        KNA1.UMSAT AS annual_sales,
        KNA1.UMJAH AS salesgiven_year,
        KNA1.UWAER AS salesfigure_currency, 
        KNA1.JMZAH AS no_of_employees_year,
        KNA1.JMJAH AS no_of_employees_yearly_given, 
        KNA1.KATR1 AS customer_attribute1,
        KNA1.KATR2 AS customer_attribute2,
        KNA1.KATR3 AS customer_attribute3,
        KNA1.KATR4 AS customer_attribute4,
        KNA1.KATR5 AS customer_attribute5,
        KNA1.KATR6 AS customer_attribute6,
        KNA1.KATR7 AS customer_attribute7,
        KNA1.KATR8 AS customer_attribute8,
        KNA1.KATR9 AS customer_attribute9,
        KNA1.KATR10 AS customer_attribute10,
        KNA1.STKZN AS natural_person,
        KNA1.TXJCD AS customer_tax_jurisdiction,
        KNA1.PERIV AS fiscal_year_variant,
        KNA1.ABRVW AS usage_indicator,
        KNA1.INSPBYDEBI AS customer_inspection_carriedout,
        KNA1.INSPATDEBI AS customer_inspection_deliverynote_outbound,
        KNA1.KTOCD AS reference_account_group_onetimeaccount,
        KNA1.PFORT AS customer_pobox_city,
        KNA1.WERKS AS plant,
        KNA1.SPERZ AS customer_paymentblock, 
        KNA1.CIVVE AS non_militaryuse_id,
        KNA1.MILVE AS militaryuse_id,
        KNA1.KDKG1 AS customer_conditiongroup1,
        KNA1.KDKG2 AS customer_conditiongroup2,
        KNA1.KDKG3 AS customer_conditiongroup3,
        KNA1.KDKG4 AS customer_conditiongroup4,
        KNA1.KDKG5 AS customer_conditiongroup5,
        KNA1.FITYP AS tax_type,
        KNA1.STCDT AS taxno_type,
        KNA1.STCD3 AS taxno_3,
        KNA1.STCD4 AS taxno_4,
        KNA1.STCD5 AS taxno_5,
        KNA1.XICMS AS is_customer_ICMSexempt,
        KNA1.XXIPI AS is_customer_IPIexempt,
        KNA1.CFOPC AS customer_CFOP_category,
        KNA1.TXLW1 AS tax_law_ICMS,
        KNA1.TXLW2 AS tax_law_IPI,
        KNA1.CASSD AS customer_central_salesblock,
        KNA1.KNURL AS uniform_resource_locator,
        KNA1.J_1KFREPRE AS representative_name,
        KNA1.J_1KFTBUS AS  business_type,
        KNA1.J_1KFTIND AS  industry_type,
        KNA1.CONFS AS status_of_authorizationchange,
        CAST(KNA1.UPDAT AS date) AS confirmedchanges_date,  
        KNA1.UPTIM AS confirmedchanges_time,  
        KNA1.NODEL AS customer_central_deletionblock, 
        KNA1.DEAR6 AS consumer,
        KNA1.SUFRAMA AS suframa_code,
        KNA1.RG AS RG_no,
        KNA1."EXP" AS issued_by,
        KNA1.UF AS state,
        CAST(KNA1.RGDATE AS DATE) AS RG_issue_date,
        KNA1.RIC AS RIC_no,
        KNA1.RNE AS foreign_national_registration,
        CAST(KNA1.RNEDATE AS DATE) AS RNE_issue_date,
        KNA1.CNAE AS CNAE,
        KNA1.LEGALNAT AS legal_nature, 
        KNA1.CRTN AS CRT_no,
        KNA1.ICMSTAXPAY AS ICMS_taxpayer,
        KNA1.INDTYP AS industry_main_type, 
        KNA1.TDT AS tax_declaration_type,
        KNA1.COMSIZE AS company_size,
        KNA1.ALC AS agency_location_code,
        KNA1.PMT_OFFICE AS payment_office,
        KNA1.PSOFG AS processor_group,
        KNA1.PSON1 AS name1,
        KNA1.PSON2 AS name2,
        KNA1.PSON3 AS name3,
        KNA1.PSOVN AS first_name,
        KNA1.PSOTL AS title,
        KNA1.ZZDEALER AS dealer,
        KNA1.ZZLICENSE AS license,
        CAST(KNA1.ZZLDATEFROM AS DATE) AS ZZL_datefrom,
        CAST(KNA1.ZZLDATETO AS DATE) AS ZZL_dateto,
        CAST(KNA1.ZZDDATEFROM  AS DATE) ZZ_datefrom,
        CAST(KNA1.ZZDDATETO  AS DATE) ZZ_dateto,
        KNA1.ZZDRUGLICENSENO AS license_no, 
        KNA1.ZZEXPIRYDATE AS  expiry_date,
        KNB1.BUKRS AS company_code,
        KNB1.PERNR AS personnel_number,
        KNB1.SPERR AS postingblock_company_code, 
        KNB1.LOEVM AS company_code_level_deletionflag, 
        KNB1.BUSAB AS accounting_clerk,
        KNB1.AKONT AS reconcilation_account_in_GL,
        KNB1.BEGRU AS authorization_group,
        KNB1.KNRZE AS headoffice_account_no,
        KNB1.KNRZB AS alternateplayer_account_no, 
        KNB1.ZAMIM AS customer_payment_notice_cleared_items,
        KNB1.ZAMIV AS sales_dept_payment_notice,
        KNB1.ZAMIR AS legal_dept_payment_notice,
        KNB1.ZAMIB AS accounting_dept_payment_notice,
        KNB1.ZAMIO AS customer_payment_notice_wo_cleared_items,
        KNB1.ZWELS AS  payment_methods_to_consider,
        KNB1.XVERR AS clearing_between_customer_and_vendor,
        KNB1.ZAHLS AS payment_block_key,
        KNB1.ZTERM AS payment_terms_key,
        KNB1.VZSKZ AS interest_calculation_indicator,
        CAST(KNB1.ZINDT AS date) AS keydate_last_interest_calculation, 
        KNB1.ZINRT AS interest_calculation_frequency_in_months, 
        KNB1.EIKTO AS accountno_at_customer,
        KNB1.ZSABE AS user_at_customer,
        KNB1.KVERM AS memo,
        KNB1.FDGRV AS planning_group,
        KNB1.VLIBB AS amount_insured,
        KNB1.VRSZL AS insurance_lead_month,
        KNB1.VRSPR AS deductible_percentage_rate,
        KNB1.VRSNR AS insurance_number,
        CAST(KNB1.VERDT AS date) AS insurance_validity_date,
        KNB1.PERKZ AS collective_invoice_variant,
        KNB1.XDEZV AS local_processing,
        KNB1.XAUSZ AS periodic_account_statements,
        KNB1.WEBTR AS exchange_limit_bill,
        KNB1.REMIT AS next_payee,
        CAST(KNB1.DATLZ AS date) AS last_insurance_calculation_run_date,
        KNB1.XZVER AS record_payment_history,
        KNB1.TOGRU AS tolerance_group_for_business_partner_GL,
        KNB1.KULTG AS probable_time_until_check_is_paid, 
        KNB1.HBKID AS house_bank_shortkey,
        KNB1.XPORE AS pay_all_items_seperately,
        KNB1.ALTKN AS previous_master_record_no,
        KNB1.ZGRUP AS payment_grouping_key,
        KNB1.URLID AS known_leave_shortkey, 
        KNB1.MGRUP AS dunning_notice_grouping_shortkey,
        KNB1.LOCKB AS lockbox_shortkey,
        KNB1.UZAWE AS payment_method_supplement,
        KNB1.EKVBD AS buying_group_accountno,
        KNB1.XEDIP AS send_payment_advices_EDI,
        KNB1.FRGRP AS release_approval_group,
        KNB1.VRSDG AS reason_code_conversion_version,
        KNB1.INTAD AS internet_address_partner_company_clerk,
        KNB1.XKNZB AS alternate_payer_using_account_no, 
        KNB1.GUZTE AS credit_memos_payment_key,
        KNB1.GRICD AS activity_code_gross_income_tax,
        KNB1.GRIDT AS employment_tax_distribution_type,
        KNB1.WBRSL AS value_adjustment_key,
        KNB1.CONFS AS authorization_change_status,
        CAST(KNB1.UPDAT AS date) AS  confirmed_changes_update_date,
        KNB1.UPTIM AS  confirmed_changes_update_time,
        KNB1.CESSION_KZ AS accounts_receivable_pledging_indicator,
        KNB1.AVSND AS send_payment_advices_XML,
        KNB1.AD_HASH AS email_address_for_Avis,
        KNB1.QLAND AS withholding_tax_country_key,  
        KNB1.GMVKZD AS customer_is_in_execution,        
        KNVV.VKORG AS sales_organization,
        KNVV.VTWEG AS distribution_channel,
        KNVV.SPART AS division,
        KNVV.LOEVM AS customer_deletion_flag,
        KNVV.VERSG AS customer_statistics_group,
        KNVV.AUFSD AS customer_order_block,
        KNVV.KALKS AS pricing_procedure_assigned_To_customer,
        KNVV.KDGRP AS customer_group,
        KNVV.BZIRK AS sales_district,
        KNVV.KONDA AS customer_price_group,
        KNVV.PLTYP AS price_list_type,
        KNVV.AWAHR AS item_order_probability,
        KNVV.INCO1 AS incoterms_part1,
        KNVV.INCO2 AS incoterms_part2,
        KNVV.LIFSD AS customer_delivery_block_sales,
        KNVV.AUTLF AS complete_delivery_each_sales_order,
        KNVV.ANTLF AS max_partial_deliveries_allowed_per_item,
        KNVV.KZTLF AS partial_delivery_item_level,
        KNVV.KZAZU AS order_combination_indicator,
        KNVV.CHSPL AS batch_split_allowed,
        KNVV.LPRIO AS delivery_priority,
        KNVV.EIKTO AS shipper_accountno_at_customer,
        KNVV.VSBED AS shipping_conditions,
        KNVV.FAKSD AS customers_sales_billing_block,
        KNVV.MRNKZ AS manual_invoice_maintenance,
        KNVV.PERFK AS invoice_dates,
        KNVV.PERRL AS invoice_list_schedule,
        KNVV.KVAKZ AS cost_estimate_indicator,
        KNVV.KVAWT AS cost_estimate_valuelimit,
        KNVV.WAERS AS currency,
        KNVV.KLABC AS customer_classification_abcanalysis,
        KNVV.KTGRD AS account_assignment_group,
        KNVV.ZTERM AS terms_payment_key,
        KNVV.VWERK AS delivery_plant,
        KNVV.VKGRP AS sales_group,
        KNVV.VKBUR AS sales_office,
        KNVV.VSORT AS item_proposal,
        KNVV.KVGR1 AS customer_group1,
        KNVV.KVGR2 AS customer_group2,
        KNVV.KVGR3 AS customer_group3,
        KNVV.KVGR4 AS customer_group4,
        KNVV.KVGR5 AS customer_group5,
        KNVV.BOKRE AS is_customer_relevant,
        KNVV.KURST AS exchange_rate_type,
        KNVV.PRFRE AS relevant_price_determination_id,
        KNVV.PRAT1 AS product_attribute1_id,
        KNVV.PRAT2 AS product_attribute2_id,
        KNVV.PRAT3 AS product_attribute3_id,
        KNVV.PRAT4 AS product_attribute4_id,
        KNVV.PRAT5 AS product_attribute5_id,
        KNVV.PRAT6 AS product_attribute6_id,
        KNVV.PRAT7 AS product_attribute7_id,
        KNVV.PRAT8 AS product_attribute8_id,
        KNVV.PRAT9 AS product_attribute9_id,
        KNVV.PRATA AS product_attribute10_id,
        KNVV.KABSS AS customer_payment_guarantee_procedure,
        KNVV.KKBER AS credit_control_area,
        KNVV.CASSD AS customer_sales_block,
        KNVV.AGREL AS relevant_agency_business,
        KNVV.MEGRU AS measure_group_unit,
        KNVV.UEBTO AS overdelivery_tolerance_limit,
        KNVV.UNTTO AS underdelivery_tolerance_limit,
        KNVV.UEBTK AS unlimited_overdelivery_allowed,
        KNVV.PVKSM AS customer_procedure_product_proposal,
        KNVV.CARRIER_NOTIF AS carrier_to_notified
        FROM        
        SAPABAP1.KNA1 
        LEFT JOIN SAPABAP1.KNB1 ON (KNA1.MANDT = KNB1.MANDT AND KNA1.KUNNR = KNB1.KUNNR AND  KNA1.SPRAS='E')
        LEFT JOIN SAPABAP1.KNVV ON (KNA1.MANDT = KNVV.MANDT AND KNA1.KUNNR = KNVV.KUNNR AND  KNVV.VKORG IN (6100,6300) )
        WHERE 1=1 AND KNA1.MANDT=300;

CREATE VIEW ETL.VW_LOCATIONS AS
SELECT WERKS,LGORT,LGOBE
FROM SAPABAP1.T001L WHERE MANDT =300;

CREATE VIEW ETL.VW_MARKITT_ITEMS_NOT_OPENED AS
SELECT DISTINCT MATERIAL FROM SAPABAP1.ZMARKIT_item_LOG 
WHERE 1=1 AND billdate BETWEEN '20221001' AND '20221130'
AND upper(MATERIAL) NOT IN (
SELECT DISTINCT upper(normt) FROM MARKITT_ITEMS 
) ORDER BY 1;

CREATE VIEW ETL.VW_MSEG AS
SELECT 
MSEG.MANDT CLIENT
,MSEG.AUFNR ORDER_NUMBER
,MSEG.MBLNR MATERIAL_DOC_NUMBER,MSEG.MJAHR DOC_YEAR,MSEG.DMBTR AMOUNT,MSEG.BNBTR DELIVERY_COST,MSEG.BUALT AMOUNT_POSTED
,MSEG.SHKUM DEBIT_CREDIT,MSEG.MATNR ITEM_CODE,MAKT.MAKTX ITEM_DESC,AFPO.PGMNG 	Total_planned_order_quantity, MSEG.MENGE QTY
 FROM SAPABAP1.MSEG 
 LEFT OUTER JOIN SAPABAP1.AFPO ON (AFPO.MANDT=MSEG.MANDT AND AFPO.AUFNR=MSEG.AUFNR)
 LEFT OUTER JOIN SAPABAP1.MAKT ON (MAKT.MANDT=MSEG.MANDT AND MAKT.MATNR=MSEG.MATNR)
WHERE 1=1 AND MSEG.MANDT=300 
---AND MSEG.AUFNR='100008016631'
--AND MATNR LIKE '%1015000065'
AND BWART IN ( '101');

CREATE VIEW ETL.VW_PM_TABLES_COUNT AS
SELECT 'AUFK',count(*) REC_COUNT FROM SAPABAP1.AUFK
 UNION ALL
 SELECT 'AFKO',count(*) FROM SAPABAP1.AFKO
 UNION ALL
 SELECT 'AFPO',count(*) FROM SAPABAP1.AFPO
 UNION ALL
 SELECT 'RESB',count(*) FROM SAPABAP1.RESB
 UNION ALL
 SELECT 'AFVC',count(*) FROM SAPABAP1.AFVC
 UNION ALL
 SELECT 'AFVV',count(*) FROM SAPABAP1.AFVV
 UNION ALL
 SELECT 'AFVU',count(*) FROM SAPABAP1.AFVU
 UNION ALL
 SELECT 'AFFL',count(*) FROM SAPABAP1.AFFL
 UNION ALL
 SELECT 'JEST',count(*) FROM SAPABAP1.JEST
 UNION ALL
 SELECT 'JSTO',count(*) FROM SAPABAP1.JSTO
 UNION ALL
 SELECT 'AFRU',count(*) FROM SAPABAP1.AFRU
 UNION ALL
 SELECT 'S022',count(*) FROM SAPABAP1.S022
 UNION ALL
 SELECT 'AFIH',count(*) FROM SAPABAP1.AFIH
 UNION ALL
 SELECT 'AUFM',count(*) FROM SAPABAP1.AUFM;

CREATE VIEW ETL.VW_RESB AS
SELECT 

RESB.MANDT MANDT_CLIENT
,RESB.AUFNR Order_Number,RESB.KDAUF 	Sales_Order_Number,RESB.BWART 	Movement_Type
,RESB.RSNUM RSNUM_RESERVATION_NUMBER,RESB.RSPOS RSPOS_ITEM_NUMBER,RESB.RSART RSART_RECORD_TYPE,RESB.BDART BDART_REQUIREMENT_TYPE
,RESB.RSSTA RSSTA_RESERVATION_STATUS
,RESB.XLOEK XLOEK_ITEM_DELETED,RESB.XWAOK XWAOK_GOOD_MVMNT_ALLW,RESB.KZEAR KZEAR_FINAL_ISSUE_RESERVATION
,RESB.XFEHL XFEHL_MISSING_PART
,RESB.MATNR MATNR_ITEM_CODE

,MAKT.MAKTG ITEM_DESC,RESB.SGTXT,   RESB.WERKS WERKS_PLANT,T001W.NAME1 WERKS_PLANT_DESC
,RESB.LGORT LGORT_STORAGE_LOC
,T001L.LGOBE STORAGE_LOC_DESC
,RESB.PRVBE 	PRVBE_Prod_Supply_Area,RESB.CHARG 	CHARG_CBatch_Number,RESB.SOBKZ SOBKZ_STOCK_INDICATOR,RESB.BDTER Req_DATE_Component,RESB.BDMNG Requirement_QTY
,RESB.MEINS 	Base_Unit,RESB.SHKZG 	Debit_CREDIT,RESB.FMENG 	QTY_FXD,RESB.ENMNG QTY_WDRAW,ENWRT 	Value_WDRAW,RESB.WAERS 	CurrencY,RESB.ERFMG QTY_UNIT_ENTRY
,RESB.NOMNG REQUIRED_QTY
,RESB.PLNUM 	Planned_ORD_NUM
,RESB.BANFN 	Pur_Req_Number,RESB.BNFPO 	Item_NUM_PUR_REQ 
,RESB.SAKNR 	GL_Account ,SKAT.TXT50 GL_ACCOUNT_DESC
,RESB.OBJNR

FROM SAPABAP1.RESB

LEFT OUTER JOIN SAPABAP1.MAKT ON (MAKT.MANDT=RESB.MANDT AND MAKT.MATNR=RESB.MATNR AND MAKT.SPRAS='E')
LEFT OUTER JOIN SAPABAP1.T001W ON (T001W.MANDT=RESB.MANDT AND T001W.WERKS=RESB.WERKS)
LEFT OUTER JOIN SAPABAP1.T001L  ON (T001L.MANDT=RESB.MANDT AND T001L.WERKS=RESB.WERKS  AND T001L.LGORT=RESB.LGORT)
LEFT OUTER JOIN SAPABAP1.SKAT  ON (SKAT.MANDT=RESB.MANDT AND SKAT.SAKNR=RESB.SAKNR  AND SKAT.SPRAS='E')

WHERE 1=1 AND RESB.MANDT=300 order by RESB.RSPOS;

CREATE VIEW ETL.VW_TVAUT AS
SELECT "MANDT" , "SPRAS" , "AUGRU" , "BEZEI" FROM sapabap1.TVAUT WHERE SPRAS ='E' AND MANDT=300;

CREATE VIEW ETL.VW_VBAP_DATA_ORDER AS
SELECT 
  DISTINCT 
  VBAK.VBELN ,VBAK.AUART ,VBAK.VKBUR ,VBAK.BSTNK SALESFLO_ORDER_NUMBER ,VBAK.VKORG ,VBAP.VBELN VBAP_VBELN ,
  NULL POSNR,
  ---VBAP.POSNR ,
  VBAP.MATNR ,VBAP.ERDAT ,VBAK.ERDAT  VBAK_ERDAT,VBAP.WERKS PLANTS,VBAP.PSTYV ITEM_CATEGORY
  FROM SAPABAP1.VBAK VBAK , SAPABAP1.VBAP VBAP
  WHERE 1=1   AND VBAK.VBELN =VBAP.VBELN  
  --  AND VBAP.VBELN='3900229830'
    AND VBAP.ERDAT ='20220202' 
    AND VBAP.WERKS  IN (6100,6300)  
    AND VBAK.BSTNK<>'';

CREATE VIEW ETL.VW_ZMARKIT_ITEM AS
SELECT 
MANDT,BILLNO,SERIALNO,BRANCH,CAST(to_char(BILLDATE,'yyyy-mm-dd') AS date)BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG
FROM SAPABAP1.ZMARKIT_ITEM zil
WHERE 1=1;

CREATE VIEW ETL.ZBW_ACTUALVSBUDGETFINAL AS
SELECT "RYEAR" , "SETNAME" , "RCLNT" , "RBUKRS" , "PRCTR" , "RRCTY" , "DRCRK" , "HSL01" , "HSL02" , "HSL03" , "HSL04" , "HSL05" , "HSL06" , "HSL07" , "HSL08" , "HSL09" , "HSL10" , "HSL11" , "HSL12" , "GL_ACCOUNT" , "RECORD_TYPE" FROM (SELECT
	 A.RYEAR ,
	B.SETNAME ,
	A.RCLNT ,
	A.RBUKRS ,
	A.PRCTR ,
	A.RRCTY ,
	A.DRCRK ,
	HSL01 ,
	HSL02 ,
	HSL03 ,
	HSL04 ,
	HSL05 ,
	HSL06 ,
	HSL07 ,
	HSL08 ,
	HSL09 ,
	HSL10 ,
	HSL11 ,
	HSL12 ,
	A.RACCT GL_ACCOUNT ,
	CASE WHEN B.SETNAME IN ('ZPCTSCL008' ,
	'ZPCTSCL009' ,
	'ZPCTSCL010' ,
	'ZPCTSCL016' ,
	'ZPCTSCL017' ,
	'ZPCTSCL018' ,
	'ZPCTSCL011' ,
	'ZPCTSCL013' ,
	'ZPCTSCL014' ,
	'ZPCTSCL015' ,
	'ZPCTSCL002') 
	THEN 'Pharma' WHEN B.SETNAME = 'ZPCTSCL005' 
	THEN 'Consumer' WHEN B.SETNAME = 'ZPCTSCL012' 
	THEN 'Searl Biosciences' WHEN B.SETNAME = 'ZPCTSCL019' 
	THEN 'Toll manufacturing' WHEN B.SETNAME = 'ZPCTSCL004' 
	THEN 'Nutrition' WHEN B.SETNAME = 'ZPCTSCL042' 
	THEN 'Disposables' WHEN B.SETNAME = 'ZPCTSCL007' 
	THEN 'Institution' WHEN B.SETNAME IN ('ZPCTSCL043' ,
	'ZPCTSCL021' ,
	'ZPCTSCL022' ,
	'ZPCTSCL023' ,
	'ZPCTSCL040' ,
	'ZPCTSCL024' ,
	'ZPCTSCL026' ,
	'ZPCTSCL027' ,
	'ZPCTSCL028' ,
	'ZPCTSCL039' ,
	'ZPCTSCL029' ,
	'ZPCTSCL030' ,
	'ZPCTSCL031' ,
	'ZPCTSCL035' ,
	'ZPCTSCLSG054' ,
	'ZPCTSCL037' ,
	'ZPCTSCL034') 
	THEN 'GBD' 
	ELSE B.SETNAME 
	END RECORD_TYPE 
	FROM SAPABAP1.FAGLFLEXT A 
	INNER JOIN SAPABAP1.SETLEAF B ON (B.MANDT = A.RCLNT 
		AND B.VALFROM = A.PRCTR) 
	WHERE 1 = 1 
	AND RYEAR > '2018' 
	AND A.RYEAR <= YEAR(CURRENT_DATE) + 1 --in (2019,2020)
 
	AND A.RCLNT = 300 
	AND RBUKRS = 1000 --AND A.PRCTR='1010110406'
 
	AND A.RCLNT = B.MANDT 
	AND B.VALFROM = A.PRCTR 
	AND B.SETNAME IN ('ZPCTSCL008' ,
	'ZPCTSCL009' ,
	'ZPCTSCL010' ,
	'ZPCTSCL016' ,
	'ZPCTSCL017' ,
	'ZPCTSCL018' ,
	'ZPCTSCL011' ,
	'ZPCTSCL013' ,
	'ZPCTSCL014' ,
	'ZPCTSCL015' ,
	'ZPCTSCL002' ,
	'ZPCTSCL005' ,
	'ZPCTSCL012' ,
	'ZPCTSCL019' ,
	'ZPCTSCL004' ,
	'ZPCTSCL042' ,
	'ZPCTSCL007' --GBD
 ,
	'ZPCTSCL043' ,
	'ZPCTSCL021' ,
	'ZPCTSCL022' ,
	'ZPCTSCL023' ,
	'ZPCTSCL040' ,
	'ZPCTSCL024' ,
	'ZPCTSCL026' ,
	'ZPCTSCL027' ,
	'ZPCTSCL028' ,
	'ZPCTSCL039' ,
	'ZPCTSCL029' ,
	'ZPCTSCL030' ,
	'ZPCTSCL031' ,
	'ZPCTSCL035' ,
	'ZPCTSCLSG054' ,
	'ZPCTSCL037' ,
	'ZPCTSCL034') 
	AND EXISTS (SELECT
	 1 
		FROM SAPABAP1.SETLEAF STL 
		WHERE 1 = 1 
		AND SETNAME IN ('ZPCFS181',
	 'ZPCFS182',
	 'ZPCFS183') 
		AND STL.VALFROM = A.RACCT)) ORDER BY RECORD_TYPE;

CREATE VIEW ETL.ZBW_BUDGETSEARLE AS
select
	 year,
	 currency,
	 general_ledger_accounting,
	 RRCTY,
	 RVERS,
	 GL_ACCOUNT,
	 PRCTR,
	 PRCTR_DESC,
	 LTEXT,
	 COST_ELEMENT,
	 DRCRK,
	 HSL01,
	 HSL02,
	 HSL03,
	 HSL04,
	 HSL05,
	 HSL06,
	 HSL07,
	 HSL08,
	 HSL09,
	 HSL10,
	 HSL11,
	 HSL12,
	 (HSL01+HSL02+HSL03+HSL04+HSL05+HSL06+HSL07+HSL08+HSL09+HSL10+HSL11+HSL12)MONTH_TOTAL,
	 RBUKRS 
from ( SELECT
	 A.RYEAR year,
	 A.RTCUR currency,
	 A.RLDNR General_ledger_accounting,
	 A.RRCTY,
	 RVERS,
	 A.RACCT GL_ACCOUNT,
	 A.PRCTR,
	 CASE WHEN A.PRCTR='0000000011' 
	THEN 'SEARLE' WHEN A.PRCTR='2302010101' 
	THEN 'CONSUMER' WHEN A.PRCTR='2302050001' 
	THEN 'INSTITUTION' WHEN A.PRCTR='3000000001' 
	THEN 'GLOBAL BUSINESS DIVISION' WHEN A.PRCTR='2302060001' 
	THEN 'NUTRITION' WHEN A.PRCTR='2302130001' 
	THEN 'SEARLE BIOSCIENCES' WHEN A.PRCTR='2302310001' 
	THEN 'TOLL MANUFACTURING' WHEN A.PRCTR IN ('2302100001',
	 '2302110001',
	 '2302120001',
	 '2302130001',
	 '2302140001',
	 '2302150001' ,
	 '2302160001',
	 '2302170001',
	 '2302180001',
	 '2302190001',
	 '2302200001',
	 '2302310001') 
	THEN 'PHARMA' 
	ELSE A.PRCTR 
	END PRCTR_DESC,
	 B.LTEXT,
	 A.COST_ELEM COST_ELEMENT,
	 A.DRCRK ,
	 A.HSL01,
	 A.HSL02,
	 A.HSL03,
	 A.HSL04,
	 A.HSL05 ,
	 A.HSL06,
	 A.HSL07,
	 A.HSL08 ,
	 A.HSL09,
	 A.HSL10,
	 A.HSL11 ,
	 A.HSL12,
	 A.KOKRS,
	 A.PPRCTR,
	 A.RBUKRS 
	FROM sapabap1.FAGLFLEXT A 
	inner JOIN SAPABAP1.CEPCT B ON (B.MANDT=A.RCLNT 
		AND B.PRCTR=A.PRCTR) 
	WHERE RRCTY = '1' 
	AND A.RYEAR > '2018' 
	and a.ryear<=year(CURRENT_DATE)+1 
	AND A.RCLNT=300 
	AND B.MANDT=A.RCLNT 
	AND RACCT IN ( '0401020105',
	 '0501020150',
	 '0502010105',
	 '0519080305',
	 '0403020110',
	 '0508010105',
	 '0519010105',
	 '0522010105' ) 
	AND RBUKRS = '1000' 
	AND a.PRCTR in ('0000000011') ) ORDER BY year;

CREATE VIEW ETL.ZBW_CONSOLIDATE_INCOME_STATEMENT AS
select
	 bukrs COMPANY_CODE,
	 CASE WHEN BUKRS='1000' 
THEN 'The Searle Company Ltd' WHEN BUKRS='1100' 
THEN 'SPPL' WHEN BUKRS='1500' 
AND SETNAME='ZSBSPHARMA' 
THEN 'PHARMA' WHEN BUKRS='1500' 
AND SETNAME='ZSBSBIO' 
THEN 'BIO' WHEN BUKRS='1600' 
THEN 'NEXTAR' WHEN BUKRS='1200' 
THEN 'IBLHC' 
ELSE BUKRS 
end COMPANY_DESC,
	 gjahr,
	 case when shkzg='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END dmbtr,
	 hkont,
	 shkzg,
	 pswsl,
	 prctr,
	 kokrs,
	 kostl,
	 kunnr,
	 lifnr,
	 blart,
	 budat,
	 setname ,
	 GL_SET,
	 CASE WHEN SETNAME IN ('ZGLSPPL01',
	 'ZGLSBS01',
	 'ZNXT01',
	 'ZPCFS163',
	 'ZPCFS181' ) 
THEN 'Revenue' WHEN SETNAME IN ('ZGLSPPL02',
	 'ZGLSBS02',
	 'ZNXT02',
	 'ZPCFS164') 
THEN 'Cost of sales' when setname in ('ZGLSPPL03',
	 'ZGLSBS03',
	 'ZNXT03',
	 'ZPCFSHC03',
	 'ZPCFSHC04') 
THEN 'Operating expenses' when setname in ('ZGLSPPL04',
	 'ZGLSBS04',
	 'ZNXT04',
	 'ZPCFSHC05') 
then 'Other income' when setname in ('519010105',
	 'ZGLSPPL05',
	 'ZGLSBS05',
	 'NXNT05',
	 '') 
then 'Financial & Other Expenses' when setname in ('ZGLSPPL06',
	 'ZGLSBS06',
	 'ZNXT06',
	 'ZPCFSHC06') 
then 'Taxation' ---changes For SBS
when SETNAME IN ('ZPCFSHC07') 
THEN 'Financial & Other expenses' WHEN gl_set in ('ZGLSBS01') 
and SETNAME IN ('ZSBSPHARMA' ,
	 'ZSBSBIO') 
THEN 'Revenue' WHEN gl_set in ('ZGLSBS02') 
and SETNAME IN ('ZSBSPHARMA' ,
	 'ZSBSBIO') 
THEN 'Cost of sales' WHEN gl_set in ('ZGLSBS03') 
and SETNAME IN ('ZSBSPHARMA' ,
	 'ZSBSBIO') 
THEN 'Operating expenses' WHEN gl_set in ('ZGLSBS04') 
and SETNAME IN ('ZSBSPHARMA' ,
	 'ZSBSBIO') 
THEN 'Other income' WHEN gl_set in ('ZGLSBS05') 
and SETNAME IN ('ZSBSPHARMA' ,
	 'ZSBSBIO') 
THEN 'Financial & Other Expenses' WHEN gl_set in ('ZGLSBS06') 
and SETNAME IN ('ZSBSPHARMA' ,
	 'ZSBSBIO') 
THEN 'Taxation' 
else setname 
END SETNAME_DESC 
from ( SELECT
	 a.bukrs,
	 a.gjahr,
	 a.dmbtr,
	 a.hkont,
	 a.shkzg,
	 a.pswsl,
	 a.prctr,
	 a.kokrs,
	 a.kostl,
	 a.kunnr,
	 a.lifnr,
	 b.blart,
	 b.budat,
	 c.setname ,
	 NULL GL_SET 
	FROM sapabap1.BSEG AS a 
	INNER JOIN sapabap1.BKPF as b ON a.bukrs = b.bukrs 
	AND a.BELNR = B.BELNR 
	AND a.gjahr = b.gjahr 
	INNER JOIN sapabap1.SETLEAF as c ON a.HKONT = c.VALFROM 
	WHERE a.BUKRS = 1100 
	AND a.BUKRS = 1100 
	AND ( a.hkont IN ( SELECT
	 VALFROM 
			FROM sapabap1.SETLEAF 
			WHERE SETNAME IN ('ZGLSPPL01',
	 'ZGLSPPL02',
	 'ZGLSPPL03',
	 'ZGLSPPL04',
	 'ZGLSPPL05',
	 'ZGLSPPL06') ) 
		OR a.hkont IN ( SELECT
	 VALTO 
			FROM sapabap1.SETLEAF 
			WHERE SETNAME IN ('ZGLSPPL01',
	 'ZGLSPPL02',
	 'ZGLSPPL03',
	 'ZGLSPPL04',
	 'ZGLSPPL05',
	 'ZGLSPPL06') ) ) 
	and b.budat < current_date 
	AND c.SETNAME in ('ZGLSPPL01',
	 'ZGLSPPL02',
	 'ZGLSPPL03',
	 'ZGLSPPL04',
	 'ZGLSPPL05',
	 'ZGLSPPL06') 
	UNION all SELECT
	 a.bukrs,
	 a.gjahr,
	 a.dmbtr,
	 a.hkont,
	 a.shkzg,
	 a.pswsl,
	 a.prctr,
	 a.kokrs,
	 a.kostl,
	 a.kunnr,
	 a.lifnr,
	 b.blart,
	 b.budat,
	 c.setname ,
	 NULL GL_SET 
	FROM sapabap1.BSEG AS a 
	INNER JOIN sapabap1.BKPF as b ON a.bukrs = b.bukrs 
	AND a.BELNR = B.BELNR 
	AND a.gjahr = b.gjahr 
	INNER JOIN sapabap1.SETLEAF as c ON a.HKONT = c.VALFROM 
	WHERE a.BUKRS = 1200 
	AND a.BUKRS = 1200 
	AND ( a.hkont IN ( SELECT
	 VALFROM 
			FROM sapabap1.SETLEAF 
			WHERE SETNAME IN ('ZPCFS163',
	 'ZPCFS164',
	 'ZPCFSHC03',
	 'ZPCFSHC04',
	 'ZPCFSHC05',
	 'ZPCFSHC06',
	 'ZPCFSHC07',
	 'ZPCFS181',
	 'ZPCFS182',
	 'ZPCFS183') ) 
		OR a.hkont IN ( SELECT
	 VALTO 
			FROM sapabap1.SETLEAF 
			WHERE SETNAME IN ('ZPCFS163',
	 'ZPCFS164',
	 'ZPCFSHC03',
	 'ZPCFSHC04',
	 'ZPCFSHC05',
	 'ZPCFSHC06',
	 'ZPCFSHC07',
	 'ZPCFS181',
	 'ZPCFS182',
	 'ZPCFS183') ) ) 
	and b.budat < current_date 
	AND c.SETNAME in ('ZPCFS163',
	 'ZPCFS164',
	 'ZPCFSHC03',
	 'ZPCFSHC04',
	 'ZPCFSHC05',
	 'ZPCFSHC06',
	 'ZPCFSHC07',
	 'ZPCFS181',
	 'ZPCFS182',
	 'ZPCFS183') 
	UNION all SELECT
	 a.bukrs,
	 a.gjahr,
	 a.dmbtr,
	 a.hkont,
	 a.shkzg,
	 a.pswsl,
	 a.prctr,
	 a.kokrs,
	 a.kostl,
	 a.kunnr,
	 a.lifnr,
	 b.blart,
	 b.budat,
	 d.setname ,
	 C.SETNAME GL_SET 
	FROM sapabap1.BSEG AS a 
	INNER JOIN sapabap1.BKPF as b ON a.bukrs = b.bukrs 
	AND a.BELNR = B.BELNR 
	AND a.gjahr = b.gjahr 
	INNER JOIN sapabap1.SETLEAF as c ON a.HKONT = c.VALFROM 
	INNER JOIN sapabap1.SETLEAF as d ON a.prctr = d.valfrom 
	WHERE a.BUKRS = 1500 
	AND a.BUKRS = 1500 
	AND ( a.hkont IN ( SELECT
	 VALFROM 
			FROM sapabap1.SETLEAF 
			WHERE SETNAME IN ('ZGLSBS01',
	 'ZGLSBS02',
	 'ZGLSBS03',
	 'ZGLSBS04',
	 'ZGLSBS05',
	 'ZGLSBS06' ) ) 
		OR a.hkont IN ( SELECT
	 VALTO 
			FROM sapabap1.SETLEAF 
			WHERE SETNAME IN ('ZGLSBS01',
	 'ZGLSBS02',
	 'ZGLSBS03',
	 'ZGLSBS04',
	 'ZGLSBS05',
	 'ZGLSBS06' ) ) ) 
	AND d.setname IN ('ZSBSPHARMA',
	 'ZSBSBIO') 
	and b.budat < current_date 
	AND c.SETNAME in ('ZGLSBS01',
	 'ZGLSBS02',
	 'ZGLSBS03',
	 'ZGLSBS04',
	 'ZGLSBS05',
	 'ZGLSBS06' ) 
	AND d.subclass = '' 
	UNION all SELECT
	 a.bukrs,
	 a.gjahr,
	 a.dmbtr,
	 a.hkont,
	 a.shkzg,
	 a.pswsl,
	 a.prctr,
	 a.kokrs,
	 a.kostl,
	 a.kunnr,
	 a.lifnr,
	 b.blart,
	 b.budat,
	 c.setname ,
	 NULL GL_SET 
	FROM sapabap1.BSEG AS a 
	INNER JOIN sapabap1.BKPF as b ON a.bukrs = b.bukrs 
	AND a.BELNR = B.BELNR 
	AND a.gjahr = b.gjahr 
	INNER JOIN sapabap1.SETLEAF as c ON a.HKONT = c.VALFROM 
	WHERE a.BUKRS = 1600 
	AND a.BUKRS = 1600 
	AND ( a.hkont IN ( SELECT
	 VALFROM 
			FROM sapabap1.SETLEAF 
			WHERE SETNAME IN ('ZNXT01',
	 'ZNXT02',
	 'ZNXT03',
	 'ZNXT04',
	 'NXNT05',
	 'ZNXT06') ) 
		OR a.hkont IN ( SELECT
	 VALTO 
			FROM sapabap1.SETLEAF 
			WHERE SETNAME IN ('ZNXT01',
	 'ZNXT02',
	 'ZNXT03',
	 'ZNXT04',
	 'NXNT05',
	 'ZNXT06') ) ) 
	and b.budat < current_date 
	AND c.SETNAME in ('ZNXT01',
	 'ZNXT02',
	 'ZNXT03',
	 'ZNXT04',
	 'NXNT05',
	 'ZNXT06') ) order by bukrs;

CREATE VIEW ETL.ZBW_IBLHEALTHCARE_GL_DATA AS
(SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 E.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Administrative Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('ZPCFSHC03',
	 'ZPCFSHC04' ) ) 
INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
	AND E.MANDT=A.MANDT 
	AND A.PRCTR BETWEEN E.VALFROM 
	AND E.VALTO 
	AND E.SETNAME IN ('HC_ADMIN' ) 
	AND E.SETCLASS='0106' ) 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=E.MANDT 
	AND SNAM.SETNAME=E.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other expenses' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('ZPCFSHC08' ) ) 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=D.MANDT 
	AND SNAM.SETNAME=D.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 E.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Selling & Distribution Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('ZPCFSHC03',
	 'ZPCFSHC04' ) ) 
INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
	AND E.MANDT=A.MANDT 
	AND A.PRCTR BETWEEN E.VALFROM 
	AND E.VALTO 
	AND E.SETNAME IN ('HC_SND' ) 
	AND E.SETCLASS='0106' ) 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=E.MANDT 
	AND SNAM.SETNAME=E.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('ZPCFS181' ) ) 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=d.MANDT 
	AND SNAM.SETNAME=d.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Cost of Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('ZPCFS164' ) ) /* 
INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
	AND E.MANDT=A.MANDT 
	AND A.PRCTR BETWEEN E.VALFROM 
	AND E.VALTO 
	AND E.SETNAME IN ('ZGLUNI03' ) 
	AND E.SETCLASS='0106' ) */ 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=d.MANDT 
	AND SNAM.SETNAME=d.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Dividend income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('XX' ) ) /* 
INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
	AND E.MANDT=A.MANDT 
	AND A.PRCTR BETWEEN E.VALFROM 
	AND E.VALTO 
	AND E.SETNAME IN ('ZGLUNI03' ) 
	AND E.SETCLASS='0106' ) */ 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=d.MANDT 
	AND SNAM.SETNAME=d.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('ZPCFSHC05') ) /* 
INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
	AND E.MANDT=A.MANDT 
	AND A.PRCTR BETWEEN E.VALFROM 
	AND E.VALTO 
	AND E.SETNAME IN ('ZGLUNI03' ) 
	AND E.SETCLASS='0106' ) */ 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=d.MANDT 
	AND SNAM.SETNAME=d.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Finance cost' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('ZPCFSHC07') ) /* 
INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
	AND E.MANDT=A.MANDT 
	AND A.PRCTR BETWEEN E.VALFROM 
	AND E.VALTO 
	AND E.SETNAME IN ('ZGLUNI03' ) 
	AND E.SETCLASS='0106' ) */ 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=d.MANDT 
	AND SNAM.SETNAME=d.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Income tax expense' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
THEN DMBTR*-1 
ELSE DMBTR 
END Amount 
FROM SAPABAP1.BKPF C 
INNER JOIN SAPABAP1.BSEG A ON (1=1 
	AND C.MANDT=A.MANDT 
	AND C.BUKRS=A.BUKRS 
	AND C.GJAHR=A.GJAHR 
	AND C.BELNR=A.BELNR) 
INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
	AND D.MANDT=A.MANDT 
	AND A.HKONT BETWEEN VALFROM 
	AND VALTO 
	AND D.SETNAME IN ('ZPCFSHC06') ) /* 
INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
	AND E.MANDT=A.MANDT 
	AND A.PRCTR BETWEEN E.VALFROM 
	AND E.VALTO 
	AND E.SETNAME IN ('ZGLUNI03' ) 
	AND E.SETCLASS='0106' ) */ 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND COMP.CLIENT_CODE=A.MANDT 
	AND COMP.COMPANY_CODE=A.BUKRS) 
LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
	AND SNAM.MANDT=d.MANDT 
	AND SNAM.SETNAME=d.SETNAME) 
LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
	AND GL.CLIENT_CODE=A.MANDT 
	AND GL.GL_ACCOUNT=A.HKONT) 
LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
	AND PRFT.CLIENT_CODE=A.MANDT 
	AND PRFT.COMPANY_CODE=A.BUKRS 
	AND PRFT.PROFIT_CENTER=A.PRCTR) 
WHERE 1=1 
AND A.BUKRS='1200' 
AND A.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT));

CREATE VIEW ETL.ZBW_IBLOPS_GL_DATA AS
(SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Administrative Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('0511010105',
'0515010105',
'0507010105',
'0507010206',
'0507030215',
'0508040105',
'0509010105',
'0510010110',
'0510020305',
'0510020310',
'0519060105',
'0505030110',
'0519050205',
'0519050105',
'0525010105',
'0512010101',
'0512010102',
'0512010104',
'0509050105',
'0509050120',
'0515020105',
'0515020125',
'0519050106',
'0515010125',
'0515015120',
'0515020205',
'0515020130',
'0515030110',
'0507020105',
'0519020115',
'0519020120',
'0501090110',
'0508020105',
'0508080105',
'0510030105',
'0509020105',
'0509020205',
'0510030405',
'0508030105',
'0508080110',
'0502010105',
'0502010115',
'0502020105',
'0502040105',
'0502040205',
'0503010105',
'0503020105',
'0503020110',
'0503030105',
'0503050105',
'0503050205',
'0503050305',
'0503050306',
'0503050425',
'0503050605',
'0503050610',
'0503050951',
'0503050955',
'0503050960',
'0504010205',
'0507010205',
'0503050310',
'0508080115',
'0519060115',
'0503040105',
'0505020105',
'0505020310',
'0505020315',
'0513020105',
'0508070105',
'0508070110',
'0508070115',
'0508070120')

		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('ZGLIH05') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other expenses' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('0519080305','0519020105')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('ZGLIH06') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Selling & Distribution Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('XXXX')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('XXXX') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('XXXX')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('XXXX') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Cost of Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('XXXX')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('XXXX') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Dividend income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('0403020125',
'0407010105',
'0407010108',
'0407010109',
'0407010116')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('ZGLIH03') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Corporate Service Income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('0403020130')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('ZGLIH04') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	'Other income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('0404010115','0404010140','0404010305','0403030125')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('ZGLIH07') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	'Finance cost'  ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('0520010536',
'0520010710',
'0520010712',
'0520010713',
'0520010714',
'0519010105',
'0517010205',
'0520010715',
'0520010705')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('ZGLIH08') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Income tax expense'  ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT IN ('0522010105')
		AND A.HKONT BETWEEN VALFROM AND VALTO 
		AND D.SETNAME IN ('ZGLIH09') ) 
	
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='0100' 
	AND A.MANDT IN (SELECT 	 ZMANDT 	FROM ZPBI_MANDT));

CREATE VIEW ETL.ZBW_LUNA_GL_DATA AS
(SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Administrative Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS184',
	 'ZPCFS185',
	 'ZPCFS186',
	 'ZPCFS187',
	 'ZPCFS188',
	 'ZPCFS189',
	 'ZPCFS190',
	 'ZPCFS191',
	 'ZPCFS192',
	 'ZPCFS193',
	 'ZPCFS194',
	 'ZPCFS195',
	 'ZPCFS196',
	 'ZPCFS197',
	 'ZPCFS198',
	 'ZPCFS199',
	 'ZPCFS200',
	 'ZPCFS201',
	 'ZPCFS202',
	 'ZPCFS203',
	 'ZPCFS204',
	 'ZPCFS205',
	 'ZPCFS206',
	 'ZPCFS207',
	 'ZPCFS208',
	 'ZPCFS209',
	 'ZPCFS210',
	 'ZPCFS211',
	 'ZPCFS212',
	 'ZPCFS213',
	 'ZPCFS214',
	 'ZPCFS215',
	 'ZPCFS216',
	 'ZPCFS217',
	 'ZPCFS218',
	 'ZPCFS219') ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
 	AND A.BUKRS='1800' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other expenses' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS168' ) ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
		AND A.BUKRS='1800' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Selling & Distribution Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS184',
	'ZPCFS219' ) ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1800' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS168' ) ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1800'
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Cost of Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS164' ) ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	
	AND A.BUKRS='1800'
		AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Dividend income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('XX' ) ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
		AND A.BUKRS='1800' --and a.hkont in ('0403020125') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS167') ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
		AND A.BUKRS='1800' --and a.hkont in ('0403020110') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Finance cost' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS169') ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
		AND A.BUKRS='1800' ---and a.hkont in ('0405010111') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,	
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Income tax expense' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS170') ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
		AND A.BUKRS='1800' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT));

CREATE VIEW ETL.ZBW_PHA_DATA_ALL AS
SELECT "COMPANYCODE" , "YEAR" , "GL" , "DEBIT_S_CREDIT_H" , "CURRENCY" , "PROFITCENTRE" , "CONTROLLINGAREA" , "COSTCENTRE" , "CUSTOMER" , "VENDOR" , "DOCUMENTTYPE" , "POSTINGDATE" , "SETNAMES" , "REC_COUNT" , "AMT" , "COMPANY_NAME" , "SETNAME_DESC" , "GL_DESC" , "CONTROLLINGAREADESC" FROM 
(
SELECT
	 PHA_DATA_F_DES.COMPANYCODE ,
	 PHA_DATA_F_DES.YEAR ,
	 PHA_DATA_F_DES.GL ,
	 PHA_DATA_F_DES.DEBIT_S_CREDIT_H ,
	 PHA_DATA_F_DES.CURRENCY ,
	 PHA_DATA_F_DES.PROFITCENTRE ,
	 PHA_DATA_F_DES.CONTROLLINGAREA ,
	 PHA_DATA_F_DES.COSTCENTRE ,
	 PHA_DATA_F_DES.CUSTOMER ,
	 PHA_DATA_F_DES.VENDOR ,
	 PHA_DATA_F_DES.DOCUMENTTYPE ,
	 PHA_DATA_F_DES.POSTINGDATE ,
	 PHA_DATA_F_DES.SETNAMES ,
	 PHA_DATA_F_DES.REC_COUNT ,
	 PHA_DATA_F_DES.AMT ,
	 T001.BUTXT AS Company_Name ,
	 CASE WHEN PHA_DATA_F_DES.SETNAMES = 'FACTORYEXP' THEN 'FACTORYEXP' 
ELSE SETHEADERT.DESCRIPT END SETNAME_DESC ,
	 SKAT.MCOD1 AS GL_Desc ,
	 TKA01.BEZEI AS ControllingAreaDesc 
FROM ZBW_PHA_DATA_F_DES PHA_DATA_F_DES
LEFT OUTER JOIN SAPABAP1.TKA01 ON PHA_DATA_F_DES.CONTROLLINGAREA = TKA01.KOKRS 
LEFT OUTER JOIN SAPABAP1.SKAT ON PHA_DATA_F_DES.GL = SKAT.SAKNR 
LEFT OUTER JOIN SAPABAP1.SETHEADERT ON PHA_DATA_F_DES.SETNAMES = SETHEADERT.SETNAME 
LEFT OUTER JOIN SAPABAP1.T001 ON PHA_DATA_F_DES.COMPANYCODE = T001.BUKRS 
WHERE 1=1 
AND (SKAT.MANDT = 300) 
AND T001.MANDT =300 
AND TKA01.MANDT = 300 
AND SETHEADERT.MANDT = 300
);

CREATE VIEW ETL.ZBW_PHA_DATA_F_DES AS
SELECT
	 t.bukrs CompanyCode ,
	 t.gjahr Year ,
	 t.hkont GL ,
	 t.shkzg Debit_S_Credit_H ,
	 t.pswsl Currency ,
	 t.prctr ProfitCentre ,
	 t.kokrs ControllingArea ,
	 t.kostl CostCentre ,
	 t.kunnr Customer ,
	 t.lifnr Vendor ,
	 t.blart DocumentType ,
	 t.budat PostingDate ,
	 t.setname SetNames ,
	 COUNT(T.budat) rec_count ,
	 SUM(T.amt) AMT ,
	 t.setnameN 
FROM ( SELECT
	 A.BUKRS ,
	A.GJAHR ,
	SUM(A.DMBTR) AMT ,
	A.HKONT ,
	A.SHKZG ,
	A.PSWSL ,
	A.PRCTR ,
	A.KOKRS ,
	A.KOSTL ,
	A.KUNNR ,
	A.LIFNR ,
	B.BLART ,
	B.BUDAT ,
	C.SETNAME ,
	C.SETNAME SETNAMEN 
	FROM SAPABAP1.BSEG AS A 
	INNER JOIN SAPABAP1.BKPF AS B ON A.BUKRS = B.BUKRS 
	AND A.BELNR = B.BELNR 
	AND A.GJAHR = B.GJAHR 
	AND A.MANDT = B.MANDT 
	INNER JOIN SAPABAP1.SETLEAF AS C ON (A.HKONT <= C.VALFROM 
		AND A.HKONT >= C.VALTO) 
	AND A.MANDT = C.MANDT 
	WHERE A.BUKRS = 1000 --( a.BUKRS   = 1000 AND a.gjahr   = 2019 )
 
	AND B.BUKRS = 1000 --( b.bukrs   = 1000 AND b.gjahr   = 2019 )
 
	AND C.SETNAME IN ('ZPCFS164' ,
	'ZPCFS166' ,
	'ZPCFS168' ,
	'ZPCFS167' ,
	'ZPCFS169' ,
	'ZPCFS170') 
	AND B.BUDAT < CURRENT_DATE 
	AND A.MANDT = 300 
	AND B.MANDT = 300 
	AND C.MANDT = 300 
	GROUP BY A.BUKRS ,
	A.GJAHR ,
	A.HKONT ,
	A.SHKZG ,
	A.PSWSL ,
	A.PRCTR ,
	A.KOKRS ,
	A.KOSTL ,
	A.KUNNR ,
	A.LIFNR ,
	B.BLART ,
	B.BUDAT ,
	C.SETNAME 
	UNION SELECT
	 A.BUKRS ,
	A.GJAHR ,
	SUM(A.DMBTR) AMT ,
	A.HKONT ,
	A.SHKZG ,
	A.PSWSL ,
	A.PRCTR ,
	A.KOKRS ,
	A.KOSTL ,
	A.KUNNR ,
	A.LIFNR ,
	B.BLART ,
	B.BUDAT ,
	C.SETNAME ,
	C.SETNAME SETNAMEN 
	FROM SAPABAP1.BSEG AS A 
	INNER JOIN SAPABAP1.BKPF AS B ON A.BUKRS = B.BUKRS 
	AND A.BELNR = B.BELNR 
	AND A.GJAHR = B.GJAHR 
	AND A.MANDT = B.MANDT 
	INNER JOIN SAPABAP1.SETLEAF AS C ON (A.HKONT <= C.VALFROM 
		AND A.HKONT >= C.VALTO) 
	AND A.MANDT = C.MANDT 
	WHERE A.BUKRS = 1000 
 
	AND B.BUKRS = 1000 
 
	AND C.SETNAME IN (SELECT
	 SUBSETNAME 
		FROM SAPABAP1.SETNODE 
		WHERE SETNAME IN ('ZPCFS163',
	 'ZPCFS165')) 
	 	AND B.BUDAT < CURRENT_DATE 
	AND A.MANDT = 300 
	AND B.MANDT = 300 
	AND C.MANDT = 300 
	GROUP BY A.BUKRS ,
	A.GJAHR ,
	A.HKONT ,
	A.SHKZG ,
	A.PSWSL ,
	A.PRCTR ,
	A.KOKRS ,
	A.KOSTL ,
	A.KUNNR ,
	A.LIFNR ,
	B.BLART ,
	B.BUDAT ,
	C.SETNAME 
	UNION SELECT
	 A.BUKRS ,
	A.GJAHR ,
	SUM(A.DMBTR) AMT ,
	A.HKONT ,
	A.SHKZG ,
	A.PSWSL ,
	A.PRCTR ,
	A.KOKRS ,
	A.KOSTL ,
	A.KUNNR ,
	A.LIFNR ,
	B.BLART ,
	B.BUDAT ,
	C.SETNAME ,
	D.SETNAME SETNAMEN --c.setname, d.setname, 
 
	FROM SAPABAP1.BSEG AS A 
	INNER JOIN SAPABAP1.BKPF AS B ON A.BUKRS = B.BUKRS 
	AND A.BELNR = B.BELNR 
	AND A.GJAHR = B.GJAHR 
	AND A.MANDT = B.MANDT 
	INNER JOIN SAPABAP1.SETLEAF AS D ON (A.PRCTR <= D.VALFROM 
		AND A.PRCTR >= D.VALTO) 
 
	INNER JOIN SAPABAP1.SETLEAF AS C ON (A.HKONT <= C.VALFROM 
		AND A.HKONT >= C.VALTO) 
 
	WHERE A.BUKRS = 1000 
	AND B.BUKRS = 1000 	AND B.BUDAT < CURRENT_DATE 
	AND C.SETNAME IN (SELECT
	 SUBSETNAME 
		FROM SAPABAP1.SETNODE 
		WHERE SETNAME = 'ZPCFS165') --AND d.setclass = '0106' 
	AND (D.SETNAME = 'FACTORYEXP' 
		AND D.SETCLASS = '0106') --AND a.mandt = 300 AND b.mandt = 300 AND c.mandt = 300
 
	GROUP BY A.BUKRS ,
	A.GJAHR ,
	A.HKONT ,
	A.SHKZG ,
	A.PSWSL ,
	A.PRCTR ,
	A.KOKRS ,
	A.KOSTL ,
	A.KUNNR ,
	A.LIFNR ,
	B.BLART ,
	B.BUDAT ,
	C.SETNAME ,
	D.SETNAME                
 ) t 
 where 1=1 
--  and t.budat>='20200101'
GROUP BY t.bukrs ,
	 t.gjahr ,
	 t.hkont ,
	 t.shkzg ,
	 t.pswsl ,
	 t.prctr ,
	 t.kokrs ,
	 t.kostl ,
	 t.kunnr ,
	 t.lifnr ,
	 t.blart ,
	 t.budat ,
	 t.setname ,
	 t.setnameN;

CREATE VIEW ETL.ZBW_SBS_GL_DATA AS
(SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Administrative Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS184',
	 'ZPCFS185',
	 'ZPCFS186',
	 'ZPCFS187',
	 'ZPCFS188',
	 'ZPCFS189',
	 'ZPCFS190',
	 'ZPCFS191',
	 'ZPCFS192',
	 'ZPCFS193',
	 'ZPCFS194',
	 'ZPCFS195',
	 'ZPCFS196',
	 'ZPCFS197',
	 'ZPCFS198',
	 'ZPCFS199',
	 'ZPCFS200',
	 'ZPCFS201',
	 'ZPCFS202',
	 'ZPCFS203',
	 'ZPCFS204',
	 'ZPCFS205',
	 'ZPCFS206',
	 'ZPCFS207',
	 'ZPCFS208',
	 'ZPCFS209',
	 'ZPCFS210',
	 'ZPCFS211',
	 'ZPCFS212',
	 'ZPCFS213',
	 'ZPCFS214',
	 'ZPCFS215',
	 'ZPCFS216',
	 'ZPCFS217',
	 'ZPCFS218',
	 'ZPCFS219') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('SBSADMIN' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other expenses' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS168' ) ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 E.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Selling & Distribution Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN('ZPCFS184',
	 'ZPCFS185',
	 'ZPCFS186',
	 'ZPCFS187',
	 'ZPCFS188',
	 'ZPCFS189',
	 'ZPCFS190',
	 'ZPCFS191',
	 'ZPCFS192',
	 'ZPCFS193',
	 'ZPCFS194',
	 'ZPCFS195',
	 'ZPCFS196',
	 'ZPCFS197',
	 'ZPCFS198',
	 'ZPCFS199',
	 'ZPCFS200',
	 'ZPCFS201',
	 'ZPCFS202',
	 'ZPCFS203',
	 'ZPCFS204',
	 'ZPCFS205',
	 'ZPCFS206',
	 'ZPCFS207',
	 'ZPCFS208',
	 'ZPCFS209',
	 'ZPCFS210',
	 'ZPCFS211',
	 'ZPCFS212',
	 'ZPCFS213',
	 'ZPCFS214',
	 'ZPCFS215',
	 'ZPCFS216',
	 'ZPCFS217',
	 'ZPCFS218',
	 'ZPCFS219') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('SBSSELDIST' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS181',
	 'ZPCFS182',
	 'ZPCFS183') ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Cost of Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS164' ) ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Dividend income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('XX' ) ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS167') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Finance cost' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS169') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Income tax expense' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS170') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 E.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'FACTORYEXP' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS184',
	 'ZPCFS185',
	 'ZPCFS186',
	 'ZPCFS187',
	 'ZPCFS188',
	 'ZPCFS189',
	 'ZPCFS190',
	 'ZPCFS191',
	 'ZPCFS192',
	 'ZPCFS193',
	 'ZPCFS194',
	 'ZPCFS195',
	 'ZPCFS196',
	 'ZPCFS197',
	 'ZPCFS198',
	 'ZPCFS199',
	 'ZPCFS200',
	 'ZPCFS201',
	 'ZPCFS202',
	 'ZPCFS203',
	 'ZPCFS204',
	 'ZPCFS205',
	 'ZPCFS206',
	 'ZPCFS207',
	 'ZPCFS208',
	 'ZPCFS209',
	 'ZPCFS210',
	 'ZPCFS211',
	 'ZPCFS212',
	 'ZPCFS213',
	 'ZPCFS214',
	 'ZPCFS215',
	 'ZPCFS216',
	 'ZPCFS217',
	 'ZPCFS218',
	 'ZPCFS219') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('FACTORYEXP' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1500' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT));

CREATE VIEW ETL.ZBW_SEARLE_GL_DATA AS
(SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Administrative Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS184',
	 'ZPCFS185',
	 'ZPCFS186',
	 'ZPCFS187',
	 'ZPCFS188',
	 'ZPCFS189',
	 'ZPCFS190',
	 'ZPCFS191',
	 'ZPCFS192',
	 'ZPCFS193',
	 'ZPCFS194',
	 'ZPCFS195',
	 'ZPCFS196',
	 'ZPCFS197',
	 'ZPCFS198',
	 'ZPCFS199',
	 'ZPCFS200',
	 'ZPCFS201',
	 'ZPCFS202',
	 'ZPCFS203',
	 'ZPCFS204',
	 'ZPCFS205',
	 'ZPCFS206',
	 'ZPCFS207',
	 'ZPCFS208',
	 'ZPCFS209',
	 'ZPCFS210',
	 'ZPCFS211',
	 'ZPCFS212',
	 'ZPCFS213',
	 'ZPCFS214',
	 'ZPCFS215',
	 'ZPCFS216',
	 'ZPCFS217',
	 'ZPCFS218',
	 'ZPCFS219') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ADMINEXP' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other expenses' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS168' ) ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 E.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Selling & Distribution Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN('ZPCFS184',
	 'ZPCFS185',
	 'ZPCFS186',
	 'ZPCFS187',
	 'ZPCFS188',
	 'ZPCFS189',
	 'ZPCFS190',
	 'ZPCFS191',
	 'ZPCFS192',
	 'ZPCFS193',
	 'ZPCFS194',
	 'ZPCFS195',
	 'ZPCFS196',
	 'ZPCFS197',
	 'ZPCFS198',
	 'ZPCFS199',
	 'ZPCFS200',
	 'ZPCFS201',
	 'ZPCFS202',
	 'ZPCFS203',
	 'ZPCFS204',
	 'ZPCFS205',
	 'ZPCFS206',
	 'ZPCFS207',
	 'ZPCFS208',
	 'ZPCFS209',
	 'ZPCFS210',
	 'ZPCFS211',
	 'ZPCFS212',
	 'ZPCFS213',
	 'ZPCFS214',
	 'ZPCFS215',
	 'ZPCFS216',
	 'ZPCFS217',
	 'ZPCFS218',
	 'ZPCFS219') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('SELANDDIST' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS181',
	 'ZPCFS182',
	 'ZPCFS183') ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Cost of Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS164' ) ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Dividend income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('XX' ) ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS167') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Finance cost' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS169') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Income tax expense' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS170') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 E.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Cost of Sales'   ACCOUNT_GROUP, --'FACTORYEXP'--- 'Cost of Sales'
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZPCFS184',
	 'ZPCFS185',
	 'ZPCFS186',
	 'ZPCFS187',
	 'ZPCFS188',
	 'ZPCFS189',
	 'ZPCFS190',
	 'ZPCFS191',
	 'ZPCFS192',
	 'ZPCFS193',
	 'ZPCFS194',
	 'ZPCFS195',
	 'ZPCFS196',
	 'ZPCFS197',
	 'ZPCFS198',
	 'ZPCFS199',
	 'ZPCFS200',
	 'ZPCFS201',
	 'ZPCFS202',
	 'ZPCFS203',
	 'ZPCFS204',
	 'ZPCFS205',
	 'ZPCFS206',
	 'ZPCFS207',
	 'ZPCFS208',
	 'ZPCFS209',
	 'ZPCFS210',
	 'ZPCFS211',
	 'ZPCFS212',
	 'ZPCFS213',
	 'ZPCFS214',
	 'ZPCFS215',
	 'ZPCFS216',
	 'ZPCFS217',
	 'ZPCFS218',
	 'ZPCFS219') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('FACTORYEXP' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='1000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT));

CREATE VIEW ETL.ZBW_UDPL_GL_DATA AS
(SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 d.DESCRIPTion SETNAME_DESC ,
	 'Administrative Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN ZPBI_STATEMENT_STRUCTURE D ON (1=1 AND COMPANY='UDPL'
		AND D.CLIENT_CODE=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('184',
'186',
'188',
'189',
'191',
'195',
'196',
'197',
'199',
'200',
'201',
'202',
'203',
'204',
'205',
'206',
'207',
'208',
'209',
'210',
'211',
'213',
'214',
'216',
'217',
'250') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('UDADMIN' ) 
		AND E.SETCLASS='0106' ) 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
/*
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
*/
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='2000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 d.DESCRIPTion SETNAME_DESC ,
	 'Other expenses' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN ZPBI_STATEMENT_STRUCTURE D ON (1=1 AND COMPANY='UDPL'
		AND D.CLIENT_CODE=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('168') ) 	
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
/*
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
*/
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='2000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 d.DESCRIPTion SETNAME_DESC ,
	 'Selling & Distribution Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN ZPBI_STATEMENT_STRUCTURE D ON (1=1 AND COMPANY='UDPL'
		AND D.CLIENT_CODE=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('184',
'186',
'188',
'189',
'191',
'195',
'196',
'197',
'199',
'200',
'201',
'202',
'203',
'204',
'205',
'206',
'207',
'208',
'209',
'210',
'211',
'213',
'214',
'216',
'217',
'250') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('UDSELDIST' ) 
		AND E.SETCLASS='0106' ) 
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
/*
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
*/
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='2000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 d.DESCRIPTion SETNAME_DESC ,
	 'Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN ZPBI_STATEMENT_STRUCTURE D ON (1=1 AND COMPANY='UDPL'
		AND D.CLIENT_CODE=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('181') ) 	
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
/*
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
*/
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='2000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 d.DESCRIPTion SETNAME_DESC ,
	 'Cost of Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN ZPBI_STATEMENT_STRUCTURE D ON (1=1 AND COMPANY='UDPL'
		AND D.CLIENT_CODE=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('164') ) 	
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
/*
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
*/
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='2000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 d.DESCRIPTion SETNAME_DESC ,
	 'Other income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN ZPBI_STATEMENT_STRUCTURE D ON (1=1 AND COMPANY='UDPL'
		AND D.CLIENT_CODE=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('167') ) 	
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
/*
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
*/
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='2000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 d.DESCRIPTion SETNAME_DESC ,
	 'Finance cost' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN ZPBI_STATEMENT_STRUCTURE D ON (1=1 AND COMPANY='UDPL'
		AND D.CLIENT_CODE=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('169') ) 	
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
/*
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
*/
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='2000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 d.DESCRIPTion SETNAME_DESC ,
	 'Income tax expense' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN ZPBI_STATEMENT_STRUCTURE D ON (1=1 AND COMPANY='UDPL'
		AND D.CLIENT_CODE=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 		AND VALTO 
		AND D.SETNAME IN ('171') ) 	
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
/*
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
*/
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='2000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT));

CREATE VIEW ETL.ZBW_UNISYS_GL_DATA AS
(SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 e.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Administrative Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI05' ) ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ADMINUNI' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 e.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other expenses' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI06' ) ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('OTHEREXUNI' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 e.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Selling & Distribution Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI05' ) ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('SDUNISYS' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI01' ) ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	and a.hkont in ('0401020105',
	'0401060105',
	'0401060110') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Cost of Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI02' ) ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	and a.hkont in ('0501020150',
	'0501020165',
	'0501050125',
	'0501080130',
	'0501080155',
	'0501080160',
	'0501080415',
	 '0501090105',
	'0501090110',
	'0514010105',
	'0519080205',
	'0519080206') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Dividend income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI03' ) ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	and a.hkont in ('0403020125') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI07') ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	and a.hkont in ('0403020110',
	 '0404010120',
	 '0404010125',
	 '0404010130',
	 '0404010135',
	 '0404010145',
	 '0404010150',
	 '0406010120',
	 '0406010130',
	 '0406010140',
	 '0406010145') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Finance cost' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI08') ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	and a.hkont in ('0405010111',
	 '0510040105',
	 '0510040110',
	 '0510040115',
	 '0517010105',
	 '0517010110',
	 '0517010205',
	 '0519010105',
	 '0520010140',
	 '0520010147',
	 '0520010536',
	 '0520010705') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Income tax expense' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUNI09') ) /*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='7000' 
	and a.hkont in ('0515015105',
	 '0515015110',
	 '0522010105',
	 '0522020105',
	 '0522020110',
	 '0522030105') 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT));

CREATE VIEW ETL.ZBW_URHABITT_GL_DATA AS
(SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Administrative Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('xxxx') ) 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('XXX' ) 
		AND E.SETCLASS='0106' ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=E.MANDT 
		AND SNAM.SETNAME=E.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='XXXX' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other expenses' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('XXXX' ) ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='XXXX' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 D.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Selling & Distribution Exp' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN('ZGLUR03') ) 
		/*
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('SELANDDIST' ) 
		AND E.SETCLASS='0106' ) 
*/	
inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=D.MANDT 
		AND SNAM.SETNAME=D.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='3100' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUR01') ) 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='3100' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Cost of Sales' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUR02' ) ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='3100' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Dividend income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('XX' ) ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='XXXX' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 --d.SETNAME
 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Other income' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUR04') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='3100' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Finance cost' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUR05') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='3100' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT)) UNION ALL (SELECT
	 A.MANDT CLIENT_CODE,
	 C.BUDAT TRANSACTION_DATE,
	 AUGDT Clearing_Date,
	 A.GJAHR FISCAL_YEAR,
	 A.BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC ,
	 d.setname ,
	 SNAM.DESCRIPT SETNAME_DESC ,
	 'Income tax expense' ACCOUNT_GROUP,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER,
	 BUZEI LINE_NUMBER,
	 BUZID LINE_IDENTIFICATION,
	 AUGBL DOCUMENT_NUMBER,
	 KOART,
	 SHKZG Debit_Credit ,
	 GSBER Business_Area,
	 A.HKONT GL_ACCOUNT,
	 GL.SHORT_TEXT,
	 GL.LONG_TEXT ,
	 A.PRCTR PROFIT_CENTER ,
	 PRFT.PROFIT_CENTER_DESC,
	 case when A.SHKZG='H' 
	THEN DMBTR*-1 
	ELSE DMBTR 
	END Amount 
	FROM SAPABAP1.BKPF C 
	INNER JOIN SAPABAP1.BSEG A ON (1=1 
		AND C.MANDT=A.MANDT 
		AND C.BUKRS=A.BUKRS 
		AND C.GJAHR=A.GJAHR 
		AND C.BELNR=A.BELNR) 
	INNER JOIN SAPABAP1.SETLEAF D ON (1=1 
		AND D.MANDT=A.MANDT 
		AND A.HKONT BETWEEN VALFROM 
		AND VALTO 
		AND D.SETNAME IN ('ZGLUR06') ) /* 
	INNER JOIN SAPABAP1.SETLEAF E ON (1=1 
		AND E.MANDT=A.MANDT 
		AND A.PRCTR BETWEEN E.VALFROM 
		AND E.VALTO 
		AND E.SETNAME IN ('ZGLUNI03' ) 
		AND E.SETCLASS='0106' ) */ 
	inner JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=A.MANDT 
		AND COMP.COMPANY_CODE=A.BUKRS) 
	LEFT OUTER JOIN sapabap1.SETHEADERT SNAM ON (1=1 
		AND SNAM.MANDT=d.MANDT 
		AND SNAM.SETNAME=d.SETNAME) 
	LEFT OUTER JOIN ZPBI_GL_ACCOUNT_DESC GL ON (1=1 
		AND GL.CLIENT_CODE=A.MANDT 
		AND GL.GL_ACCOUNT=A.HKONT) 
	LEFT OUTER JOIN ZPBI_PROFIT_CENTER PRFT ON (1=1 
		AND PRFT.CLIENT_CODE=A.MANDT 
		AND PRFT.COMPANY_CODE=A.BUKRS 
		AND PRFT.PROFIT_CENTER=A.PRCTR) 
	WHERE 1=1 
	AND A.BUKRS='3100' 
	AND A.MANDT IN (SELECT
	 ZMANDT 
		FROM ZPBI_MANDT));

CREATE VIEW ETL.ZBW_VW_ACTUALVSBUDGETFINAL AS
SELECT
	 record_type ,
	 year ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 NULL LTEXT ,
	 MONTH_TOTAL,
	 MONTH_,
	 CASE WHEN MONTH_ = 'HSL01' 
THEN CONCAT(YEAR - 1,
	 '-07-01') WHEN MONTH_ = 'HSL02' 
THEN CONCAT(YEAR - 1,
	 '-08-01') WHEN MONTH_ = 'HSL03' 
THEN CONCAT(YEAR - 1,
	 '-09-01') WHEN MONTH_ = 'HSL04' 
THEN CONCAT(YEAR - 1,
	 '-10-01') WHEN MONTH_ = 'HSL05' 
THEN CONCAT(YEAR - 1,
	 '-11-01') WHEN MONTH_ = 'HSL06' 
THEN CONCAT(YEAR - 1,
	 '-12-01') /*Year changed*/ WHEN MONTH_ = 'HSL07' 
THEN CONCAT(YEAR,
	 '-01-01') WHEN MONTH_ = 'HSL08' 
THEN CONCAT(YEAR,
	 '-02-01') WHEN MONTH_ = 'HSL09' 
THEN CONCAT(YEAR,
	 '-03-01') WHEN MONTH_ = 'HSL10' 
THEN CONCAT(YEAR,
	 '-04-01') WHEN MONTH_ = 'HSL11' 
THEN CONCAT(YEAR,
	 '-05-01') WHEN MONTH_ = 'HSL12' 
THEN CONCAT(YEAR,
	 '-06-01') 
ELSE MONTH_ 
END MONTH_DT,
	 AMT 
FROM ( SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL01' MONTH_,
	 HSL01 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL02' MONTH_,
	 HSL02 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL03' MONTH_,
	 HSL03 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL04' MONTH_,
	 HSL04 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL05' MONTH_,
	 HSL05 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL06' MONTH_,
	 HSL06 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL07' MONTH_,
	 HSL07 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL08' MONTH_,
	 HSL08 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL09' MONTH_,
	 HSL09 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL10' MONTH_,
	 HSL10 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL11' MONTH_,
	 HSL11 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
	UNION ALL SELECT
	 record_type ,
	 ryear year ,
	 'PKR' CURRENCY ,
	 '0L' GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 000 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 A.PRCTR ,
	 B.KTEXT PRCTR_DESC ,
	 NULL LTEXT ,
	 HSL01 + HSL02 + HSL03 + HSL04 + HSL05 + HSL06 + HSL07 + HSL08 + HSL09 + HSL10 + HSL11 + HSL12 MONTH_TOTAL,
	 'HSL12' MONTH_,
	 HSL12 AMT 
	FROM ZBW_actualvsbudgetfinal A 
	LEFT OUTER JOIN SAPABAP1.CEPCT B ON B.PRCTR = A.PRCTR 
	WHERE 1=1 
	--AND RYEAR=2020 
	--AND GL_ACCOUNT=0401010105 
	--AND KTEXT='KETRESS (PAK-PDF)' 
);

CREATE VIEW ETL.ZBW_VW_ACTUALVSBUDGETFINAL_N AS
SELECT
	 CASE WHEN RRCTY = 1 
THEN 'Budget' 
ELSE 'Actual' 
END AS RRCTY ,
	 RECORD_TYPE ,
	 GL_ACCOUNT ,
	 PRCTR ,
	 MONTH_DT ,
	 AMT / 1000 AS AMT 
FROM ZBW_vw_ActualVsBudgetFinal;

CREATE VIEW ETL.ZBW_VW_BUDGETSEARLE_MONTHS AS
SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 GL_ACCOUNT_DESC,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 MONTH_,
	 CASE WHEN MONTH_ = 'HSL01' 
THEN CONCAT(YEAR - 1,
	 '-07-01') WHEN MONTH_ = 'HSL02' 
THEN CONCAT(YEAR - 1,
	 '-08-01') WHEN MONTH_ = 'HSL03' 
THEN CONCAT(YEAR - 1,
	 '-09-01') WHEN MONTH_ = 'HSL04' 
THEN CONCAT(YEAR - 1,
	 '-10-01') WHEN MONTH_ = 'HSL05' 
THEN CONCAT(YEAR - 1,
	 '-11-01') WHEN MONTH_ = 'HSL06' 
THEN CONCAT(YEAR - 1,
	 '-12-01') /*Year changed*/ WHEN MONTH_ = 'HSL07' 
THEN CONCAT(YEAR,
	 '-01-01') WHEN MONTH_ = 'HSL08' 
THEN CONCAT(YEAR,
	 '-02-01') WHEN MONTH_ = 'HSL09' 
THEN CONCAT(YEAR,
	 '-03-01') WHEN MONTH_ = 'HSL10' 
THEN CONCAT(YEAR,
	 '-04-01') WHEN MONTH_ = 'HSL11' 
THEN CONCAT(YEAR,
	 '-05-01') WHEN MONTH_ = 'HSL12' 
THEN CONCAT(YEAR,
	 '-06-01') 
ELSE MONTH_ 
END MONTH_DT,
	 AMT 
FROM ( SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL01' MONTH_,
	 HSL01/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL02' MONTH_,
	 HSL02/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	---AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL03' MONTH_,
	 HSL03/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL04' MONTH_,
	 HSL04/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL05' MONTH_,
	 HSL05/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL06' MONTH_,
	 HSL06/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL07' MONTH_,
	 HSL07/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL08' MONTH_,
	 HSL08/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL09' MONTH_,
	 HSL09/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL10' MONTH_,
	 HSL10/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL11' MONTH_,
	 HSL11/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
	UNION ALL SELECT
	 YEAR ,
	 CURRENCY ,
	 GENERAL_LEDGER_ACCOUNTING ,
	 RRCTY ,
	 RVERS ,
	 GL_ACCOUNT ,
	 CASE WHEN GL_ACCOUNT = '0401020105' 
	THEN 'Sales' WHEN GL_ACCOUNT = '0501020150' 
	THEN 'Cost of sales' WHEN GL_ACCOUNT = '0502010105' 
	THEN 'Operating expenses' WHEN GL_ACCOUNT = '0519080305' 
	THEN 'Other expenses' WHEN GL_ACCOUNT = '0403020110' 
	THEN 'Other income' WHEN GL_ACCOUNT = '0508010105' 
	THEN 'Depreciation & Amortization' WHEN GL_ACCOUNT = '0519010105' 
	THEN 'Finance cost' WHEN GL_ACCOUNT = '0522010105' 
	THEN 'Income tax expense' 
	ELSE GL_ACCOUNT 
	END GL_ACCOUNT_DESC ,
	 PRCTR ,
	 PRCTR_DESC ,
	 LTEXT ,
	 COST_ELEMENT ,
	 DRCRK ,
	 MONTH_TOTAL ,
	 'HSL12' MONTH_,
	 HSL12/1000 AMT 
	FROM ZBW_BUDGETSEARLE 
	WHERE 1 = 1 
	--AND GL_ACCOUNT=0401020105 
	--AND DRCRK='S' 
);

CREATE VIEW ETL.ZBW_VW_PHARMADATA AS
SELECT
	 PHA_DATA_ALL.COMPANYCODE ,
	 PHA_DATA_ALL.YEAR ,
	 PHA_DATA_ALL.GL ,
	 PHA_DATA_ALL.DEBIT_S_CREDIT_H ,
	 PHA_DATA_ALL.CURRENCY ,
	 PHA_DATA_ALL.PROFITCENTRE ,
	 PHA_DATA_ALL.CONTROLLINGAREA ,
	 PHA_DATA_ALL.COSTCENTRE ,
	 PHA_DATA_ALL.CUSTOMER ,
	 PHA_DATA_ALL.VENDOR ,
	 PHA_DATA_ALL.DOCUMENTTYPE ,
	 PHA_DATA_ALL.POSTINGDATE ,
	 PHA_DATA_ALL.SETNAMES ,
	 PHA_DATA_ALL.REC_COUNT ,
	 PHA_DATA_ALL.AMT AMT_OLD ,
	 PHA_DATA_ALL.COMPANY_NAME ,
	 PHA_DATA_ALL.SETNAME_DESC ,
	 PHA_DATA_ALL.GL_DESC ,
	 PHA_DATA_ALL.CONTROLLINGAREADESC ,
	 CASE WHEN SETNODE.SETNAME IS NULL 
THEN PHA_DATA_ALL.SETNAMES 
ELSE SETNODE.SETNAME 
END AS GROUP_SETNAMES ,
	 CASE WHEN PHA_DATA_ALL.SETNAME_DESC = 'FACTORYEXP' 
THEN 'FACTORYEXP' WHEN SETHEADERT.DESCRIPT IS NULL 
THEN PHA_DATA_ALL.SETNAME_DESC 
ELSE SETHEADERT.DESCRIPT 
END AS GROUP_SETNAME_DESC ,
	 CASE WHEN DEBIT_S_CREDIT_H = 'S' 
THEN -AMT 
ELSE AMT 
END AS AMT ,
	 PHA_DATA_ALL.POSTINGDATE POSTING_DATE_DT 
FROM SAPABAP1.SETHEADERT 
INNER JOIN SAPABAP1.SETNODE ON SETHEADERT.SETNAME = SETNODE.SETNAME 
RIGHT OUTER JOIN ZBW_PHA_DATA_ALL PHA_DATA_ALL ON SETNODE.SUBSETNAME = PHA_DATA_ALL.SETNAMES;

CREATE VIEW ETL.ZBW_VW_PHARMADATAFINAL AS
SELECT
	 COMPANYCODE ,
	 YEAR ,
	 DEBIT_S_CREDIT_H ,
	 CURRENCY ,
	 PROFITCENTRE ,
	 COSTCENTRE ,
	 SETNAMES ,
	 Company_Name ,
	 SETNAME_DESC ,
	 GROUP_SETNAMES ,
	 GROUP_SETNAME_DESC ,
	 SUM(AMT / 1000) AS AMT ,
	 POSTING_DATE_DT ,
	 Z_GET_FIRST_DATE(POSTING_DATE_DT) MonthDT 
FROM ZBW_vw_PharmaData 
WHERE 1=1 
--AND POSTING_DATE_DT ='20200110' 
GROUP BY COMPANYCODE ,
	 YEAR ,
	 DEBIT_S_CREDIT_H ,
	 CURRENCY ,
	 PROFITCENTRE ,
	 COSTCENTRE ,
	 SETNAMES ,
	 Company_Name ,
	 SETNAME_DESC ,
	 GROUP_SETNAMES ,
	 GROUP_SETNAME_DESC ,
	 POSTING_DATE_DT ,
	 Z_GET_FIRST_DATE(POSTING_DATE_DT);

CREATE VIEW ETL.ZPBI_ACCOUNTING_DOC_TYPE AS
SELECT
	 "MANDT" ,
	 "SPRAS" ,
	 "BLART" ,
	 "LTEXT" 
FROM SAPABAP1.T003T 
WHERE 1=1 
AND MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT) 
AND SPRAS='E';

CREATE VIEW ETL.ZPBI_ALL_GL_SET AS
SELECT "CLIENT_CODE" , "SETCLASS" , "SUBCLASS" , "COMPANY" , "SETNAME" , "DESCRIPTION" , "VALFROM" , "VALTO" FROM
 (
 SELECT
	 SLF.MANDT CLIENT_CODE,
	 SLF.SETCLASS,
	 SLF.SUBCLASS,
	 ''COMPANY,
	 SLF.SETNAME,
	 SDC.DESCRIPT DESCRIPTION,
	 SLF.VALFROM,
	 SLF.VALTO 
FROM SAPABAP1.SETLEAF SLF 
LEFT OUTER JOIN SAPABAP1.SETHEADERT SDC ON (1=1 
	AND SDC.MANDT=SLF.MANDT 
	AND SDC.LANGU='E' 
	AND SDC.SETNAME=SLF.SETNAME ) 
WHERE 1=1 
AND SLF.MANDT in (select
	 zmandt 
	FROM ZPBI_MANDT) 
UNION ALL select
	 a.mandt client_code,
	''setclass,
	''subclass,
	A.VERSN COMPANY,
	A.ERGSL SETNAME,
	 B.TXT45 DESCRIPTION,
	 a.vonkt valfrom,
	a.biskt valto 
from sapabap1.FAGL_011ZC A 
LEFT OUTER JOIN sapabap1.FAGL_011QT B ON (1=1 
	AND B.MANDT=A.MANDT 
	AND B.SPRAS='E' 
	AND B.VERSN=A.VERSN 
	AND B.ERGSL=A.ERGSL 
	AND B.VERSN=A.VERSN) 
where 1=1 
and A.mandt IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT) 
AND A.VERSN='UDPL' 
)
WHERE 1=1;

CREATE VIEW ETL.ZPBI_BATCHES AS
SELECT "CLIENT_CODE" , "ITEM_CODE" , "ITEM_DESC" , "PLANT" , "PLANT_DESC" , "BATCH_NUMBER" , "VENDOR_BATCH" , "BATCH_EXP_DATE" , "BATCH_TYPE" FROM (
	 select
	 BATCH.MANDT CLIENT_CODE,
	 BATCH.MATNR ITEM_CODE,
	 ITEM.MAKTX ITEM_DESC,
	 BATCH.WERKS PLANT,
	 PLANT.PLANT_DESC,
	 BATCH.CHARG BATCH_NUMBER ,BATCH.BWTAR VENDOR_BATCH,
	 LOTEXP.VFDAT BATCH_EXP_DATE,
	 BATCH.BATCH_TYPE 
from SAPABAP1.MCHA BATCH ,
	 SAPABAP1.MAKT ITEM,
	 ZPBI_PLANTS PLANT ,
	 SAPABAP1.MCH1 LOTEXP 
WHERE 1=1 
AND BATCH.MANDT=300 
AND ITEM.MANDT=BATCH.MANDT 
AND ITEM.MATNR=BATCH.MATNR 
AND PLANT.CLIENT_CODE=BATCH.MANDT 
AND PLANT.PLANT=BATCH.WERKS 
AND LOTEXP.MANDT=BATCH.MANDT 
AND LOTEXP.MATNR=BATCH.MATNR 
AND LOTEXP.CHARG=BATCH.CHARG
)
WHERE 1=1;

CREATE VIEW ETL.ZPBI_BILLING_TYPE AS
SELECT
	 MANDT ,
	 SPRAS ,
	 FKART ,
	 VTEXT 
FROM SAPABAP1.TVFKT 
WHERE 1=1 
AND MANDT=300
AND SPRAS='E';

CREATE VIEW ETL.ZPBI_BRANCHES AS
SELECT
	 BRANCH.MANDT CLIENT_CODE,
	CLIENT.MTEXT CLIENT_DESC,
	 VKBUR BRANCH,
	BEZEI BRANCH_DESC 
FROM SAPABAP1.TVKBT BRANCH ,
	SAPABAP1.T000 CLIENT 
where 1=1 
AND BRANCH.MANDT=300 
and spras='E' 
AND CLIENT.MANDT=BRANCH.MANDT;

CREATE VIEW ETL.ZPBI_BUSLINE AS
select
	 MANDT CLIENT_CODE,
	 MVGR1 BUSLINE_CODE,
	 BEZEI BUSLINE_DESC 
from SAPABAP1.TVM1T 
WHERE 1=1 
AND MANDT=300 
AND SPRAS='E';

CREATE VIEW ETL.ZPBI_COMPANIES AS
SELECT
	 COMP.MANDT CLIENT_CODE,
	 CLIENT.MTEXT CLIENT_DESC,
	 BUKRS COMPANY_CODE,
	 BUTXT COMPANY_DESC 
FROM SAPABAP1.T001 COMP,
	 SAPABAP1.T000 CLIENT 
WHERE 1=1 
AND COMP.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT) 
AND CLIENT.MANDT=COMP.MANDT;

CREATE VIEW ETL.ZPBI_CUSTOMER_SALES_AREA AS
SELECT
	 "CLIENT_CODE" ,
	 "CLIENT_DESC" ,
	 "CUSTOMER_NUMBER" ,
	 "CUSTOMER_NAME" ,
	 "SALES_ORG" ,
	 "CHANNEL" ,
	 "DIV" ,
	 "BRANCH" ,
	 "BRANCH_DESC" 
FROM ( SELECT
	 --Not finalized yet
 C.MANDT CLIENT_CODE,
	 COMP.MTEXT CLIENT_DESC,
	 C.KUNNR CUSTOMER_NUMBER,
	 CUST.NAME1 CUSTOMER_NAME,
	 VKORG SALES_ORG ,
	 VTWEG CHANNEL,
	 SPART DIV ,
	 C.VKBUR BRANCH ,
	 BR.BRANCH_DESC 
	FROM SAPABAP1.KNVV C ,
	 SAPABAP1.T000 COMP ,
	 ZPBI_CUSTOMERS CUST ,
	 ZPBI_BRANCHES BR 
	WHERE 1=1 
	AND COMP.MANDT=C.MANDT 
	AND CUST.CUSTOMER_NUMBER=C.KUNNR 
	AND BR.BRANCH=C.VKBUR 
	AND C.MANDT=550 
	AND C.KUNNR=1000 ) 
WHERE 1=1 
AND BRANCH='1500';

CREATE VIEW ETL.ZPBI_CUSTOMERS AS
SELECT
	 CLIENT_CODE ,
	 CUSTOMER_NUMBER ,
	 NAME1 ,
	 NAME2 ,
	 COUNTRY_KEY ,
	 COUNTRY_DESC ,
	 CITY ,
	 PHONE ,
	 ADDRESS_NUMBER ,
	 STREET ,
	 ADD1 ,
	 ADD2 ,
	 ADD3 ,
	 LOCATION ,
	 CNIC ,
	 NTN ,
	 GST_REG_NUMBER 
FROM (select
	 CUST.MANDT CLIENT_CODE,
	 CUST.KUNNR CUSTOMER_NUMBER,
	 CUST.NAME1,
	 CUST.NAME2,
	 CUST.LAND1 COUNTRY_KEY ,
	 COUNTRY.NATIO COUNTRY_DESC ,
	 CUST.ORT01 CITY ,
	 CUST.TELF1 PHONE,
	 CUST.ADRNR Address_NUMBER,
	 ADRC.STREET,
	 ADRC.STR_SUPPL1 ADD1,
	 ADRC.STR_SUPPL2 ADD2,
	 ADRC.STR_SUPPL3 ADD3,
	 ADRC.LOCATION ,
	 CUST.STCD1 CNIC,
	 CUST.STCD2 NTN ,
	 CUST.STCEG GST_REG_Number 
	from SAPABAP1.KNA1 CUST 
	LEFT OUTER JOIN SAPABAP1.T005T COUNTRY ON (1=1 
		AND COUNTRY.SPRAS='E' 
		AND COUNTRY.MANDT=CUST.MANDT 
		AND COUNTRY.LAND1=CUST.LAND1) 
	LEFT OUTER JOIN SAPABAP1.ADRC ON (1=1 
		AND ADRC.CLIENT=CUST.MANDT 
		AND ADRC.ADDRNUMBER=CUST.ADRNR ) 
	WHERE 1=1 
	AND CUST.MANDT=300 --ORDER BY COUNTRY.NATIO 
) 
WHERE 1=1;

CREATE VIEW ETL.ZPBI_GL_ACCOUNT_DESC AS
SELECT MANDT CLIENT_CODE,KTOPL CHART_OF_ACCOUNT,SAKNR GL_ACCOUNT,TXT20 SHORT_TEXT,TXT50 LONG_TEXT
 FROM SAPABAP1.SKAT
 WHERE 1=1 
 AND MANDT IN (SELECT ZMANDT FROM ZPBI_MANDT);

CREATE VIEW ETL.ZPBI_GROUP_CONSOLIDATED AS
SELECT
	 CLIENT_CODE,
	 TRANSACTION_DATE,
	 CLEARING_DATE,
	 FISCAL_YEAR,
	 COMPANY_CODE,
	 COMPANY_DESC ,
	 SETNAME,
	 SETNAME_DESC ,
	 ACCOUNT_GROUP ,
	 ACCOUNTING_DOCUMENT_NUMBER,
	 LINE_NUMBER,
	 LINE_IDENTIFICATION,
	 DOCUMENT_NUMBER,
	 KOART,
	 DEBIT_CREDIT,
	 BUSINESS_AREA,
	 GL_ACCOUNT,
	 SHORT_TEXT ,
	 LONG_TEXT ,
	 PROFIT_CENTER,
	 CASE WHEN COMPANY_CODE IN ('1800',
	 '1200',
	 '1000') 
THEN '01' 
else 99
END BUSINESS_GROUP_CODE,
	 CASE WHEN COMPANY_CODE IN ('1800',
	 '1200',
	 '1000') 
THEN 'Pharmaceuticals and Healthcare' 
else 'N/A' 
END BUSINESS_GROUP,
	 CASE WHEN ACCOUNT_GROUP IN ('Sales',
	 'Cost of Sales') 
THEN '01' WHEN ACCOUNT_GROUP IN ('Dividend income',
	 'Corporate Service Income  ',
	 'Administrative Exp        ',
	 'Selling & Distribution Exp',
	 'Other expenses            ',
	 'Other income              ' ) 
THEN '03' WHEN ACCOUNT_GROUP IN ('Finance cost') 
THEN '05' WHEN ACCOUNT_GROUP IN ('Income tax expense') 
THEN '07' 
else '99' 
END DATA_GROUP_CODE,
	 IFNULL( AMOUNT ,
	0)AMOUNT 
FROM ( SELECT
	 * 
	FROM ZBW_UNISYS_GL_DATA 
	UNION ALL SELECT
	 * 
	FROM ZBW_IBLHEALTHCARE_GL_DATA 
	UNION ALL SELECT
	 * 
	FROM ZBW_LUNA_GL_DATA 
	UNION ALL SELECT
	 * 
	FROM ZBW_SEARLE_GL_DATA 
	UNION ALL SELECT
	 * 
	FROM ZBW_URHABITT_GL_DATA 
	UNION ALL SELECT
	 * 
	FROM ZBW_UDPL_GL_DATA 
	UNION ALL SELECT
	 * 
	FROM ZBW_IBLOPS_GL_DATA 
	UNION ALL SELECT
	 * 
	FROM ZBW_SBS_GL_DATA )DATA 
WHERE 1=1;

CREATE VIEW ETL.ZPBI_GROUP_LEVEL_SUM_DATA AS
SELECT "TRANSACTION_DATE" , "BUSINESS_GROUP_CODE" , "BUSINESS_GROUP" , "COMPANY_CODE" , "COMPANY_DESC" , "DATA_GROUP_CODE" , "ACCOUNT_GROUP" , "SUM(AMOUNT)" from 
(
select
	to_char( Z_GET_FIRST_DATE(TRANSACTION_DATE),'YYYYMMDD')TRANSACTION_DATE,
	 BUSINESS_GROUP_CODE,
	 BUSINESS_GROUP,
	 COMPANY_CODE,
	 COMPANY_DESC,
	 DATA_GROUP_CODE,
	 account_group,
	 sum(amount) --*
 
from ZPBI_GROUP_CONSOLIDATED 
where 1=1 
--and transaction_date between '20200101' and '20200131' 
--and company_code='1000' --and account_group IN ('Cost of Sales','FACTORYEXP') --='FACTORYEXP' 
 
group by Z_GET_FIRST_DATE(TRANSACTION_DATE),
	 BUSINESS_GROUP_CODE,
	 BUSINESS_GROUP,
	 COMPANY_CODE,
	 COMPANY_DESC,
	 DATA_GROUP_CODE,
	 account_group 
/*ORDER BY Z_GET_FIRST_DATE(TRANSACTION_DATE),
	 BUSINESS_GROUP_CODE,
	 DATA_GROUP_CODE,
	 COMPANY_DESC 
*/
union all
select
	 to_char( Z_GET_FIRST_DATE(TRANSACTION_DATE),'YYYYMMDD')TRANSACTION_DATE,
	 BUSINESS_GROUP_CODE,
	 BUSINESS_GROUP,
	 COMPANY_CODE,
	 COMPANY_DESC,
	case when b.account_mapping='Gross Profit' then 02 
	when b.account_mapping='Operating profit' then 04
	when b.account_mapping='Profit before taxation' then 06
	when b.account_mapping='Profit after taxation' then 08
	
		
	else DATA_GROUP_CODE end DATA_GROUP_CODE,
	-- a.account_group,
	 b.account_mapping,
	 sum(amount) --*
 
from ZPBI_GROUP_CONSOLIDATED a,
	 ZPBI_MAPPING_TABLE b 
where 1=1 
and b.account_group=a.account_group 
--and transaction_date between '20200101' and '20200131' 
--and company_code='1000' --and account_group IN ('Cost of Sales','FACTORYEXP') --='FACTORYEXP' 
 
group by Z_GET_FIRST_DATE(TRANSACTION_DATE),
	 BUSINESS_GROUP_CODE,
	  DATA_GROUP_CODE,
	 BUSINESS_GROUP,
	 COMPANY_CODE,
	 COMPANY_DESC,
	
	-- a.account_group ,
	b.account_mapping 
	);

CREATE VIEW ETL.ZPBI_INV_ONHAND_STOCK AS
SELECT
	 A.MANDT CLIENT_CODE ,
	 B.MTEXT CLIENT_DESC ,
	 A.MATNR ITEM_CODE ,
	 C.MAKTX ITEM_DESC ,
	 BUSLINE.BUSLINE_CODE ,
	 BUSLINE.BUSLINE_DESC ,
	 A.WERKS ,
	 A.LGORT SUBINVENTORY_CODE ,
	 E.LGOBE SUBINVENTORY_DESC ,
	 A.CHARG BATCH_NUMBER ,
	 LOTEXP.BATCH_EXP_DATE ,
	 D.BWKEY PLANT ,
	 D.NAME1 PLANT_DESC ,
	 G.BUKRS COMPANY_CODE ,
	 COMP.BUTXT COMPANY_DESC ,
	 A.CLABS QTY ,
	 IFNULL(COST.ITEM_COST,0)ITEM_COST,
	 IFNULL(TP.KBETR,
	 0) TRADE_PRICE ,
	 A.ERSDA CREATED_DATE ,
	 A.ERNAM NAME_OF_PERSON ,
	 A.LAEDA LAST_CHANGE ,
	 A.AENAM WHO_CHANGED_OBJECT ,
	 A.LFGJA FISCAL_YEAR 
FROM SAPABAP1.MCHB A 
LEFT OUTER JOIN ZPBI_ITEM_COST COST ON(COST.CLIENT_CODE=A.MANDT 
	AND COST.ITEM_CODE=A.MATNR 
	AND COST.PLANT=A.WERKS 
	AND COST.BATCH=A.CHARG) 
left outer jOIN SAPABAP1.T000 B ON (1=1 
	AND B.MANDT = A.MANDT ) 
LEFT OUTER JOIN SAPABAP1.MAKT C ON (1=1 
	AND C.MANDT = A.MANDT 
	AND C.MATNR = A.MATNR ) 
LEFT OUTER JOIN SAPABAP1.T001W D ON (1=1 
	AND D.MANDT = A.MANDT 
	AND D.WERKS = A.WERKS ) 
LEFT OUTER JOIN SAPABAP1.T001L E ON (1=1 
	AND E.MANDT = A.MANDT 
	AND E.WERKS = A.WERKS 
	AND E.LGORT = A.LGORT ) 
LEFT OUTER JOIN SAPABAP1.MARD F ON (1=1 
	AND F.MANDT = A.MANDT 
	AND F.MATNR = A.MATNR 
	AND F.WERKS = A.WERKS 
	AND F.LGORT = A.LGORT ) 
LEFT OUTER JOIN SAPABAP1.T001K G ON (1=1 
	AND G.MANDT = D.MANDT 
	AND G.BWKEY = D.BWKEY ) 
LEFT OUTER JOIN SAPABAP1.T001 COMP ON (1=1 
	AND COMP.MANDT = G.MANDT 
	AND COMP.BUKRS = G.BUKRS 
	AND COMP.LAND1='PK' ) 
LEFT OUTER JOIN ZPBI_BATCHES LOTEXP ON (1=1 
	AND LOTEXP.CLIENT_CODE = A.MANDT 
	AND LOTEXP.ITEM_CODE = A.MATNR 
	AND LOTEXP.BATCH_NUMBER = A.CHARG 
	AND LOTEXP.PLANT=A.WERKS ) 
LEFT OUTER JOIN ZPBI_ITEMS ITEM ON (1=1 
	AND ITEM.CLIENT_CODE = A.MANDT 
	AND ITEM.ITEM_CODE = A.MATNR ) 
LEFT OUTER JOIN ZPBI_BUSLINE BUSLINE ON (1=1 
	AND BUSLINE.CLIENT_CODE=ITEM.CLIENT_CODE 
	AND BUSLINE.BUSLINE_CODE=ITEM.MATERIAL_GROUP)
	
LEFT OUTER JOIN ZPBI_ITEM_TRADE_PRICE AS TP ON (TP.MANDT = A.MANDT 
		AND TP.MATNR = A.MATNR) 

	 
WHERE 1=1 
AND A.MANDT=300;

CREATE VIEW ETL.ZPBI_INV_ONHAND_STOCK_UB AS
SELECT
	 "CLIENT_CODE" ,
	 "CLIENT_DESC" ,
	 "ITEM_CODE" ,
	 "ITEM_DESC" ,
	 "BUSLINE_CODE" ,
	 "BUSLINE_DESC" ,
	 "WERKS" ,
	 "SUBINVENTORY_CODE" ,
	 "SUBINVENTORY_DESC" ,
	 "BATCH_NUMBER" ,
	 "BATCH_EXP_DATE" ,
	 "PLANT" ,
	 "PLANT_DESC" ,
	 "COMPANY_CODE" ,
	 "COMPANY_DESC" ,
	 "QTY" ,
	 "ITEM_COST" ,
	 "TRADE_PRICE" ,
	 "CREATED_DATE" ,
	 "NAME_OF_PERSON" ,
	 "LAST_CHANGE" ,
	 "WHO_CHANGED_OBJECT" ,
	 "FISCAL_YEAR" 
FROM ( SELECT
	 A.MANDT CLIENT_CODE ,
	 B.MTEXT CLIENT_DESC ,
	 A.MATNR ITEM_CODE ,
	 C.MAKTX ITEM_DESC ,
	 BUSLINE.BUSLINE_CODE ,
	 BUSLINE.BUSLINE_DESC ,
	 A.WERKS ,
	 A.LGORT SUBINVENTORY_CODE ,
	 E.LGOBE SUBINVENTORY_DESC ,
	 A.CHARG BATCH_NUMBER ,
	 LOTEXP.BATCH_EXP_DATE ,
	 D.BWKEY PLANT ,
	 D.NAME1 PLANT_DESC ,
	 G.BUKRS COMPANY_CODE ,
	 COMP.BUTXT COMPANY_DESC ,
	 A.CLABS QTY ,
	 IFNULL(COST.ITEM_COST,
	 0)ITEM_COST,
	 IFNULL(TP.KBETR,
	 0) TRADE_PRICE ,
	 A.ERSDA CREATED_DATE ,
	 A.ERNAM NAME_OF_PERSON ,
	 A.LAEDA LAST_CHANGE ,
	 A.AENAM WHO_CHANGED_OBJECT ,
	 A.LFGJA FISCAL_YEAR 
	FROM SAPABAP1.MCHB A 
	LEFT OUTER JOIN ZPBI_ITEM_COST COST ON(COST.CLIENT_CODE=A.MANDT 
		AND COST.ITEM_CODE=A.MATNR 
		AND COST.PLANT=A.WERKS 
		AND COST.BATCH=A.CHARG) 
	left outer jOIN SAPABAP1.T000 B ON (1=1 
		AND B.MANDT = A.MANDT ) 
	LEFT OUTER JOIN SAPABAP1.MAKT C ON (1=1 
		AND C.MANDT = A.MANDT 
		AND C.MATNR = A.MATNR ) 
	LEFT OUTER JOIN SAPABAP1.T001W D ON (1=1 
		AND D.MANDT = A.MANDT 
		AND D.WERKS = A.WERKS ) 
	LEFT OUTER JOIN SAPABAP1.T001L E ON (1=1 
		AND E.MANDT = A.MANDT 
		AND E.WERKS = A.WERKS 
		AND E.LGORT = A.LGORT ) 
	LEFT OUTER JOIN SAPABAP1.MARD F ON (1=1 
		AND F.MANDT = A.MANDT 
		AND F.MATNR = A.MATNR 
		AND F.WERKS = A.WERKS 
		AND F.LGORT = A.LGORT ) 
	LEFT OUTER JOIN SAPABAP1.T001K G ON (1=1 
		AND G.MANDT = D.MANDT 
		AND G.BWKEY = D.BWKEY ) 
	LEFT OUTER JOIN SAPABAP1.T001 COMP ON (1=1 
		AND COMP.MANDT = G.MANDT 
		AND COMP.BUKRS = G.BUKRS 
		AND COMP.LAND1='PK' ) 
	LEFT OUTER JOIN ZPBI_BATCHES LOTEXP ON (1=1 
		AND LOTEXP.CLIENT_CODE = A.MANDT 
		AND LOTEXP.ITEM_CODE = A.MATNR 
		AND LOTEXP.BATCH_NUMBER = A.CHARG 
		AND LOTEXP.PLANT=A.WERKS ) 
	LEFT OUTER JOIN ZPBI_ITEMS ITEM ON (1=1 
		AND ITEM.CLIENT_CODE = A.MANDT 
		AND ITEM.ITEM_CODE = A.MATNR ) 
	LEFT OUTER JOIN ZPBI_BUSLINE BUSLINE ON (1=1 
		AND BUSLINE.CLIENT_CODE=ITEM.CLIENT_CODE 
		AND BUSLINE.BUSLINE_CODE=ITEM.MATERIAL_GROUP) 
	LEFT OUTER JOIN ZPBI_ITEM_TRADE_PRICE AS TP ON (TP.MANDT = A.MANDT 
		AND TP.MATNR = A.MATNR) 
	WHERE 1=1 
	AND A.MANDT=300 ) 
WHERE 1=1 AND COMPANY_CODE=6100
AND COMPANY_CODE=6100;

CREATE VIEW ETL.ZPBI_INV_TRANS_ISSUE AS
SELECT
	 INV.CLIENT_CODE ,
	 INV.COMPANY_CODE ,
	 INV.COMPANY_DESC ,
	 INV.TRANSACTION_DATE ,
	 INV.MATERIAL_DOCUMENT_NUMBER ,
	 INV.DOCUMENT_YEAR ,
	 INV.UNIQUE_LINE_ID ,
	 INV.PARENT_ID ,
	 INV.ORIGINAL_LINE_ACCOUNT ,
	 INV.MOVEMENT_TYPE ,
	 INV.MOVEMENT_DESC ,
	 INV.PLANT ,
	 INV.PLANT_DESC ,
	 INV.PURCHASING_ORG ,
	 INV.PURCHASING_ORG_DESC ,
	 INV.STORAGE_LOCATION ,
	 INV.STORAGE_LOC_DESC ,
	 INV.STOCK_TYPE ,
	 INV.AUTO_CREATE_ITEM ,
	 INV.ITEM_CODE ,
	 INV.ITEM_DESC ,
	 INV.DOCUMENT_REFERENCE ,
	 INV.QTY ,
	 INV.VALUE ,
	 INV.BATCH_NUMBER ,
	 INV.BATCH_EXPIRY_DATE ,
	 INV.DIV_CD ,
	 INV.DIV_DESC ,
	 INV.TRANSPORTER_TR_NUMBER_DATE 
FROM ZPBI_INVENTORY_MOVEMENT INV 
WHERE 1=1 
AND INV.MOVEMENT_TYPE IN ('313') 
AND INV.AUTO_CREATE_ITEM='';

CREATE VIEW ETL.ZPBI_INV_TRANS_RCV AS
SELECT
	 INV.CLIENT_CODE ,
	 INV.COMPANY_CODE ,
	 INV.COMPANY_DESC ,
	 INV.TRANSACTION_DATE ,
	 INV.MATERIAL_DOCUMENT_NUMBER ,
	 INV.DOCUMENT_YEAR ,
	 INV.UNIQUE_LINE_ID ,
	 INV.PARENT_ID ,
	 INV.ORIGINAL_LINE_ACCOUNT ,
	 INV.MOVEMENT_TYPE ,
	 INV.MOVEMENT_DESC ,
	 INV.PLANT ,
	 INV.PLANT_DESC ,
	 INV.PURCHASING_ORG ,
	 INV.PURCHASING_ORG_DESC ,
	 INV.STORAGE_LOCATION ,
	 INV.STORAGE_LOC_DESC ,
	 INV.STOCK_TYPE ,
	 INV.AUTO_CREATE_ITEM ,
	 INV.ITEM_CODE ,
	 INV.ITEM_DESC ,
	 INV.DOCUMENT_REFERENCE ,
	 INV.QTY ,
	 INV.VALUE ,
	 INV.BATCH_NUMBER ,
	 INV.BATCH_EXPIRY_DATE ,
	 INV.DIV_CD ,
	 INV.DIV_DESC ,
	 INV.TRANSPORTER_TR_NUMBER_DATE 
FROM ZPBI_INVENTORY_MOVEMENT INV 
WHERE 1=1 
AND INV.MOVEMENT_TYPE IN ('315') 
AND INV.AUTO_CREATE_ITEM='';

CREATE VIEW ETL.ZPBI_INVENTORY_MOVEMENT AS
SELECT "MATERIAL_DOCUMENT_NUMBER_HEADER" , "CLIENT_CODE" , "COMPANY_CODE" , "COMPANY_DESC" , "TRANSACTION_DATE" , "MATERIAL_DOCUMENT_NUMBER" , "DOCUMENT_HEADER_TEXT" , "DOCUMENT_REFERENCE" , "DELIVERY_NOTE_NUMBER" , "PURCHASE_ORDER_NUMBER" , "DOCUMENT_YEAR" , "UNIQUE_LINE_ID" , "PARENT_ID" , "ORIGINAL_LINE_ACCOUNT" , "MOVEMENT_TYPE" , "MOVEMENT_DESC" , "PLANT" , "PLANT_DESC" , "PURCHASING_ORG" , "PURCHASING_ORG_DESC" , "STORAGE_LOCATION" , "STORAGE_LOC_DESC" , "ITEM_CODE" , "ITEM_DESC" , "UOM" , "STOCK_TYPE" , "MATERIAL_GROUP" , "MATERIAL_GROUP_DESC" , "BATCH_NUMBER" , "BATCH_EXPIRY_DATE" , "ITEM_NUMBER_PURCHASING_DOCUMENT" , "VENDOR_CODE" , "VENDOR_NAME" , "AUTO_CREATE_ITEM" , "QTY" , "QTY_UNIT_DELIVERY_NOTE" , "VALUE" , "DIV_CD" , "DIV_DESC" , "TRANSPORTER_TR_NUMBER_DATE" , "USER_NAME" , "TRANSACTION_CODE" , "SHKZG" , "DELIVERY_COMPLETED_INDICATOR" , "CREATED_BY" FROM (SELECT
	 MTLH.MBLNR MATERIAL_DOCUMENT_NUMBER_HEADER,
	 MTL.MANDT CLIENT_CODE ,
	 MTL.BUKRS COMPANY_CODE ,
	 COMP.BUTXT COMPANY_DESC ,
	 MTL.BUDAT_MKPF TRANSACTION_DATE ,
	 MTL.MBLNR MATERIAL_DOCUMENT_NUMBER ,
	 MTLH.BKTXT DOCUMENT_HEADER_TEXT,
	 MTL.XBLNR_MKPF DOCUMENT_REFERENCE ,
	 CASE WHEN MTL.BWART IN ('101',
	 '102') 
	THEN MTL.XBLNR_MKPF 
	ELSE NULL 
	END DELIVERY_NOTE_NUMBER ,
	 MTL.EBELN PURCHASE_ORDER_NUMBER ,
	 MTL.MJAHR DOCUMENT_YEAR ,
	 MTL.LINE_ID UNIQUE_LINE_ID ,
	 MTL.PARENT_ID ,
	 MTL.MAA_URZEI ORIGINAL_LINE_ACCOUNT ,
	 MTL.BWART MOVEMENT_TYPE ,
	 MOVEMENT.BTEXT MOVEMENT_DESC ,
	 MTL.WERKS PLANT ,
	 PLANT.NAME1 PLANT_DESC ,
	 PLANT.EKORG PURCHASING_ORG ,
	 PUR_ORG.EKOTX PURCHASING_ORG_DESC ,
	 MTL.LGORT STORAGE_LOCATION ,
	 STR_LOC.LGOBE STORAGE_LOC_DESC ,
	 MTL.MATNR ITEM_CODE ,
	 ITEM.MAKTX ITEM_DESC ,
	 MTL.BPRME UOM ,
	 CASE WHEN MTL.LGORT IS NOT NULL 
	AND SUBSTRING(MTL.LGORT,
	 1,
	 1) = '8' 
	THEN 'Sellable' WHEN MTL.LGORT IS NOT NULL 
	AND SUBSTRING(MTL.LGORT,
	 1,
	 1) = '2' 
	THEN 'Demage' WHEN MTL.LGORT IS NOT NULL 
	AND SUBSTRING(MTL.LGORT,
	 1,
	 1) = '3' 
	THEN 'Expiry' WHEN MTL.LGORT IS NOT NULL 
	AND SUBSTRING(MTL.LGORT,
	 1,
	 1) = '4' 
	THEN 'Near Expiry' WHEN MTL.LGORT IS NOT NULL 
	AND SUBSTRING(MTL.LGORT,
	 1,
	 1) = '10' 
	THEN 'Transferable HUB' WHEN MTL.LGORT IS NOT NULL 
	AND SUBSTRING(MTL.LGORT,
	 1,
	 1) = '13' 
	THEN 'Hub Demage' WHEN MTL.LGORT IS NOT NULL 
	AND SUBSTRING(MTL.LGORT,
	 1,
	 1) = '15' 
	THEN 'Hub Expiry' WHEN MTL.LGORT IS NOT NULL 
	AND SUBSTRING(MTL.LGORT,
	 1,
	 1) = '17' 
	THEN 'Near Expiry' 
	ELSE MTL.LGORT 
	END STOCK_TYPE ,
	 ITEM_GROUP.MATKL MATERIAL_GROUP,
	 ITEMG.WGBEZ MATERIAL_GROUP_DESC,
	 MTL.CHARG BATCH_NUMBER,
	 BATCH.VFDAT BATCH_EXPIRY_DATE ,
	 MTL.EBELP ITEM_NUMBER_PURCHASING_DOCUMENT,
	 MTL.LIFNR VENDOR_CODE ,
	 VENDOR.NAME1 VENDOR_NAME ,
	 MTL.XAUTO AUTO_CREATE_ITEM ,
	 CASE WHEN MOVEMENT_TRANS.SHKZG='H' 
	THEN MTL.ERFMG*-1 
	ELSE MTL.ERFMG 
	END QTY ,
	--MTL.OIGLBPR,
	
	 MTL.LSMNG QTY_UNIT_DELIVERY_NOTE,
	 MTL.DMBTR VALUE ,
	-- MTL.CHARG BATCH_NUMBER ,
	 DIV.MATKL DIV_CD ,
	 DIV_DES.WGBEZ DIV_DESC ,
	 MTL.SGTXT TRANSPORTER_TR_NUMBER_DATE ,
	 MTL.USNAM_MKPF USER_NAME ,
	 MTL.TCODE2_MKPF TRANSACTION_CODE ,
	 MOVEMENT_TRANS.SHKZG ,
	 -- DC_REF.LFSNR,
 MTL.ELIKZ Delivery_Completed_Indicator ,
	 MTLH.USNAM CREATED_BY 
	FROM SAPABAP1.MSEG MTL 
	LEFT OUTER JOIN SAPABAP1.MKPF MTLH ON (1=1 
		AND MTLH.MANDT=MTL.MANDT 
		AND MTLH.MBLNR=MTL.MBLNR 
		AND MTLH.MJAHR=MTL.MJAHR) 
	LEFT OUTER JOIN SAPABAP1.T001W AS PLANT ON (1 = 1 
		AND PLANT.MANDT = MTL.MANDT 
		AND PLANT.WERKS = MTL.WERKS) 
	LEFT OUTER JOIN SAPABAP1.T024E AS PUR_ORG ON (1 = 1 
		AND PUR_ORG.MANDT = MTL.MANDT 
		AND PUR_ORG.EKORG = PLANT.EKORG) 
	LEFT OUTER JOIN SAPABAP1.MAKT ITEM ON (1 = 1 
		AND ITEM.MANDT = MTL.MANDT 
		AND ITEM.MATNR = MTL.MATNR) 
	LEFT OUTER JOIN SAPABAP1.T156T MOVEMENT ON (1 = 1 
		AND MOVEMENT.MANDT = MTL.MANDT 
		AND MOVEMENT.BWART = MTL.BWART 
		AND MOVEMENT.SPRAS = 'E' 
		AND MOVEMENT.SOBKZ = MTL.SOBKZ 
		AND MOVEMENT.KZBEW = MTL.KZBEW 
		AND MOVEMENT.KZZUG = MTL.KZZUG 
		AND MOVEMENT.KZVBR = MTL.KZVBR) 
	LEFT OUTER JOIN SAPABAP1.T001 COMP ON (1 = 1 
		AND COMP.MANDT = MTL.MANDT 
		AND COMP.BUKRS = MTL.BUKRS) 
	LEFT OUTER JOIN SAPABAP1.T001L STR_LOC ON (1 = 1 
		AND STR_LOC.MANDT = MTL.MANDT 
		AND STR_LOC.WERKS = MTL.WERKS 
		AND STR_LOC.LGORT = MTL.LGORT) 
	LEFT OUTER JOIN SAPABAP1.MCHA BATCH ON (1 = 1 
		AND BATCH.MANDT = MTL.MANDT 
		AND BATCH.MATNR = MTL.MATNR 
		AND BATCH.CHARG = MTL.CHARG 
		AND BATCH.WERKS = MTL.WERKS) 
	LEFT OUTER JOIN SAPABAP1.MARA DIV ON (1 = 1 
		AND DIV.MATNR = MTL.MATNR 
		AND DIV.MANDT = MTL.MANDT) 
	LEFT OUTER JOIN SAPABAP1.T023T DIV_DES ON (1 = 1 
		AND DIV_DES.MANDT = MTL.MANDT 
		AND DIV_DES.MATKL = DIV.MATKL) 
	LEFT OUTER JOIN SAPABAP1.LFA1 VENDOR ON (1 = 1 
		AND VENDOR.MANDT = MTL.MANDT 
		AND VENDOR.LIFNR = MTL.LIFNR) 
	LEFT OUTER JOIN sapabap1.t156 MOVEMENT_TRANS ON (1=1 
		AND MOVEMENT_TRANS.MANDT=MTL.MANDT 
		AND MOVEMENT_TRANS.BWART=MTL.BWART) 
	LEFT OUTER JOIN SAPABAP1.MARA ITEM_GROUP ON (1=1 
		AND ITEM_GROUP.MANDT=MTL.MANDT 
		AND ITEM_GROUP.MATNR=MTL.MATNR) 
	LEFT OUTER JOIN SAPABAP1.T023T ITEMG ON (1=1 
		AND ITEMG.MANDT=ITEM_GROUP.MANDT 
		AND ITEMG.MATKL=ITEM_GROUP.MATKL) 
	WHERE 1 = 1 
	AND MTL.MANDT = 300 
	--	AND MTL.MBLNR = '5000100654' 
 --	AND MTL.BWART IN ('101',	 '102') 
) 
WHERE 1=1;

CREATE VIEW ETL.ZPBI_INVOICE_PAYMENT_DATA AS
SELECT
	 -- /*
 BSAD.MANDT CLIENT_CODE,
	 CLNT.MTEXT CLIENT_DESC,
	 BUKRS COMPANY_CODE,
	 COMP.COMPANY_DESC,
	 KUNNR CUSTOMER_NUMBER,
	 CUST.NAME1 CUSTOMER_NAME,
	 AUGDT CLEARING_DATE,
	 AUGBL DOCUMENT_NUMBER ,
	 GJAHR GL_YEAR,
	 BELNR ACCOUNTING_DOC_NUMBER,
	 BSAD.BUZEI NUMBER_OF_LINE_ITEM,
	 BSAD.BUDAT POSTING_DATE ,
	 BSAD.XBLNR REF_DOCUMENT_NUMBER,
	 BSAD.BLART DOCUMENT_TYPE,
	 DOC.LTEXT DOCUMENT_TYPE_DESC ,
	 BSAD.SHKZG,
	 CASE WHEN BSAD.SHKZG='H' 
THEN BSAD.DMBTR*-1 
ELSE BSAD.DMBTR 
END AMOUNT ,
	 BSAD.SGTXT ITEM_TEXT --*/
 --BSAD.*
 
FROM SAPABAP1.BSAD 
LEFT OUTER JOIN SAPABAP1.T000 CLNT ON (1=1 
	AND CLNT.MANDT=BSAD.MANDT) 
LEFT OUTER JOIN ZPBI_COMPANIES COMP ON (1=1 
	AND BSAD.MANDT=COMP.CLIENT_CODE 
	AND BSAD.BUKRS=COMP.COMPANY_CODE) 
LEFT OUTER JOIN ZPBI_CUSTOMERS CUST ON (1=1 
	AND CUST.CLIENT_CODE=BSAD.MANDT 
	AND CUST.CUSTOMER_NUMBER=BSAD.KUNNR) 
LEFT OUTER JOIN ZPBI_ACCOUNTING_DOC_TYPE DOC ON (1=1 
	AND DOC.MANDT=BSAD.MANDT 
	AND DOC.BLART=BSAD.BLART) 
WHERE 1=1 
AND BSAD.MANDT IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT);

CREATE VIEW ETL.ZPBI_INVOICE_RECEIPTS AS
SELECT
	 ZIPD.CLIENT_CODE,
	COMPANY_CODE,
	ZIPD.CUSTOMER_NUMBER,
	CUST.NAME1 CUSTOMER_NAME ,
	REF_DOCUMENT_NUMBER ,
	SUM(AMOUNT)AMOUNT 
FROM ZPBI_INVOICE_PAYMENT_DATA ZIPD 
left outer join ZPBI_CUSTOMERS CUST ON (1=1 
	AND CUST.CLIENT_CODE=ZIPD.CLIENT_CODE 
	AND CUST.CUSTOMER_NUMBER=ZIPD.CUSTOMER_NUMBER) 
WHERE 1=1 
AND DOCUMENT_TYPE='DZ' 
AND REF_DOCUMENT_NUMBER<>'' 
GROUP BY ZIPD.CLIENT_CODE,
	COMPANY_CODE,
	ZIPD.CUSTOMER_NUMBER,
	REF_DOCUMENT_NUMBER,
	CUST.NAME1;

CREATE VIEW ETL.ZPBI_ITEM_COST AS
SELECT   
 
 BATCH.MANDT CLIENT_CODE
,BATCH.MATNR ITEM_CODE
,BATCH.WERKS PLANT
,BATCH.CHARG BATCH
,BATCH.BWTAR VENDOR_BATCH
,COST.VERPR ITEM_COST

FROM SAPABAP1.MCHA BATCH

LEFT OUTER JOIN SAPABAP1.MBEW COST ON (COST.MANDT=BATCH.MANDT AND COST.MATNR=BATCH.MATNR AND COST.BWTAR=BATCH.BWTAR 
AND COST.BWKEY=BATCH.WERKS )


WHERE
--MAIN CONDITION
 1=1   AND BATCH.MANDT=300;

CREATE VIEW ETL.ZPBI_ITEM_TRADE_PRICE AS
select
	 a5.mandt ,
	 mandt_desc.mtext ,
	 a5.kschl,
	 a5.matnr,
	 item.maktx ,
	 -- a5.datab start_date,
 -- a5.datbi end_date,
 MAX( a5.knumh)KNUMH ,
	 MAX( knp.kbetr )KBETR 
from sapabap1.konp knp,
	 sapabap1.a514 a5 ,
	 sapabap1.t000 mandt_desc,
	 sapabap1.makt item 
where 1=1 
and knp.mandt=300  
and knp.kschl='ZTRP' -- For TradePrice
and a5.mandt=knp.mandt 
and a5.kschl=knp.kschl 
and a5.knumh=knp.knumh 
and mandt_desc.mandt=a5.mandt 
and item.mandt=a5.mandt 
and item.matnr=a5.matnr 
GROUP BY a5.mandt ,
	 mandt_desc.mtext ,
	 a5.kschl,
	 a5.matnr,
	 item.maktx;

CREATE VIEW ETL.ZPBI_ITEMS AS
select
	 MARA.MANDT CLIENT_CODE,
	 BISMT Old_ITEM_CODE,
	 MARA.MATNR ITEM_CODE,
	 ITEM_DES.MAKTX ITEM_DESC,
	 MARA.ERSDA CREATED_ON,
	 MARA.MTART MATERIAL_TYPE ,
	 MAT_TYPE.MTBEZ MATERIAL_TYPE_DESC ,
	 MARA.MBRSH Industry_sector,
	 IND_TYPE.MBBEZ INDUSTRY_SECTOR_DESC ,
	 MARA.MATKL MATERIAL_GROUP ,
	 MAT_GRP.WGBEZ MATERIAL_GROUP_DESC 
from SAPABAP1.MARA 
LEFT OUTER JOIN SAPABAP1.T134T MAT_TYPE ON (1=1 
	AND MAT_TYPE.SPRAS='E' 
	AND MAT_TYPE.MANDT=MARA.MANDT 
	AND MAT_TYPE.MTART=MARA.MTART) 
LEFT OUTER JOIN SAPABAP1.T137T IND_TYPE ON (1=1 
	AND IND_TYPE.SPRAS='E' 
	AND IND_TYPE.MANDT=MARA.MANDT 
	AND IND_TYPE.MBRSH=MARA.MBRSH) 
LEFT OUTER JOIN SAPABAP1.T023T MAT_GRP ON (1=1 
	AND MAT_GRP.SPRAS='E' 
	AND MAT_GRP.MANDT=MARA.MANDT 
	AND MAT_GRP.MATKL=MARA.MATKL) 
LEFT OUTER JOIN SAPABAP1.MAKT ITEM_DES ON (1=1 
	AND ITEM_DES.SPRAS='E' 
	AND ITEM_DES.MANDT=MARA.MANDT 
	AND ITEM_DES.MATNR=MARA.MATNR) 
WHERE 1=1 
AND MARA.MANDT=300;

CREATE VIEW ETL.ZPBI_ORDER_TYPE AS
select
	 MANDT,
	SPRAS,
	AUART,
	BEZEI 
from SAPABAP1.TVAKT order_type 
where 1=1 
AND MANDT=300 
and SPRAS='E';

CREATE VIEW ETL.ZPBI_PAYMENT_TERMS AS
SELECT * FROM SAPABAP1.TVZBT  WHERE 1=1 AND MANDT =300 AND SPRAS ='E'
AND VTEXT<>'';

CREATE VIEW ETL.ZPBI_PLANTS AS
SELECT
	 MANDT CLIENT_CODE,
	WERKS PLANT,
	 NAME1 PLANT_DESC,
	STRAS HOUSE_NUMBER_STREET 
FROM SAPABAP1.T001W 
WHERE 1=1 
AND MANDT=300;

CREATE VIEW ETL.ZPBI_PROFIT_CENTER AS
SELECT
	MANDT CLIENT_CODE,PRCTR PROFIT_CENTER,KOKRS COMPANY_CODE,KTEXT PROFIT_CENTER_DESC
FROM SAPABAP1.CEPCT PRF_TXT 
WHERE 1=1 
AND PRF_TXT.MANDT IN (SELECT ZMANDT FROM ZPBI_MANDT)
and PRF_TXT.SPRAS='E';

CREATE VIEW ETL.ZPBI_SALE_INVOICE_TAX AS
select
--Not finalized yet
	 DISTINCT KONV.MANDT,
	KONV.KNUMV,
	KONV.KNUMH,
	KONP.KSCHL,
	KONP.MWSK1,
	TXT.VTEXT ,
	KONV.KPOSN,
	KONP.KBETR ---TXT.*
 
from SAPABAP1.KONV,
	SAPABAP1.KONP ,
	SAPABAP1.T685T TXT 
WHERE 1=1 
AND KONV.MANDT=300 
AND KONV.KNUMV='0000716571' --AND KONV.KSCHL='ZMRT'
 
AND KONP.MANDT=KONV.MANDT 
AND KONP.KNUMH=KONV.KNUMH 
AND KONP.KSCHL=KONV.KSCHL 
AND TXT.SPRAS='E' 
AND TXT.KSCHL=KONP.KSCHL 
and KONP.MWSK1='O9'/*FOR TAX*/ 
AND KONV.KPOSN IN (/*'000041',
	'000011',
	'000021',
	'000031'*/'000010');

CREATE VIEW ETL.ZPBI_SALE_INVOICES AS
select
	 CREATED_DATE,
	 CREATED_BY,
	 CLIENT_CODE,
	 CLIENT_DESC,
	 COMPANY_CODE,
	 COMPANY_DESC,
	 BRANCH,
	 BRANCH_DESC,
	 INVOICE_DATE,
	 SALES_ORDER_NUMBER,
	 SALES_ORDER_TYPE,
	 SALES_ORDER_TYPE_DESC,
	 INVOICE_NUMBER,
	 Z_CHECK_INVOICE_RETURN(INVOICE_NUMBER) INVOICE_EXECUTION_STATUS,
	 CANCELLED_BILLING_DOCUMENT,
	 PRICING_LIST_CODE,
	 PRICING_LIST_DESC,
	 BILLING_TYPE,
	 BILLING_TYPE_DESC,
	 BILLING_CATEGORY,
	 SD_DOCUMENT_CATEGORY,
	 DISTRIBUTION_CHANNEL,
	 PRICE_GROUP,
	 BOOKER_ID,
	 BOOKER_NAME,
	 SUPPLIER_ID,
	 SUPPLIER_NAME,
	 ITEM_CODE,
	 ITEM_DESCRIPTION,
	 BUSINESS_LINE_ID,
	 BUSLINE_DESCRIPTION,
	 CHANNEL_ID,
	 CHANNEL_DESCRIPTION,
	 CUSTOMER_ID,
	 CUSTOMER_NUMBER,
	 CUSTOMER_NAME,
	 SALES_ORDER_TYPE_ID,
	 SOLD_QTY,
	 LIST_PRICE,
	 UNIT_SELLING_PRICE,
	 BATCH_NUMBER,
	 LOT_EXPIRY_DATE,
	 QTY_TYPE,
	 CLAIMABLE_DISCOUNT,
	 UN_CLAIMABLE_DISCOUNT,
	 GROSS,
	 TAX_RECOVERABLE,
	 GROSS +(CLAIMABLE_DISCOUNT+UN_CLAIMABLE_DISCOUNT)+TAX_RECOVERABLE NET,
	 CUSTOMER_TRX_ID,
	 ACCOUNTING_DOCUMENT_NUMBER,
	 FISCAL_YEAR,
	 COUNTY_CODE,
	 CITY_CODE,
	 RECORD_CREATED,
	 RECORD_CHANGED,
	 TAXM1,
	 TAXM2,
	 TAXM3,
	 TAXM4,
	 TAXM5,
	 TAXM6,
	 TAXM7,
	 TAXM8,
	 TAXM9,
	 CUSTOMER_ACT_GROUP,
	 CUSTOMER_ACT_GROUP_DESC,
	 PAYMENT_TERM_ID,
	 PAYMENT_TERM_DESC,
	 CREDIT_DAYS,
	 TAXCLASS_CUST,
	 GST_REGISTRATION_NO,
	 TAX_TYPE,
	 SGTXT,
	 KNUMV,
	 PSTYV,
	 VGPOS,
	 POSNR,
	 SHKZG 
FROM ( select
	 A.ERDAT CREATED_DATE ,
	 A.ERNAM CREATED_BY ,
	 A.MANDT CLIENT_CODE ,
	 MANDT_DESC.MTEXT CLIENT_DESC ,
	 A.BUKRS COMPANY_CODE ,
	 COMP.BUTXT COMPANY_DESC ,
	 B.VKBUR BRANCH ,
	 LOC.BEZEI BRANCH_DESC ,
	 A.FKDAT INVOICE_DATE ,
	 B.AUBEL SALES_ORDER_NUMBER ,
	 ORDER_TYPE.AUART SALES_ORDER_TYPE ,
	 OTYPED.BEZEI SALES_ORDER_TYPE_DESC,
	 A.VBELN INVOICE_NUMBER ,
	 A.SFAKN Cancelled_billing_document,
	 --B.AUBEL SALES_ORDER_DOCUMENT ,
 A.KALSM PRICING_LIST_CODE,
	 PRICE_LIST.VTEXT PRICING_LIST_DESC,
	 A.FKART BILLING_TYPE ,
	 ST.VTEXT BILLING_TYPE_DESC,
	 A.FKTYP BILLING_CATEGORY ,
	 A.VBTYP SD_DOCUMENT_CATEGORY ,
	 A.VTWEG DISTRIBUTION_CHANNEL ,
	 A.KONDA PRICE_GROUP ,
	 C.PERNR BOOKER_ID ,
	 D.VORNA || '-' || D.NACHN BOOKER_NAME ,
	 E.PERNR SUPPLIER_ID ,
	 F.VORNA || '-' || F.NACHN SUPPLIER_NAME ,
	 B.MATNR ITEM_CODE ,
	 B.ARKTX ITEM_DESCRIPTION ,
	 ITEM.MATKL BUSINESS_LINE_ID ,
	 BUSLINE_DESC.WGBEZ BUSLINE_DESCRIPTION ,
	 A.KDGRP CHANNEL_ID ,
	 CHNL.KTEXT CHANNEL_DESCRIPTION ,
	 A.KUNAG CUSTOMER_ID ,
	 A.KUNAG CUSTOMER_NUMBER ,
	 CUST.NAME1 || ' ' || CUST.NAME2 CUSTOMER_NAME ,
	 B.AUBEL SALES_ORDER_TYPE_ID ,
	 case when B.SHKZG='X' 
	THEN B.FKIMG*-1 
	ELSE B.FKIMG 
	END SOLD_QTY ,
	 PRICE.KBETR LIST_PRICE,
	 B.CMPRE UNIT_SELLING_PRICE ,
	 --DLV.CHARG LOT_NUMBER ,
 B.CHARG BATCH_NUMBER,
	 LOTEXP.VFDAT LOT_EXPIRY_DATE ,
	 CASE WHEN B.PSTYV = 'TANN' 
	THEN 'BONUS' 
	ELSE 'SOLD' 
	END QTY_TYPE ,
	 CASE WHEN B.SHKZG='X' 
	THEN IFNULL(DIS.KWERT,
	 0)*-1 
	ELSE IFNULL(DIS.KWERT,
	 0) 
	END CLAIMABLE_DISCOUNT ,
	 CASE WHEN B.SHKZG='X' 
	THEN IFNULL(DIS1.KWERT,
	 0)*-1 
	ELSE IFNULL(DIS1.KWERT,
	 0) 
	END UN_CLAIMABLE_DISCOUNT ,
	 CASE WHEN B.SHKZG='X' 
	THEN DIS2.KWERT*-1 
	ELSE DIS2.KWERT 
	END GROSS ,
	 CASE WHEN B.SHKZG='X' 
	THEN B.MWSBP*-1 
	ELSE B.MWSBP 
	END TAX_RECOVERABLE ,
	 0 CUSTOMER_TRX_ID ,
	 A.BELNR ACCOUNTING_DOCUMENT_NUMBER ,
	 A.GJAHR FISCAL_YEAR ,
	 A.COUNC COUNTY_CODE ,
	 A.CITYC CITY_CODE ,
	 A.ERDAT RECORD_CREATED ,
	 A.AEDAT RECORD_CHANGED ,
	 B.TAXM1 ,
	 B.TAXM2 ,
	 B.TAXM3 ,
	 B.TAXM4 ,
	 B.TAXM5 ,
	 B.TAXM6 ,
	 B.TAXM7 ,
	 B.TAXM8 ,
	 B.TAXM9 ,
	 a.ktgrd CUSTOMER_ACT_GROUP,
	 ACT_GRP.VTEXT CUSTOMER_ACT_GROUP_DESC,
	 A.ZTERM PAYMENT_TERM_ID,
	 PAY_TERM.VTEXT PAYMENT_TERM_DESC,
	 Z_RETURN_CREDIT_DAYS( PAY_TERM.VTEXT)CREDIT_DAYS,
	 A.TAXK1||A.TAXK2||A.TAXK3||A.TAXK4||A.TAXK5||A.TAXK6||A.TAXK7||A.TAXK8||A.TAXK9 TAXCLASS_CUST,
	 A.STCEG GST_REGISTRATION_NO,
	 A.J_1AFITP TAX_TYPE,
	 B.SGTXT ,
	 A.KNUMV ,
	 B.PSTYV ,
	 B.VGPOS ,
	 B.POSNR,
	 b.shkzg 
	FROM SAPABAP1.VBRK AS A 
	LEFT OUTER JOIN SAPABAP1.VBRP AS B ON (A.MANDT = B.MANDT 
		AND A.VBELN = B.VBELN) 
	LEFT OUTER JOIN SAPABAP1.MCH1 AS LOTEXP ON (LOTEXP.MANDT = B.MANDT 
		AND LOTEXP.MATNR = B.MATNR 
		AND LOTEXP.CHARG = B.CHARG) 
	LEFT OUTER JOIN SAPABAP1.KONV AS DIS ON (DIS.MANDT = A.MANDT 
		AND DIS.KPOSN =B.POSNR -- B.VGPOS --'000010'						 
 
		AND DIS.KNUMV = A.KNUMV 
		AND DIS.KSCHL = 'ZCDV' --CLAIMABLE DISCOUNT
 ) 
	LEFT OUTER JOIN SAPABAP1.KONV AS DIS1 ON (DIS1.MANDT = A.MANDT 
		AND DIS1.KPOSN = B.POSNR --B.VGPOS --'000010'
 
		AND DIS1.KNUMV = A.KNUMV 
		AND DIS1.KSCHL = 'ZUDV' --UN-CLAIMABLE DISCOUNT
 ) 
	LEFT OUTER JOIN SAPABAP1.KONV AS DIS2 ON (DIS2.MANDT = A.MANDT 
		AND DIS2.KPOSN = B.POSNR --B.VGPOS --'000010'               
 
		AND DIS2.KNUMV = A.KNUMV 
		AND DIS2.KSCHL = 'ZTRP' --GROSS        
 
		and dis2.KINAK='' ) 
	LEFT OUTER JOIN SAPABAP1.T151T CHNL ON (1 = 1 
		AND CHNL.MANDT = A.MANDT 
		AND CHNL.SPRAS = 'E' 
		AND CHNL.KDGRP = A.KDGRP) 
	LEFT OUTER JOIN SAPABAP1.KONV PRICE ON (1=1 
		AND PRICE.MANDT = A.MANDT 
		AND PRICE.KPOSN = B.POSNR --B.VGPOS --- '000010'  
 
		AND PRICE.KNUMV = A.KNUMV 
		AND PRICE.KSCHL = 'ZTRP' /*Unit Selling Price*/ 
		AND PRICE.KINAK='' ) 
	LEFT OUTER JOIN SAPABAP1.T001 COMP ON (1=1 
		AND COMP.MANDT = A.MANDT 
		AND COMP.BUKRS = A.BUKRS) 
	LEFT OUTER JOIN SAPABAP1.VBAK ORDER_TYPE ON (1=1 
		AND ORDER_TYPE.MANDT = A.MANDT 
		AND ORDER_TYPE.VBELN = B.AUBEL) 
	LEFT OUTER JOIN SAPABAP1.KNA1 CUST ON (1=1 
		AND CUST.MANDT = A.MANDT 
		AND CUST.KUNNR = A.KUNAG) 
	LEFT OUTER JOIN SAPABAP1.TVKBT LOC ON (1=1 
		AND LOC.MANDT = A.MANDT 
		AND LOC.VKBUR = B.VKBUR 
		AND LOC.SPRAS = 'E') 
	LEFT OUTER JOIN SAPABAP1.VBPA C ON (1=1 
		AND C.PARVW = 'BK' --Booker Id Flag   
 
		AND C.MANDT = A.MANDT 
		AND C.VBELN = A.VBELN) 
	LEFT OUTER JOIN SAPABAP1.PA0002 D ON (1=1 
		AND D.ENDDA = '99991231' --this is using to check booker with end date 99991231 because we were getting duplicate booker
 
		AND D.MANDT = C.MANDT 
		AND D.PERNR = C.PERNR) 
	LEFT OUTER JOIN SAPABAP1.VBPA E ON (1=1 
		AND E.PARVW = 'ZS' --Supplier id
 
		AND E.MANDT = A.MANDT 
		AND E.VBELN = A.VBELN ) 
	LEFT OUTER JOIN SAPABAP1.PA0002 F ON (1=1 
		AND F.ENDDA = '99991231' --this is using to check supplier with end date 99991231 because we were getting duplicate supplier
 
		AND F.MANDT = E.MANDT 
		AND F.PERNR = E.PERNR ) 
	LEFT OUTER JOIN SAPABAP1.MARA ITEM ON (1=1 
		AND ITEM.MATNR = B.MATNR 
		AND ITEM.MANDT=B.MANDT) 
	LEFT OUTER JOIN SAPABAP1.T023T BUSLINE_DESC ON (1=1 
		AND BUSLINE_DESC.MANDT = A.MANDT 
		AND BUSLINE_DESC.MATKL = ITEM.MATKL) 
	LEFT OUTER JOIN SAPABAP1.T000 MANDT_DESC ON (1=1 
		AND MANDT_DESC.MANDT = A.MANDT) 
	LEFT OUTER JOIN SAPABAP1.TVKTT ACT_GRP ON (1=1 
		AND ACT_GRP.SPRAS='E' 
		AND ACT_GRP.MANDT=A.MANDT 
		AND ACT_GRP.KTGRD=A.KTGRD ) 
	LEFT OUTER JOIN SAPABAP1.TVZBT PAY_TERM ON (1=1 
		AND PAY_TERM.SPRAS='E' 
		AND PAY_TERM.MANDT=A.MANDT 
		AND PAY_TERM.ZTERM=A.ZTERM ) 
	LEFT OUTER JOIN SAPABAP1.T683U PRICE_LIST ON (1=1 
		AND PRICE_LIST.MANDT=A.MANDT 
		AND PRICE_LIST.SPRAS='E' 
		AND PRICE_LIST.KALSM=A.KALSM ) 
	LEFT OUTER JOIN ZPBI_SALES_INVOICE_TYPE ST ON (1=1 
		AND ST.MANDT=A.MANDT 
		AND ST.SPRAS='E' 
		AND ST.FKART=A.FKART) 
	LEFT OUTER JOIN ZPBI_ORDER_TYPE OTYPED ON (1=1 
		AND OTYPED.MANDT=ORDER_TYPE.MANDT 
		AND OTYPED.SPRAS='E' 
		AND OTYPED.AUART=ORDER_TYPE.AUART ) ) 
WHERE 1 = 1 --AND CANCELLED_BILLING_DOCUMENT<>''
---AND INVOICE_NUMBER IN ('1016000000','1015000211')
 
and client_CODE IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT) ORDER BY INVOICE_NUMBER;

CREATE VIEW ETL.ZPBI_SALES_INVOICE_TYPE AS
SELECT
	 MANDT ,
	 SPRAS ,
	 FKART ,
	 VTEXT 
FROM SAPABAP1.TVFKT 
WHERE 1=1 
AND MANDT=300 
AND SPRAS='E';

CREATE VIEW ETL.ZPBI_SALES_ORDER AS
SELECT "CLIENT_CODE" , "ORDER_NUMBER" , "COMPANY_CODE" , "COMPANY_DESC" , "BRANCH" , "BRANCH_DESC" , "REF_DOCUMENT_NUMBER" , "ORDER_DATE" , "ENTRY_TIME" , "DOCUMENT_DATE" , "SD_DOCUMENT_CATEGORY" , "SD_DOCUMENT_CATEGORY_DESC" , "DOCUMENT_TYPE" , "DOCUMENT_TYPE_DESC" , "HEADER_ORDER_REASON" , "HEADER_ORDER_REASON_DESC" , "ITEM_ORDER_REASON" , "ORDER_ITEM_REASON_DESC" , "BOOKER_NAME" , "ITEM_CODE" , "ITEM_DESC" , "SALES_DOCUMENT_ITEM_NO" , "MATERIAL_GROUP" , "MATERIAL_GROUP_DESC" , "ITEM_TYPE" , "BATCH_NUMBER" , "CREATION_DATE" , "NET_VALUE_HEADER" , "CUSTOMER_GROUP_CODE" , "CUSTOMER_GROUP_DESC" , "PAYMENT_TERM_CODE" , "PAYMENT_TERMS_DESC" , "LIST_PRICE" , "UNIT_SELLING_PRICE" , "PLANT" , "PLANT_DESC" , "ORDER_QTY" , "ORDER_QTY_VALUE" , "REQUIRED_QTY" , "REQUIRED_QTY_VALUE" , "CONFIRMED_QTY" , "CONFIRMED_QTY_VALUE" , "CREATED_BY" , "CHANGED_ON" , "OBJECT_HEADER" , "OBJECT_ITEM_LINES" , "SHORT_TEXT_SALES_ORDER" , "PROFIT_CENTER" , "KNUMV" , "ALL_ITEMS_REFERENCE_H" , "ALL_ITEMS_REFERENCE_DESC_H" , "CONFIRMATION_STATUS_H" , "CONFIRMATION_STATUS_DESC_H" , "DELIVERY_STATUS_H" , "DELIVERY_STATUS_DESC_H" , "OVERALL_DELIVERY_STATUS_ITEMS_H" , "OVERALL_DELIVERY_STATUS_DESC_ITEMS_H" , "BILLING_STATUS_H" , "BILLING_STATUS_DESC_H" , "OVERALL_REJECTION_STATUS_H" , "OVERALL_REJECTION_STATUS_DESC_H" , "STATUS_PICK_CONFIRMATION_H" , "STATUS_PICK_CONFIRMATION_DESC_H" , "OVERALL_BLOCKED_STATUS_H" , "OVERALL_BLOCKED_STATUS_DESC_H" , "OVERALL_INVOICE_STATUS_L" , "OVERALL_INVOICE_STATUS_DESC_L" , "REJECTION_STATUS_ITEM" , "REJECTION_STATUS_ITEM_DESC" , "DELAY_STATUS" , "DELAY_STATUS_DESC" , "VGPOS" , "UNIQUE_KEY" FROM (SELECT
	 ORD.MANDT CLIENT_CODE ,
	 ORD.VBELN ORDER_NUMBER ,
	 ORD.BUKRS_VF COMPANY_CODE,
	 COMP.COMPANY_DESC,
	  ORD.VKBUR BRANCH ,
	 LOC.BEZEI BRANCH_DESC ,
	 ORDSTAT.RFSTK REF_DOCUMENT_NUMBER ,
	 ORD.ERDAT ORDER_DATE ,
	 ORD.ERZET ENTRY_TIME ,
	 ORD.AUDAT DOCUMENT_DATE ,
	 ORD.VBTYP SD_DOCUMENT_CATEGORY ,
	 VBTYPE.DDTEXT SD_DOCUMENT_CATEGORY_DESC ,
	 ORD.AUART DOCUMENT_TYPE ,
	 DOCTYPE.BEZEI DOCUMENT_TYPE_DESC ,
	 ORD.AUGRU HEADER_ORDER_REASON ,
	 ORDREASON.BEZEI HEADER_ORDER_REASON_DESC ,
	 ORDITEM.ABGRU ITEM_ORDER_REASON ,
	 REASON.BEZEI ORDER_ITEM_REASON_DESC ,
	 BOOKER.VORNA || '-' || BOOKER.NACHN BOOKER_NAME ,
	 ORDITEM.MATNR ITEM_CODE ,
	 ITEM.MAKTX ITEM_DESC ,
	 ORDITEM.POSNR SALES_DOCUMENT_ITEM_NO ,
	 ORDITEM.MATKL MATERIAL_GROUP ,
	 MTGRP.WGBEZ MATERIAL_GROUP_DESC ,
	 ORDITEM.POSAR ITEM_TYPE ,
	 ORDITEM.CHARG BATCH_NUMBER ,
	
	 ORD.ERDAT CREATION_DATE ,
	 ORD.NETWR NET_VALUE_HEADER ,
	 CUSTGRP.KDGRP CUSTOMER_GROUP_CODE ,
	 CUSTGRPD.KTEXT CUSTOMER_GROUP_DESC ,
	 CUSTGRP.ZTERM PAYMENT_TERM_CODE ,
	 PAYMENT.VTEXT PAYMENT_TERMS_DESC ,
	 PRICE.KBETR LIST_PRICE,
	 ORDITEM.CMPRE UNIT_SELLING_PRICE ,
	 ORDITEM.WERKS PLANT ,
	 PLANT.NAME1 PLANT_DESC ,
	 ORDITEM.KWMENG ORDER_QTY ,
	 ORDITEM.KWMENG * ORDITEM.CMPRE ORDER_QTY_VALUE ,
	 ORDITEM.LSMENG REQUIRED_QTY ,
	 ORDITEM.LSMENG * ORDITEM.CMPRE REQUIRED_QTY_VALUE ,
	 ORDITEM.KBMENG CONFIRMED_QTY ,
	 ORDITEM.KBMENG * ORDITEM.CMPRE CONFIRMED_QTY_VALUE ,
	 ORD.ERNAM CREATED_BY ,
	 ORD.AEDAT CHANGED_ON ,
	 ORD.OBJNR OBJECT_HEADER ,
	 ORDITEM.OBJNR OBJECT_ITEM_LINES ,
	 ORDITEM.ARKTX SHORT_TEXT_SALES_ORDER ,
	 ORDITEM.PRCTR PROFIT_CENTER ,
	 ORD.KNUMV ,
	 --STATUS BLOCK 
 ORDSTAT.RFGSK ALL_ITEMS_REFERENCE_H ,
	 RFGSK.BEZEI ALL_ITEMS_REFERENCE_DESC_H ,
	 ORDSTAT.BESTK CONFIRMATION_STATUS_H ,
	 BESTK.BEZEI CONFIRMATION_STATUS_DESC_H ,
	 ORDSTAT.LFSTK DELIVERY_STATUS_H ,
	 LFSTK.BEZEI DELIVERY_STATUS_DESC_H ,
	 ORDSTAT.LFGSK OVERALL_DELIVERY_STATUS_ITEMS_H ,
	 LFGSK.BEZEI OVERALL_DELIVERY_STATUS_DESC_ITEMS_H ,
	 ORDSTAT.FKSTK BILLING_STATUS_H ,
	 FKSTK.BEZEI BILLING_STATUS_DESC_H ,
	 ORDSTAT.ABSTK OVERALL_REJECTION_STATUS_H ,
	 ABSTK.BEZEI OVERALL_REJECTION_STATUS_DESC_H ,
	 ORDSTAT.KOQUK STATUS_PICK_CONFIRMATION_H ,
	 KOQUK.BEZEI STATUS_PICK_CONFIRMATION_DESC_H ,
	 ORDSTAT.SPSTG OVERALL_BLOCKED_STATUS_H ,
	 SPSTG.BEZEI OVERALL_BLOCKED_STATUS_DESC_H ,
	 ORDITM_STAT.FKSAA OVERALL_INVOICE_STATUS_L ,
	 FKSAA.BEZEI OVERALL_INVOICE_STATUS_DESC_L ,
	 ORDITM_STAT.ABSTA REJECTION_STATUS_ITEM ,
	 ABSTA.BEZEI REJECTION_STATUS_ITEM_DESC ,
	 --ORDITM_STATUS.DCSTA Delay_status ,
 ORDITM_STAT.DCSTA DELAY_STATUS ,
	 DCSTA.BEZEI DELAY_STATUS_DESC ,
	 ORDITEM.VGPOS ,
	 ORDITEM.MANDT || ORDITEM.VBELN || ORDITEM.MATNR || ORDITEM.POSNR UNIQUE_KEY 
	FROM SAPABAP1.VBAK ORD 
	LEFT OUTER JOIN SAPABAP1.VBUK ORDSTAT ON (1 = 1 
		AND ORDSTAT.MANDT = ORD.MANDT 
		AND ORDSTAT.VBELN = ORD.VBELN) 
	LEFT OUTER JOIN SAPABAP1.VBAP ORDITEM ON (1 = 1 
		AND ORDITEM.MANDT = ORD.MANDT 
		AND ORDITEM.VBELN = ORD.VBELN) 
	LEFT OUTER JOIN SAPABAP1.VBUP ORDITM_STAT ON (1 = 1 
		AND ORDITM_STAT.MANDT = ORDITEM.MANDT 
		AND ORDITM_STAT.VBELN = ORDITEM.VBELN 
		AND ORDITM_STAT.POSNR = ORDITEM.POSNR) 
	LEFT OUTER JOIN SAPABAP1.VBPA AS ORDLINE ON (1 = 1 
		AND ORDLINE.MANDT = ORD.MANDT 
		AND ORDLINE.VBELN = ORD.VBELN 
		AND ORDLINE.PARVW = 'BK') 
	LEFT OUTER JOIN SAPABAP1.KONV PRICE ON (1 = 1 
		AND PRICE.MANDT = ORD.MANDT 
		AND PRICE.KNUMV = ORD.KNUMV 
		AND PRICE.KPOSN = ORDITEM.POSNR 
		AND KSCHL = 'ZTRP' 
		AND PRICE.KINAK = '') 
	LEFT OUTER JOIN SAPABAP1.T001W PLANT ON (1 = 1 
		AND PLANT.MANDT = ORDITEM.MANDT 
		AND PLANT.WERKS = ORDITEM.WERKS) 
	LEFT OUTER JOIN SAPABAP1.TVKBT LOC ON (1 = 1 
		AND LOC.SPRAS = 'E' 
		AND LOC.MANDT = ORD.MANDT 
		AND LOC.VKBUR = ORD.VKBUR) 
	LEFT OUTER JOIN SAPABAP1.PA0002 BOOKER ON (1 = 1 
		AND ENDDA = '99991231' 
		AND BOOKER.MANDT = ORDLINE.MANDT 
		AND BOOKER.PERNR = ORDLINE.PERNR) 
	LEFT OUTER JOIN SAPABAP1.MAKT ITEM ON (1 = 1 
		AND ITEM.MANDT = ORDITEM.MANDT 
		AND ITEM.MATNR = ORDITEM.MATNR) 
	LEFT OUTER JOIN SAPABAP1.T023T MTGRP ON (1 = 1 
		AND MTGRP.MANDT = ORDITEM.MANDT 
		AND MTGRP.SPRAS = 'E' 
		AND MTGRP.MATKL = ORDITEM.MATKL) 
	LEFT OUTER JOIN SAPABAP1.VBKD CUSTGRP ON (1 = 1 
		AND CUSTGRP.MANDT = ORD.MANDT 
		AND CUSTGRP.VBELN = ORD.VBELN 
		AND CUSTGRP.POSNR = ORDITEM.POSNR) 
	LEFT OUTER JOIN SAPABAP1.TVBST RFGSK ON (1 = 1 
		AND RFGSK.MANDT = ORDSTAT.MANDT 
		AND RFGSK.SPRAS = 'E' 
		AND RFGSK.TBNAM = 'VBUK' 
		AND RFGSK.FDNAM = 'RFGSK' 
		AND RFGSK.STATU = ORDSTAT.RFGSK) 
	LEFT OUTER JOIN SAPABAP1.TVBST BESTK ON (1 = 1 
		AND BESTK.MANDT = ORDSTAT.MANDT 
		AND BESTK.SPRAS = 'E' 
		AND BESTK.TBNAM = 'VBUK' 
		AND BESTK.FDNAM = 'BESTK' 
		AND BESTK.STATU = ORDSTAT.BESTK) 
	LEFT OUTER JOIN SAPABAP1.TVBST LFSTK ON (1 = 1 
		AND LFSTK.MANDT = ORDSTAT.MANDT 
		AND LFSTK.SPRAS = 'E' 
		AND LFSTK.TBNAM = 'VBUK' 
		AND LFSTK.FDNAM = 'LFSTK' 
		AND LFSTK.STATU = ORDSTAT.LFSTK) ---
 
	LEFT OUTER JOIN SAPABAP1.TVBST LFGSK ON (1 = 1 
		AND LFGSK.MANDT = ORDSTAT.MANDT 
		AND LFGSK.SPRAS = 'E' 
		AND LFGSK.TBNAM = 'VBUK' 
		AND LFGSK.FDNAM = 'LFGSK' 
		AND LFGSK.STATU = ORDSTAT.LFGSK) -------- LINE ITEM STATUS
 
	LEFT OUTER JOIN SAPABAP1.TVBST FKSTK ON (1 = 1 
		AND FKSTK.MANDT = ORDSTAT.MANDT 
		AND FKSTK.SPRAS = 'E' 
		AND FKSTK.TBNAM = 'VBUK' 
		AND FKSTK.FDNAM = 'FKSTK' 
		AND FKSTK.STATU = ORDSTAT.LFGSK) 
	LEFT OUTER JOIN SAPABAP1.TVBST ABSTK ON (1 = 1 
		AND ABSTK.MANDT = ORDSTAT.MANDT 
		AND ABSTK.SPRAS = 'E' 
		AND ABSTK.TBNAM = 'VBUK' 
		AND ABSTK.FDNAM = 'ABSTK' 
		AND ABSTK.STATU = ORDSTAT.ABSTK) 
	LEFT OUTER JOIN SAPABAP1.TVBST KOQUK ON (1 = 1 
		AND KOQUK.MANDT = ORDSTAT.MANDT 
		AND KOQUK.SPRAS = 'E' 
		AND KOQUK.TBNAM = 'VBUK' 
		AND KOQUK.FDNAM = 'KOQUK' 
		AND KOQUK.STATU = ORDSTAT.KOQUK) 
	LEFT OUTER JOIN SAPABAP1.TVBST SPSTG ON (1 = 1 
		AND SPSTG.MANDT = ORDSTAT.MANDT 
		AND SPSTG.SPRAS = 'E' 
		AND SPSTG.TBNAM = 'VBUK' 
		AND SPSTG.FDNAM = 'SPSTG' 
		AND SPSTG.STATU = ORDSTAT.SPSTG) 
	LEFT OUTER JOIN SAPABAP1.TVBST FKSAA ON (1 = 1 
		AND FKSAA.MANDT = ORDITM_STAT.MANDT 
		AND FKSAA.SPRAS = 'E' 
		AND FKSAA.TBNAM = 'VBUP' 
		AND FKSAA.FDNAM = 'FKSAA' 
		AND FKSAA.STATU = ORDITM_STAT.FKSAA) ---
 
	LEFT OUTER JOIN SAPABAP1.TVBST ABSTA ON (1 = 1 
		AND ABSTA.MANDT = ORDITM_STAT.MANDT 
		AND ABSTA.SPRAS = 'E' 
		AND ABSTA.TBNAM = 'VBUP' 
		AND ABSTA.FDNAM = 'ABSTA' 
		AND ABSTA.STATU = ORDITM_STAT.ABSTA) ---
 
	LEFT OUTER JOIN SAPABAP1.TVBST DCSTA ON (1 = 1 
		AND DCSTA.MANDT = ORDSTAT.MANDT 
		AND DCSTA.SPRAS = 'E' 
		AND DCSTA.TBNAM = 'VBUP' 
		AND DCSTA.FDNAM = 'DCSTA' 
		AND DCSTA.STATU = ORDITM_STAT.DCSTA) 
	LEFT OUTER JOIN SAPABAP1.T151T CUSTGRPD ON (1 = 1 
		AND CUSTGRPD.SPRAS = 'E' 
		AND CUSTGRPD.MANDT = CUSTGRP.MANDT 
		AND CUSTGRPD.KDGRP = CUSTGRP.KDGRP) 
	LEFT OUTER JOIN SAPABAP1.TVZBT PAYMENT ON (1 = 1 
		AND PAYMENT.SPRAS = 'E' 
		AND PAYMENT.MANDT = CUSTGRP.MANDT 
		AND PAYMENT.ZTERM = CUSTGRP.ZTERM) 
	LEFT OUTER JOIN SAPABAP1.TVAGT REASON ON (1 = 1 
		AND REASON.SPRAS = 'E' 
		AND REASON.MANDT = ORDITEM.MANDT 
		AND REASON.ABGRU = ORDITEM.ABGRU) 
	LEFT OUTER JOIN SAPABAP1.TVAUT ORDREASON ON (1 = 1 
		AND ORDREASON.SPRAS = 'E' 
		AND ORDREASON.MANDT = ORD.MANDT 
		AND ORDREASON.AUGRU = ORD.AUGRU) 
	LEFT OUTER JOIN SAPABAP1.DD07T VBTYPE ON (1 = 1 
		AND VBTYPE.DDLANGUAGE = 'E' 
		AND VBTYPE.DOMNAME = 'VBTYP' 
		AND VBTYPE.DOMVALUE_L = ORD.VBTYP) 
	LEFT OUTER JOIN SAPABAP1.TVAKT DOCTYPE ON (1 = 1 
		AND DOCTYPE.MANDT = ORD.MANDT 
		AND DOCTYPE.SPRAS = 'E' 
		AND DOCTYPE.AUART = ORD.AUART) 
	LEFT OUTER JOIN ZPBI_COMPANIES COMP ON (1=1 
		AND COMP.CLIENT_CODE=ORD.MANDT 
		AND COMP.COMPANY_CODE=ORD.BUKRS_VF) 
	WHERE 1 = 1 
	AND ORD.MANDT = 300 
	---AND ORD.VBELN ='8000000009' -- '8100000010'
 ---'8000000031'--'7119000179'--- --'8000000054'             
 ---AND    ORD.ERNAM IN ('A.ROSHAN', 'S.KAMRAN','JUNAID.Q')
) 
WHERE 1 = 1 ORDER BY ORDER_DATE,
	 ORDER_NUMBER;

CREATE VIEW ETL.ZPBI_SALES_ORDER_EXECUTION AS
SELECT
	 COMPANY_CODE,
	 COMPANY_DESC,
	 BRANCH,
	 BRANCH_DESC,
	 INVOICE_DATE,
	 SALES_ORDER_NUMBER,
	 INVOICE_NUMBER,
	 -- BILLING_TYPE_DESC,
 ITEM_CODE,
	 ITEM_DESCRIPTION,
	 SUM( SOLD_QTY)QTY,
	 UNIT_SELLING_PRICE 
FROM ( select
	 COMPANY_CODE,
	 COMPANY_DESC,
	 BRANCH,
	 BRANCH_DESC,
	 INVOICE_DATE,
	 SALES_ORDER_NUMBER,
	 CASE WHEN CANCELLED_BILLING_DOCUMENT='' 
	THEN INVOICE_NUMBER 
	ELSE CANCELLED_BILLING_DOCUMENT 
	END INVOICE_NUMBER,
	 BILLING_TYPE_DESC,
	 ITEM_CODE,
	 ITEM_DESCRIPTION,
	 SOLD_QTY,
	 UNIT_SELLING_PRICE 
	from zpbi_SALE_INVOICES 
	WHERE 1=1 
	---And invoice_number in ('1017000051'/*'8200000001',	 '1016002211'*/) 
) 
WHERE 1=1 
AND UNIT_SELLING_PRICE<>0 
GROUP BY COMPANY_CODE,
	 COMPANY_DESC,
	 BRANCH,
	 BRANCH_DESC,
	 INVOICE_DATE,
	 SALES_ORDER_NUMBER,
	 INVOICE_NUMBER,
	 --- BILLING_TYPE_DESC,
 ITEM_CODE,
	 ITEM_DESCRIPTION,
	 UNIT_SELLING_PRICE ORDER BY ITEM_CODE;

CREATE VIEW ETL.ZPBI_SAP_TABLE_COLUMNS AS
SELECT
	 TABNAME ,
	 FIELDNAME ,
	 AS4LOCAL ,
	 AS4VERS ,
	 POSITION ,
	 KEYFLAG ,
	 MANDATORY ,
	 ROLLNAME ,
	 CHECKTABLE ,
	 ADMINFIELD ,
	 INTTYPE ,
	 INTLEN ,
	 REFTABLE ,
	 PRECFIELD ,
	 REFFIELD ,
	 CONROUT ,
	 NOTNULL ,
	 DATATYPE ,
	 LENG ,
	 DECIMALS ,
	 DOMNAME ,
	 SHLPORIGIN ,
	 TABLETYPE ,
	 DEPTH ,
	 COMPTYPE ,
	 REFTYPE ,
	 LANGUFLAG ,
	 DBPOSITION ,
	 ANONYMOUS ,
	 OUTPUTSTYLE 
FROM SAPABAP1.DD03l 
WHERE 1=1;

CREATE VIEW ETL.ZPBI_SETNAME_DESC AS
SELECT
	   SLF.MANDT   CLIENT_CODE,SLF.SETCLASS,SLF.SUBCLASS,''COMPANY,SLF.SETNAME,SDC.DESCRIPT DESCRIPTION,
	   
	   SLF.VALFROM,SLF.VALTO
FROM SAPABAP1.SETLEAF SLF
LEFT OUTER JOIN SAPABAP1.SETHEADERT SDC ON (1=1 AND SDC.MANDT=SLF.MANDT AND SDC.LANGU='E' AND SDC.SETNAME=SLF.SETNAME )
WHERE 1=1 
AND SLF.MANDT in (select zmandt FROM ZPBI_MANDT);

CREATE VIEW ETL.ZPBI_STATEMENT_STRUCTURE AS
select 
a.mandt client_code,''setclass,''subclass,A.VERSN COMPANY,A.ERGSL SETNAME, B.TXT45 DESCRIPTION,
a.vonkt valfrom,a.biskt valto
--b.*
from sapabap1.FAGL_011ZC A
LEFT OUTER JOIN sapabap1.FAGL_011QT  B ON (1=1 AND B.MANDT=A.MANDT AND B.SPRAS='E'
AND B.VERSN=A.VERSN AND B.ERGSL=A.ERGSL AND B.VERSN=A.VERSN)
where 1=1 
and A.mandt IN (SELECT
	 ZMANDT 
	FROM ZPBI_MANDT) 
	AND A.VERSN='UDPL' ORDER BY A.ERGSL;

CREATE VIEW ETL.ZPBI_T_DC_NOT_RCV AS
SELECT
	 INV.STORAGE_LOCATION ORGANIZATION_ID ,
	 INV.STORAGE_LOC_DESC ORG_DESC ,
	 INV.TRANSACTION_DATE DC_DATE ,
	 INV.MATERIAL_DOCUMENT_NUMBER DC_NUMBER ,
	 IFNULL(INV1.MATERIAL_DOCUMENT_NUMBER,
	 'X') RECEIVING_DOCUMENT_NUMBER ,
	 INV.STORAGE_LOCATION SOURCE_ORG_ID ,
	 INV.STORAGE_LOC_DESC SOURCE_ORG_DESC ,
	 INV1.STORAGE_LOCATION TRANSFER_ORGANIZATION_ID ,
	 INV1.STORAGE_LOC_DESC RECEIVE_BR_DES ,
	 INV.ITEM_CODE INVENTORY_ITEM_ID ,
	 INV.ITEM_CODE ,
	 INV.ITEM_DESC ITEM_DESC ,
	 INV.QTY ,
	 IFNULL(INV1.QTY,
	 0) QTY_RCV ,
	 INV.VALUE TRANS_VALUE ,
	 INV.TRANSPORTER_TR_NUMBER_DATE 
FROM ZPBI_INV_TRANS_ISSUE INV 
LEFT OUTER JOIN ZPBI_INV_TRANS_RCV AS INV1 ON (INV1.CLIENT_CODE = INV.CLIENT_CODE 
	AND INV1.COMPANY_CODE = INV.COMPANY_CODE 
	AND INV1.DOCUMENT_REFERENCE = INV.MATERIAL_DOCUMENT_NUMBER 
	AND INV1.ITEM_CODE = INV.ITEM_CODE) 
WHERE 1 = 1 
AND INV.CLIENT_CODE = 300 ORDER BY INV.TRANSACTION_DATE;

CREATE VIEW ETL.ZPBI_T_LIVE_INV AS
SELECT
	 werks ORGANIZATION_ID ,
	 PLANT_DESC ORG_DESC ,
	 ITEM_CODE INVENTORY_ITEM_ID ,
	 ITEM_CODE ,
	 ITEM_DESC ITEM_DESC ,
	 BUSLINE_CODE BUSLINE_ID ,
	 BUSLINE_DESC BUSLINE_DESC ,
	 SUBINVENTORY_DESC SUBINVENTORY_CODE ,
	 BATCH_NUMBER BATCH_NUMBER ,
	 BATCH_EXP_DATE EXPIRATION_DATE ,
	 ITEM_COST ITEM_COST ,
	 TRADE_PRICE TRADE_PRICE ,
	 QTY QTY 
FROM ZPBI_INV_ONHAND_STOCK inv;

CREATE VIEW ETL.ZVIEWPHARMA AS
SELECT  t.bukrs CompanyCode, t.gjahr Year , t.hkont GL, t.shkzg Debit_S_Credit_H, t.pswsl Currency, t.prctr ProfitCentre, t.kokrs ControllingArea,
       t.kostl CostCentre, t.kunnr Customer, t.lifnr Vendor, t.blart DocumentType, t.budat PostingDate, t.setname SetNames,  COUNT(T.budat) rec_count, SUM(T.amt) AMT FROM 
       
       (
SELECT a.bukrs , a.gjahr, sum(a.dmbtr) amt, a.hkont, a.shkzg, a.pswsl, a.prctr, a.kokrs,
       a.kostl, a.kunnr, a.lifnr, b.blart, b.budat, c.setname
        FROM SAPABAP1.BSEG AS a
    INNER JOIN SAPABAP1.BKPF    as b ON a.bukrs = b.bukrs
                           AND a.BELNR = B.BELNR
                           AND a.gjahr = b.gjahr
                           AND a.mandt = b.mandt
    INNER JOIN SAPABAP1.SETLEAF as c  ON ( a.HKONT <= c.VALFROM AND a.HKONT >= c.VALTO ) AND a.mandt = c.mandt
           WHERE ( a.BUKRS   = 1000 AND a.gjahr   = 2019 )
           AND ( b.bukrs   = 1000 AND b.gjahr   = 2019 )
                    
           AND  c.SETNAME 
           				in('ZPCFS164','ZPCFS166','ZPCFS168','ZPCFS167','ZPCFS169','ZPCFS170' )
           				--'ZPCFS192','ZPCFS185',
           AND ( b.BUDAT BETWEEN '20180701' AND '20190331' )
           AND a.mandt = 300 AND b.mandt = 300 AND c.mandt = 300  
group by a.bukrs, a.gjahr,   a.hkont, a.shkzg, a.pswsl, a.prctr, a.kokrs,
       a.kostl, a.kunnr, a.lifnr, b.blart, b.budat, c.setname                    
UNION            
           
 SELECT a.bukrs, a.gjahr, sum(a.dmbtr) amt, a.hkont, a.shkzg, a.pswsl, a.prctr, a.kokrs,
       a.kostl, a.kunnr, a.lifnr, b.blart, b.budat, c.setname
        FROM SAPABAP1.BSEG AS a
    INNER JOIN SAPABAP1.BKPF    as b ON a.bukrs = b.bukrs
                           AND a.BELNR = B.BELNR
                           AND a.gjahr = b.gjahr
                           AND a.mandt = b.mandt
    INNER JOIN SAPABAP1.SETLEAF as c  ON ( a.HKONT <= c.VALFROM AND a.HKONT >= c.VALTO ) AND a.mandt = c.mandt
           WHERE ( a.BUKRS   = 1000 AND a.gjahr   = 2019 )
           AND ( b.bukrs   = 1000 AND b.gjahr   = 2019 )
           AND       c.SETNAME IN ( SELECT SUBSETNAME
                               FROM SAPABAP1.SETNODE
                              WHERE SETNAME in('ZPCFS163','ZPCFS165') )
           AND ( b.BUDAT BETWEEN '20180701' AND '20190331' )
           AND a.mandt = 300 AND b.mandt = 300 AND c.mandt = 300
group by a.bukrs, a.gjahr,   a.hkont, a.shkzg, a.pswsl, a.prctr, a.kokrs,
       a.kostl, a.kunnr, a.lifnr, b.blart, b.budat, c.setname                      
        ) t
         group by t.bukrs , t.gjahr  , t.hkont , t.shkzg , t.pswsl , t.prctr , t.kokrs ,   t.kostl , t.kunnr , t.lifnr , t.blart , t.budat , t.setname;

CREATE FUNCTION Z_CHECK_INVOICE_RETURN (I_INVOICE_NUMBER VARCHAR(50))
 RETURNS V_INVOICE_TYPE VARCHAR(100) LANGUAGE  SQLSCRIPT AS
BEGIN 

DECLARE
 V_COUNT INTEGER:=0;

 SELECT
	 COUNT(*) 
INTO V_COUNT 
FROM SAPABAP1.VBRK A 
WHERE A.SFAKN=I_INVOICE_NUMBER ;

IF
(V_COUNT>0) THEN V_INVOICE_TYPE:='RETURN';
ELSEIF
(V_COUNT=0) THEN V_INVOICE_TYPE:='EXECUTION';

END IF;

END;

CREATE 
 FUNCTION Z_GET_CLAIM_UN_DISCOUNT (I_DISC_TYPE VARCHAR(1), I_KPOSN DOUBLE, I_KNUMV DOUBLE)
 RETURNS V_DISCOUNT_AMOUNT DOUBLE LANGUAGE  SQLSCRIPT AS
BEGIN 



IF
I_DISC_TYPE='C'
 THEN 
 
 SELECT SUM(KBETR) INTO V_DISCOUNT_AMOUNT FROM SAPABAP1.KONV
WHERE 1=1 AND MANDT=300 AND KPOSN=I_KPOSN  AND KNUMV=I_KNUMV
 AND KSCHL IN ('ZCDP','ZCDV');
 
END IF;

END;

CREATE   FUNCTION Z_GET_FIRST_DATE (I_DATE DATE)
 RETURNS FIRST_DATE date  AS 
BEGIN 
DECLARE 
DAY_OF_MONTH INTEGER;
DECLARE
LAST_DAY_OF_MONTH INTEGER;

DAY_OF_MONTH :=EXTRACT (DAY FROM CURRENT_DATE);

 FIRST_DATE:=add_days(I_DATE, - extract (day from I_DATE)+1);


END;

create function Z_RETURN_CREDIT_DAYS (I_PAYMENT_TERMS VARCHAR(50))
RETURNS vResult nvarchar(100)   LANGUAGE  SQLSCRIPT AS
begin

declare vString nvarchar(100):=I_PAYMENT_TERMS;
declare vCharInd int default -1;

VRESULT:='';
while vCharInd <> 0 do

select locate_regexpr(START '[\p{N}]' IN :vString ) into "VCHARIND" from dummy; -- numbers

if (:vCharInd > 0) then
select concat(:vResult, substring(:vString, :vCharInd, 1)) into "VRESULT" from dummy;
select substring(:vString, :vCharInd+1) into "VSTRING" from dummy;
end if;

end while;
end;