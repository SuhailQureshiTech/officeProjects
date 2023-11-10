-- Drop table

-- DROP TABLE franchise_sales;

CREATE TABLE franchise_sales (
	company_code varchar NULL,
	franchise_customer_order_no varchar NULL,
	franchise_customer_invoice_date date NULL,
	franchise_customer_invoice_number varchar NULL,
	channel varchar NULL,
	franchise_code varchar NULL,
	franchise_customer_number varchar NULL,
	ibl_customer_number varchar NULL,
	rd_customer_name varchar NULL,
	ibl_customer_name varchar NULL,
	customer_address varchar NULL,
	franchise_item_code varchar NULL,
	ibl_item_code varchar NULL,
	franchise_item_description varchar NULL,
	ibl_item_description varchar NULL,
	quantity_sold numeric(18, 2) NULL,
	gross_amount float8 NULL,
	reason varchar NULL,
	foc numeric(18, 2) NULL,
	batch_no varchar NULL,
	price float8 NULL,
	bon_qty float8 NULL,
	disc_amt float8 NULL,
	net_amt float8 NULL,
	discounted_rate float8 NULL,
	brick_code varchar NULL,
	brick_name varchar NULL,
	created_date timestamp NULL DEFAULT now()
);

-- Drop table

-- DROP TABLE franchise_sales_1;

CREATE TABLE franchise_sales_1 (
	company_code varchar NULL,
	franchise_customer_order_no varchar NULL,
	franchise_customer_invoice_date date NULL,
	franchise_customer_invoice_number varchar NULL,
	channel varchar NULL,
	franchise_code varchar NULL,
	franchise_customer_number varchar NULL,
	ibl_customer_number varchar NULL,
	rd_customer_name varchar NULL,
	ibl_customer_name varchar NULL,
	customer_address varchar NULL,
	franchise_item_code varchar NULL,
	ibl_item_code varchar NULL,
	franchise_item_description varchar NULL,
	ibl_item_description varchar NULL,
	quantity_sold numeric(18, 2) NULL,
	gross_amount float8 NULL,
	reason varchar NULL,
	foc numeric(18, 2) NULL,
	batch_no varchar NULL,
	price float8 NULL,
	bon_qty float8 NULL,
	disc_amt float8 NULL,
	net_amt float8 NULL,
	discounted_rate float8 NULL,
	brick_code varchar NULL,
	brick_name varchar NULL,
	created_date timestamp NULL
);

-- Drop table

-- DROP TABLE franchise_sales_2;

CREATE TABLE franchise_sales_2 (
	franchise_customer_order_no int8 NULL,
	franchise_customer_invoice_date timestamp NULL,
	franchise_customer_invoice_number text NULL,
	channel text NULL,
	franchise_code int8 NULL,
	franchise_customer_number int8 NULL,
	ibl_customer_number int8 NULL,
	rd_customer_name text NULL,
	ibl_customer_name text NULL,
	customer_address text NULL,
	franchise_item_code int8 NULL,
	ibl_item_code int8 NULL,
	franchise_item_description text NULL,
	ibl_item_description text NULL,
	quantity_sold int8 NULL,
	gross_amount float8 NULL,
	reason text NULL,
	foc int8 NULL,
	batch_no text NULL,
	price float8 NULL,
	bon_qty int8 NULL,
	disc_amt float8 NULL,
	net_amt float8 NULL,
	discounted_rate int8 NULL,
	brick_code int8 NULL,
	brick_name text NULL
);

-- Drop table

-- DROP TABLE franchise_sales_27oct;

CREATE TABLE franchise_sales_27oct (
	company_code varchar NULL,
	franchise_customer_order_no varchar NULL,
	franchise_customer_invoice_date date NULL,
	franchise_customer_invoice_number varchar NULL,
	channel varchar NULL,
	franchise_code varchar NULL,
	franchise_customer_number varchar NULL,
	ibl_customer_number varchar NULL,
	rd_customer_name varchar NULL,
	ibl_customer_name varchar NULL,
	customer_address varchar NULL,
	franchise_item_code varchar NULL,
	ibl_item_code varchar NULL,
	franchise_item_description varchar NULL,
	ibl_item_description varchar NULL,
	quantity_sold numeric(18, 2) NULL,
	gross_amount float8 NULL,
	reason varchar NULL,
	foc numeric(18, 2) NULL,
	batch_no varchar NULL,
	price float8 NULL,
	bon_qty float8 NULL,
	disc_amt float8 NULL,
	net_amt float8 NULL,
	discounted_rate float8 NULL,
	brick_code varchar NULL,
	brick_name varchar NULL,
	created_date timestamp NULL
);

-- Drop table

-- DROP TABLE franchise_sales_31oct23;

CREATE TABLE franchise_sales_31oct23 (
	company_code varchar NULL,
	franchise_customer_order_no varchar NULL,
	franchise_customer_invoice_date date NULL,
	franchise_customer_invoice_number varchar NULL,
	channel varchar NULL,
	franchise_code varchar NULL,
	franchise_customer_number varchar NULL,
	ibl_customer_number varchar NULL,
	rd_customer_name varchar NULL,
	ibl_customer_name varchar NULL,
	customer_address varchar NULL,
	franchise_item_code varchar NULL,
	ibl_item_code varchar NULL,
	franchise_item_description varchar NULL,
	ibl_item_description varchar NULL,
	quantity_sold numeric(18, 2) NULL,
	gross_amount float8 NULL,
	reason varchar NULL,
	foc numeric(18, 2) NULL,
	batch_no varchar NULL,
	price float8 NULL,
	bon_qty float8 NULL,
	disc_amt float8 NULL,
	net_amt float8 NULL,
	discounted_rate float8 NULL,
	brick_code varchar NULL,
	brick_name varchar NULL,
	created_date timestamp NULL
);

-- Drop table

-- DROP TABLE franchise_stock;

CREATE TABLE franchise_stock (
	company_code varchar NULL,
	ibl_distributor_code varchar NULL,
	dated date NULL,
	distributor_item_code varchar NULL,
	ibl_item_code varchar NULL,
	distributor_item_description varchar NULL,
	lot_number varchar NULL,
	expiry_date date NULL,
	stock_qty numeric NULL,
	stock_value numeric NULL,
	ibl_branch_code varchar NULL,
	price float8 NULL,
	in_transit_stock float8 NULL,
	purchase_unit float8 NULL
);

-- Drop table

-- DROP TABLE locations;

CREATE TABLE locations (
	location_id serial4 NOT NULL,
	location_name varchar(50) NULL,
	branch_code varchar(50) NULL,
	CONSTRAINT locations_pkey PRIMARY KEY (location_id)
);
CREATE INDEX ix_locations_location_id ON franchise.locations USING btree (location_id);

-- Drop table

-- DROP TABLE missing_customers;

CREATE TABLE missing_customers (
	"index" int8 NULL,
	distributor_customer_code text NULL,
	ref_customer_code text NULL,
	ibl_distributor_code int8 NULL,
	ibl_distributor_desc text NULL,
	ibl_customer_name text NULL,
	"IBL_CN" int8 NULL
);
CREATE INDEX ix_franchise_missing_customers_index ON franchise.missing_customers USING btree (index);

-- Drop table

-- DROP TABLE roles;

CREATE TABLE roles (
	id serial4 NOT NULL,
	roles_name varchar(100) NULL,
	CONSTRAINT roles_pkey PRIMARY KEY (id)
);
CREATE INDEX ix_roles_id ON franchise.roles USING btree (id);

-- Drop table

-- DROP TABLE temp_locations;

CREATE TABLE temp_locations (
	location_id int4 NULL,
	location_name varchar(50) NULL,
	branch_code varchar(50) NULL
);

-- Drop table

-- DROP TABLE test_franchise_sales;

CREATE TABLE test_franchise_sales (
	company_code varchar NULL,
	ibl_distributor_code varchar NULL,
	order_no varchar NULL,
	invoice_no varchar NULL,
	invoice_date date NULL,
	channel varchar NULL,
	distributor_customer_no varchar NULL,
	ibl_customer_no varchar NULL,
	customer_name varchar NULL,
	distributor_item_code varchar NULL,
	ibl_item_code varchar NULL,
	qty_sold numeric NULL,
	gross_amount numeric NULL,
	reason varchar NULL,
	bonus_qty numeric NULL,
	discount numeric NULL,
	item_description varchar NULL,
	address varchar NULL,
	current_dates timestamp NULL
);

-- Drop table

-- DROP TABLE test_franchise_stock;

CREATE TABLE test_franchise_stock (
	company_code varchar NULL,
	ibl_distributor_code varchar NULL,
	dated date NULL,
	distributor_item_code varchar NULL,
	ibl_item_code varchar NULL,
	distributor_item_description varchar NULL,
	lot_number varchar NULL,
	expiry_date date NULL,
	stock_qty numeric NULL,
	stock_value numeric NULL
);

-- Drop table

-- DROP TABLE users;

CREATE TABLE users (
	id serial4 NOT NULL,
	company_code varchar(100) NULL,
	email varchar(100) NULL,
	distributor_id varchar(100) NULL,
	username varchar(100) NULL,
	"password" varchar(100) NULL,
	created_at date NULL,
	status bool NULL,
	store_name varchar(50) NULL,
	role_id int4 NULL,
	location_id int4 NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id),
	CONSTRAINT users_location_id_fkey FOREIGN KEY (location_id) REFERENCES locations(location_id),
	CONSTRAINT users_role_id_fkey FOREIGN KEY (role_id) REFERENCES roles(id)
);
CREATE INDEX ix_users_id ON franchise.users USING btree (id);

-- Drop table

-- DROP TABLE users_activity_log;

CREATE TABLE users_activity_log (
	"Distributor_code" text NULL,
	"File_Name" text NULL,
	"Sales_Quantity" int8 NULL,
	"Sales_Gross_Amount" int8 NULL,
	"Sales_Discounts" int8 NULL,
	"Sales_Bonus_Quantity" int8 NULL,
	"Total_Sales_SKUs" int8 NULL,
	"Total_Sales_Rows" int8 NULL,
	recrod_date timestamp NULL DEFAULT now()
);

-- Drop table

-- DROP TABLE users_backup;

CREATE TABLE users_backup (
	id int4 NULL,
	company_code varchar(100) NULL,
	email varchar(100) NULL,
	distributor_id varchar(100) NULL,
	username varchar(100) NULL,
	"password" varchar(100) NULL,
	created_at date NULL,
	status bool NULL,
	store_name varchar(50) NULL,
	role_id int4 NULL,
	location_id int4 NULL
);

-- Drop table

-- DROP TABLE users_backup_26oct23;

CREATE TABLE users_backup_26oct23 (
	id int4 NULL,
	company_code varchar(100) NULL,
	email varchar(100) NULL,
	distributor_id varchar(100) NULL,
	username varchar(100) NULL,
	"password" varchar(100) NULL,
	created_at date NULL,
	status bool NULL,
	store_name varchar(50) NULL,
	role_id int4 NULL,
	location_id int4 NULL
);

CREATE OR REPLACE VIEW franchise.franchise_data
AS SELECT fs2.company_code,
    fs2.franchise_code AS ibl_distributor_code,
    u.username AS ibl_distributor_desc,
    u.branch_code,
    u.location_id AS distributor_location_id,
    u.location_name AS distributor_location_desc,
    fs2.franchise_customer_order_no AS order_no,
    fs2.franchise_customer_invoice_number AS invoice_number,
    fs2.franchise_customer_invoice_date AS invoice_date,
    fs2.channel,
    fs2.franchise_customer_number AS distributor_customer_code,
    TRIM(BOTH FROM replace(fs2.ibl_customer_number::text, '.0'::text, ''::text)) AS ibl_customer_code,
    fs2.ibl_customer_name,
    fs2.franchise_item_code AS distributor_item_code,
    fs2.ibl_item_code,
    fs2.ibl_item_description,
    fs2.quantity_sold AS sold_qty,
    fs2.gross_amount,
    fs2.bon_qty AS bonus_qty,
    fs2.disc_amt AS discount,
    fs2.reason,
    fs2.created_date AS record_date,
    fs2.customer_address AS address,
    fs2.brick_code,
    fs2.brick_name
   FROM franchise_sales fs2
     LEFT JOIN user_details u ON u.distributor_id::text = fs2.franchise_code::text;

CREATE OR REPLACE VIEW franchise.user_details
AS SELECT u.id,
    u.distributor_id,
    u.username,
    u.location_id,
    l.location_name,
    l.branch_code
   FROM users u
     LEFT JOIN locations l ON l.location_id = u.location_id
  ORDER BY u.username;

CREATE OR REPLACE VIEW franchise.vw_users_activity_log
AS SELECT ual."Distributor_code",
    ual."File_Name",
    ual."Sales_Quantity",
    ual."Sales_Gross_Amount",
    ual."Sales_Discounts",
    ual."Sales_Bonus_Quantity",
    ual."Total_Sales_SKUs",
    ual."Total_Sales_Rows",
    ((ual.recrod_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'Asia/Karachi'::text) AS recrod_date
   FROM users_activity_log ual;

CREATE OR REPLACE FUNCTION franchise.updateusers()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
declare 
begin 
	
	update users 
	set created_at=current_date,role_id =3,"status" =true,company_code ='6300'
	,"password" ='$2b$12$LKVeJNJJVthy56roCiamlOTl422W7YqbXIokK/p9xU9WK19X4QIZe'	;
	
	update users u
	set location_id =(select distinct  
			location_id from locations l
			where cast(l.branch_code  as text)=cast(u.email as text)
		) where u.email  is not null and u.location_id is null;

	update users 
	set email =null where 1=1 and location_id is not null;

	return 'Done'	;
end;
$function$
;