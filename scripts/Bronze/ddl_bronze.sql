--creation table bronze.crm_cust_info
create table bronze.crm_cust_info(
cst_id INT,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(10),
cst_gndr varchar(10),
cst_create_date date
)
go
--creation table bronze.prd_info
create table bronze.crm_prd_info(
prd_id INT,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost  DECIMAL(10, 2),
prd_line varchar(50),
prd_start_dt DATE,
prd_end_dt DATE
)
go
--creation table bronze.sales_details

create table bronze.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt INT,
sls_sales DECIMAL(10,2),
sls_quantity INT,
sls_price DECIMAL(10,2)
)
go

--creation table bronze.prd_info
create table bronze.erp_CUST_AZ12(
CID varchar(50),
BDATE Date,
GEN varchar(50)
)
go
--creation table bronze.LOC_A101
create table bronze.erp_LOC_A101(
CID varchar(50),
CNTRY varchar(50)
)
go

-- creation table erp_PX_CAT_G1V2 

create table bronze.erp_PX_CAT_G1V2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50)
)
