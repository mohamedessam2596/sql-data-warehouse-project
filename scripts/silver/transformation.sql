/*transformation for silver.crm_prd_info*/

select 
prd_id,
substring(prd_key,7,LEN(prd_key)) as prd_key ,
replace(substring(prd_key,1,5),'-','_') as cat_ID,
isnull(prd_cost,0) as prd_cost,
case upper(Trim(prd_line)) 
	when 'M' Then 'Mountain'
	when 'R' Then 'Road'
	when 'S' Then 'other sales'
	when 'T' Then 'Touring'
	else  'n/a'
	end as prd_line,
	prd_start_dt,
	 DATEADD(
        day,
        -1,
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        )
    ) AS prd_end_dt
    into  silver.crm_prd_info
	from [bronze].[crm_prd_info]]



/*transformation and load for silver.[crm_cust_info] */

with max_date as (
select *,
	rank() over(partition by cst_id order by cst_create_date desc) as rank_max_date 
from bronze.[crm_cust_info])
select cst_id,cst_key,
		trim(COALESCE(cst_firstname+' '+cst_lastname,cst_firstname,cst_lastname)) as customer_name,
		case when upper(trim(cst_marital_status))='M' then 'married'
		 when upper(trim(cst_marital_status))='S' then 'single' 
		 else 'n/a' end as cst_marital_status,
	case when upper(trim(cst_gndr))='M' then 'male'
		 when upper(trim(cst_gndr))='F' then 'female' 
		 else 'n/a' end as cst_gndr,
		 cst_create_date
		 into silver.[crm_cust_info]
from max_date
where rank_max_date=1 and cst_id is not null

/*transformation for silver.crm_sales_details*/

select sls_ord_num,sls_prd_key,sls_cust_id,
       case
			when sls_order_dt <0 or len(sls_order_dt)!=8 then null
	        else cast(cast(sls_order_dt as varchar(50)) as date) 
	   end as sls_order_dt,
       case
			when sls_ship_dt <0 or len(sls_ship_dt)!=8 then null
	        else cast(cast(sls_ship_dt as varchar) as date) 
	   end as sls_ship_dt,
	     case
			when sls_due_dt <0 or len(sls_due_dt)!=8 then null
	        else cast(cast(sls_due_dt as varchar) as date) 
	   end as sls_due_dt,
	   round(case when sls_sales<0 and sls_price>0 then sls_price*sls_quantity 
	          else sls_sales
		 end ,2)as sls_sales,
    round(case 
	     when sls_price<0 and sls_sales>0 then sls_sales/nullif(sls_quantity,0)
		 else sls_price
		 end,2 )as sls_price
		 into silver.crm_sales_details
from bronze.crm_sales_details





















