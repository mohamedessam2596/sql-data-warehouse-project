/*table "[bronze].[crm_prd_info] " don't have dublicate on row i check by this query
*/
select prd_id,count(*) from 
[bronze].[crm_prd_info]
group by prd_nm 
having count(*)>1 
/*
but when i check by the under query i found dublicate i noticed every row is unique by prd_id 
the product_name must unique but i fiund it it dublicate so we track history of product but all every row
unique by prd_id 
*/
select prd_nm,count(*) from 
[bronze].[crm_prd_info]
group by prd_nm 
having count(*)>1 
--prd_key=BI-RB-BK-R93R-62
select top 1000 * from [bronze].[crm_prd_info]
where prd_key like '%BK-R93R-62%'

--check null prd_id
select * from [bronze].[crm_prd_info]
where prd_id is null


--check null in prd_key
select prd_key as prd_key from [bronze].[crm_prd_info]
where prd_key is null

/*substring prd_key from prd_key in crm_prd_info table to make join 
with sls_prd_key in table crm_sales_details */

select substring(prd_key,7,LEN(prd_key)) as prd_key from [bronze].[crm_prd_info]
--check space
with prd_key_cte as  (
select substring(prd_key,7,LEN(prd_key)) as prd_key from [bronze].[crm_prd_info]
)
select * from prd_key_cte
where trim(prd_key)!=prd_key

--check the product not include any sales transaction 
with prd_key_cte as  (
select substring(prd_key,7,LEN(prd_key)) as prd_key from [bronze].[crm_prd_info]
)
select * from prd_key_cte
where prd_key not in (select sls_prd_key from [bronze].crm_sales_details)

/*substring cat_ID from prd_key in crm_prd_info table to make join with erp_px_CAT_G1V2
*/
select substring(prd_key,1,5) as cat_ID from [bronze].[crm_prd_info]
--check space
with cat_ID_cte as  (
select substring(prd_key,1,5) as cat_ID from [bronze].[crm_prd_info])
select * from cat_ID_cte
where trim(cat_ID)!=cat_ID

--check joining between crm_prd_info and erp_px_CAT_G1V2
with cat_ID_cte as  (
select replace(substring(prd_key,1,5),'-','_') as cat_ID from [bronze].[crm_prd_info])
select * from cat_ID_cte 
inner join [bronze].[erp_PX_CAT_G1V2] cat
on cat_ID_cte.cat_ID=cat.ID

--check product in product not in cat_product 
-- we found CO-PE cat_prod does not found in erp_PX_CAT_G1V2

with cat_ID_cte as  (
select replace(substring(prd_key,1,5),'-','_') as cat_ID from [bronze].[crm_prd_info])
select * from cat_ID_cte 
where cat_ID_cte.cat_ID not in (select ID from [bronze].[erp_PX_CAT_G1V2])
-- see row belong to 'CO_PE' category
with cat_ID_cte as  (
select *,replace(substring(prd_key,1,5),'-','_') as cat_ID from [bronze].[crm_prd_info])
select * from cat_ID_cte 
left join [bronze].[erp_PX_CAT_G1V2] cat
on cat_ID_cte.cat_ID=cat.ID
where cat_ID_cte.cat_ID='CO_PE'

--check null or negative and replace it
select isnull(prd_cost,0) from [bronze].[crm_prd_info]
where prd_cost is null or prd_cost<0

--stanaderzation  and mapping prd_line
select distinct prd_line
from [bronze].[crm_prd_info]

select 
	case upper(Trim(prd_line)) 
	when 'M' Then 'Mountain'
	when 'R' Then 'Road'
	when 'S' Then 'other sales'
	when 'T' Then 'Touring'
	else  'n/a'
	end as prd_line
from [bronze].[crm_prd_info]

--check prd_end_dt<prd_start_dt 
-- we gound alot we will solve it by under query 
select top 1000 * from [bronze].[crm_prd_info]
where prd_end_dt<prd_start_dt

select 
	prd_start_dt,
	 DATEADD(
        day,
        -1,
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        )
    ) AS prd_end_dt
from [bronze].[crm_prd_info]

----- collect all this transformation column on one 


select 
prd_id,
substring(prd_key,7,LEN(prd_key)) as prd_key ,
replace(substring(prd_key,1,5),'-','_') as cat_ID,
isnull(prd_cost,0),
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

	from [bronze].[crm_prd_info]

