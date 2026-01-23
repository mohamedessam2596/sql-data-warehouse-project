/*transformation for silver.crm_prd_info*/

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
