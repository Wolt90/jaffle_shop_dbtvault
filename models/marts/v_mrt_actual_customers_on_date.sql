with customers as 
(select scd.first_name, scd.last_name, scd.email, scd2.country, scd2.age 
,scd.effective_from as scd_effective_from
,coalesce (lead(scd.effective_from) over(partition by scd.customer_pk order by scd.effective_from), '9999-12-31') as scd_effective_to
--,scd2.effective_from as scd2_effective_from
,coalesce (scd2.effective_from,scd.effective_from)  as scd2_effective_from
--,coalesce (lead(scd2.effective_from) over(partition by scd2.customer_pk order by scd2.effective_from), '9999-12-31') as scd2_effective_to
,coalesce(scd2.scd2_effective_to, '9999-12-31') as scd2_effective_to
from {{ ref('hub_customer') }} hc  --dbt.hub_customer hc
left join {{ ref('sat_customer_details') }} scd on scd.customer_pk = hc.customer_pk  --dbt.sat_customer_details
left join (select *, lead(scd2.effective_from) over(partition by scd2.customer_pk order by scd2.effective_from) as scd2_effective_to from {{ ref('sat_customer_details_crm') }} scd2)scd2 on scd2.customer_pk = hc.customer_pk  --dbt.sat_customer_details_crm
)
select first_name, last_name, email, country, age
from customers
where current_timestamp between scd_effective_from and scd_effective_to
and current_timestamp between scd2_effective_from and scd2_effective_to
order by 1




-- SELECT scd.first_name, scd.last_name, scd.email
-- , case 
-- 	when effective_to = '99991231' then 'point2'
-- 	else 'point1'
-- end as point
-- from (SELECT first_name, last_name, email, effective_from
-- 	  ,coalesce (lead(effective_from) over(partition by customer_pk order by effective_from), '9999-12-31') as effective_to
-- 	  FROM {{ ref('sat_customer_details') }}
-- 	  left join {{ ref('sat_customer_details_crm') }}) scd2
-- 	  ) scd
-- order by 1


-- SELECT first_name, last_name, email
-- FROM (SELECT first_name, last_name, email, effective_from
-- 	  ,coalesce (lead(effective_from) over(partition by customer_pk order by effective_from), '9999-12-31') as effective_to
-- 	  FROM {{ ref('sat_customer_details') }} ) scd   --dbt.sat_customer_details
-- where current_timestamp between effective_from and effective_to
-- order by 1