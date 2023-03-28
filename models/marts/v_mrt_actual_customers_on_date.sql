SELECT scd.first_name, scd.last_name, scd.email
, case 
	when effective_to = '99991231' then 'point2'
	else 'point1'
end as point
from (SELECT first_name, last_name, email, effective_from
	  ,coalesce (lead(effective_from) over(partition by customer_pk order by effective_from), '9999-12-31') as effective_to
	  FROM {{ ref('sat_customer_details') }}) scd
order by 1


-- SELECT first_name, last_name, email
-- FROM (SELECT first_name, last_name, email, effective_from
-- 	  ,coalesce (lead(effective_from) over(partition by customer_pk order by effective_from), '9999-12-31') as effective_to
-- 	  FROM {{ ref('sat_customer_details') }} ) scd   --dbt.sat_customer_details
-- where current_timestamp between effective_from and effective_to
-- order by 1