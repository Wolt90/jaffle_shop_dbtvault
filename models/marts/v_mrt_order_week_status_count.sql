SELECT date_trunc('week', order_date)::date order_week, status, count(order_pk) count
FROM {{ ref('sat_order_details') }}
group by order_date, status
order by 1