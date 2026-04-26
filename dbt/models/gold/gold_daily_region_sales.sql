select
    event_date,
    region,
    count(*) as delivered_orders,
    cast(sum(order_amount) as decimal(12, 2)) as delivered_revenue
from {{ ref('silver_latest_order_state') }}
where status = 'delivered'
group by 1, 2
