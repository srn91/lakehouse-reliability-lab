select
    customer_id,
    count(*) as active_orders,
    cast(sum(case when status = 'delivered' then 1 else 0 end) as bigint) as delivered_orders,
    cast(
        sum(
            case
                when status = 'delivered' then order_amount
                else cast(0 as decimal(12, 2))
            end
        ) as decimal(12, 2)
    ) as delivered_revenue,
    max(event_ts) as latest_order_event_ts
from {{ ref('silver_latest_order_state') }}
group by 1
