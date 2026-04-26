with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by event_ts desc, ingestion_ts desc
        ) as latest_rank
    from {{ ref('silver_orders') }}
)

select
    order_id,
    customer_id,
    region,
    status,
    order_amount,
    event_ts,
    ingestion_ts,
    event_date,
    ingestion_date
from ranked
where latest_rank = 1
