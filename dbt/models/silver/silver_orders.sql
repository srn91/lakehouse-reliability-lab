with deduped as (
    select
        *,
        row_number() over (
            partition by event_id
            order by ingestion_ts desc, event_ts desc
        ) as dedupe_rank
    from {{ ref('bronze_orders') }}
    where event_id is not null
      and order_id is not null
      and customer_id is not null
      and region is not null
      and status is not null
)

select
    event_id,
    order_id,
    customer_id,
    region,
    status,
    order_amount,
    event_ts,
    ingestion_ts,
    cast(event_ts as date) as event_date,
    cast(ingestion_ts as date) as ingestion_date,
    source_file
from deduped
where dedupe_rank = 1
