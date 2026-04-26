select
  source_file,
  event_id,
  order_id,
  customer_id,
  lower(region) as region,
  lower(status) as status,
  {{ as_decimal_money("order_amount") }} as order_amount,
  cast(event_ts as timestamp) as event_ts,
  cast(ingestion_ts as timestamp) as ingestion_ts
from {{ source('raw', 'order_events') }}
