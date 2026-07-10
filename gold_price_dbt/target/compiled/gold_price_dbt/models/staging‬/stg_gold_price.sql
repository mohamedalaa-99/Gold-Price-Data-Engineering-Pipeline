

with raw_silver as (
    select * from databricks_workspace.default.silver_gold_prices
)

select
    event_date,
    gold_price as price_usd,
    currency,
    source,
    ingestion_type,
    ingestion_timestamp
from raw_silver