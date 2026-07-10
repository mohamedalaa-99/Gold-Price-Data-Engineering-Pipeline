

with stg_data as (
    select * from `databricks_workspace`.`gold_layer`.`stg_gold_price`
),

dim_date as (
    select * from `databricks_workspace`.`gold_layer`.`dim_date`
),

dim_time as (
    select * from `databricks_workspace`.`gold_layer`.`dim_time`
),

dim_currency as (
    select * from `databricks_workspace`.`gold_layer`.`dim_currency`
)

select
    d.date_id,
    t.time_id,
    c.currency_id,
    s.price_usd as price,
    1.0 as exchangeRate 
from stg_data s
left join dim_date d on s.event_date = d.full_date
left join dim_time t on hour(s.ingestion_timestamp) = t.hour 
                     and minute(s.ingestion_timestamp) = t.minute 
                     and second(s.ingestion_timestamp) = t.second
left join dim_currency c on s.currency = c.currency_name