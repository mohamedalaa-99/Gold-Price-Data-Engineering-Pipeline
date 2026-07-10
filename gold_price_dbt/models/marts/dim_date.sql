{{ config(materialized='table') }}

with date_raw as (
    select distinct event_date as full_date
    from {{ ref('stg_gold_price') }} 
)

select
    cast(date_format(full_date, 'yyyyMMdd') as int) as date_id,
    full_date,
    day(full_date) as day,
    month(full_date) as month,
    year(full_date) as year
from date_raw
where full_date is not null