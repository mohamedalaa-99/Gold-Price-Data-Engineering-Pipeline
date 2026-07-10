{{ config(materialized='table') }}

with currency_raw as (
    select distinct currency as currency_name
    from {{ ref('stg_gold_price') }}
)

select
    abs(hash(currency_name)) as currency_id,
    currency_name,
    case 
        when currency_name = 'USD' then '$'
        when currency_name = 'EGP' then 'E£'
        else 'Symbol'
    end as symbol
from currency_raw
where currency_name is not null