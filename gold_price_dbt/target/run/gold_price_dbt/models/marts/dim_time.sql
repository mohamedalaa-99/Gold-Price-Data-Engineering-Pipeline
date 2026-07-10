
  
    
        create or replace table `databricks_workspace`.`gold_layer`.`dim_time`
      
      
    using delta
  
      
      
      
      
      
      
      
      as
      with time_raw as (
    select distinct ingestion_timestamp as full_time
    from `databricks_workspace`.`gold_layer`.`stg_gold_price`
)

select
    cast(date_format(full_time, 'HHmmss') as int) as time_id,
    hour(full_time) as hour,
    minute(full_time) as minute,
    second(full_time) as second,
    date_format(full_time, 'HH:mm:ss') as full_time
from time_raw
where full_time is not null
  