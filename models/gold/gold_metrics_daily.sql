{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','dt'],
    partitioned_by=['dt']
) }}

select
  measurement, cdevice, pdevice, parameter, dt,
  avg(value)  as avg_value,
  min(value)  as min_value,
  max(value)  as max_value,
  count(*)    as sample_count
from {{ ref('silver_metrics') }}
{% if is_incremental() %}
where dt >= (select date_add('day', -{{ var('lookback_days', 3) }}, max(dt)) from {{ this }})
{% endif %}
group by 1,2,3,4,5
