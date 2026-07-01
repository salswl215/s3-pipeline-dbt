{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','event_day'],
    partitioned_by=['day(event_day)']
) }}

select
  measurement, cdevice, pdevice, parameter,
  date_trunc('day', ts) as event_day,
  avg(value)  as avg_value,
  min(value)  as min_value,
  max(value)  as max_value,
  count(*)    as sample_count
from {{ ref('silver_metrics') }}
{% if is_incremental() %}
  where ts >= (select date_trunc('day', max(event_day) - interval '1' day) from {{ this }})
{% endif %}
group by 1, 2, 3, 4, 5
