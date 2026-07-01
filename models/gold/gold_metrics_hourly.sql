-- Hourly aggregate per device+parameter+measurement. ts_hour = date_trunc('hour', ts) is the
-- hour grain; partitioned via day(ts_hour) (hidden partitioning) so reads filtering on ts_hour
-- prune day partitions. Reads from silver_metrics (daily/hourly grain isn't in silver).
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','ts_hour'],
    partitioned_by=['day(ts_hour)']
) }}

select
  measurement, cdevice, pdevice, parameter,
  date_trunc('hour', ts) as ts_hour,
  avg(value)  as avg_value,
  min(value)  as min_value,
  max(value)  as max_value,
  count(*)    as sample_count
from {{ ref('silver_metrics') }}
{% if is_incremental() %}
where ts >= (select date_trunc('hour', max(ts_hour) - interval '{{ var('lookback_days', 1) }}' minute) from {{ this }})
{% endif %}
group by 1, 2, 3, 4, 5
