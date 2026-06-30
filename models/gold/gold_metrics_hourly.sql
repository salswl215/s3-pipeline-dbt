-- Hourly aggregate per device+parameter+measurement. Same shape as gold_metrics_daily
-- but bucketed to the hour via date_trunc('hour', ts). Reads from silver_metrics (daily
-- has already aggregated the hour away). Each hour bucket is fully contained in a `dt`
-- partition, so the daily-recompute incremental logic stays correct for min/max.
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','ts_hour'],
    partitioned_by=['dt']
) }}

select
  measurement, cdevice, pdevice, parameter,
  date_trunc('hour', ts) as ts_hour,
  dt,
  avg(value)  as avg_value,
  min(value)  as min_value,
  max(value)  as max_value,
  count(*)    as sample_count
from {{ ref('silver_metrics') }}
{% if is_incremental() %}
where dt >= (select date_add('day', -{{ var('lookback_days', 3) }}, max(dt)) from {{ this }})
{% endif %}
group by 1, 2, 3, 4, 5, 6
