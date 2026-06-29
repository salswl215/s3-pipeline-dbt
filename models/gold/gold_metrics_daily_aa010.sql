-- Example: a measurement-scoped gold mart carved from the shared gold_metrics_daily.
-- Filters one measurement ('AA010') into its own Iceberg table, reusing the daily
-- aggregate already computed upstream (DRY). Same grain/key as gold_metrics_daily.
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','dt'],
    partitioned_by=['dt']
) }}

select
  measurement, cdevice, pdevice, parameter, dt,
  avg_value, min_value, max_value, sample_count
from {{ ref('gold_metrics_daily') }}
where measurement = 'AA010'
{% if is_incremental() %}
  and dt >= (select date_add('day', -{{ var('lookback_days', 3) }}, max(dt)) from {{ this }})
{% endif %}
