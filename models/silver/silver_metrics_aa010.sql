-- Example: a measurement-scoped silver mart carved from the shared silver_metrics.
-- Demonstrates filtering one measurement ('AA010') into its own Iceberg table while
-- reusing the dedup/typing already done upstream (DRY). Same natural key as silver_metrics.
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','ts_ns'],
    partitioned_by=['dt']
) }}

select cdevice, pdevice, parameter, value, ts, ts_ns, measurement, dt
from {{ ref('silver_metrics') }}
where measurement = 'AA010'
{% if is_incremental() %}
  and dt >= (select date_add('day', -{{ var('lookback_days', 3) }}, max(dt)) from {{ this }})
{% endif %}
