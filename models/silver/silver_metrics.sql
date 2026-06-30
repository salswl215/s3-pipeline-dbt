{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','ts_ns'],
    partitioned_by=['dt']
) }}

with src as (
  select cdevice, pdevice, parameter, value, ts, ts_ns, measurement, dt
  from {{ source('bronze','metrics') }}
  where 1 = 1
  {% if is_incremental() %}
    and dt >= (select date_add('day', -{{ var('lookback_days', 3) }}, max(dt)) from {{ this }})
  {% endif %}
  {% if var('start_date', none) is not none %}
    -- floor for the first (full-refresh) seed run: bound the bronze scan to dt >= start_date
    and dt >= date '{{ var('start_date') }}'
  {% endif %}
),
ranked as (
  select *,
    row_number() over (
      partition by measurement, cdevice, pdevice, parameter, ts_ns
      order by ts
    ) as rn
  from src
)
select cdevice, pdevice, parameter, value, ts, ts_ns, measurement, dt
from ranked
where rn = 1
