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
  {% if is_incremental() %}
  where dt >= (select date_add('day', -{{ var('lookback_days', 3) }}, max(dt)) from {{ this }})
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
