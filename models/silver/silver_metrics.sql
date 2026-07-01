{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','ts_ns'],
    partitioned_by=['day(ts)']
) }}

with src as (
  select b.cdevice, b.pdevice, b.parameter, b.value, b.ts, b.ts_ns, b.measurement
  from {{ source('bronze','metrics') }} b
  {% if is_incremental() %}
  cross join (select max(ts) as max_ts from {{ this }}) w
  {% endif %}
  where 1 = 1
  {% if is_incremental() %}
    and b.ts >= w.max_ts - interval '{{ var('lookback_minutes', 60) }}' minute
  {% endif %}
  {% if var('start_date', none) is not none %}
    and b.ts >= timestamp '{{ var('start_date') }} 00:00:00 UTC'
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
select cdevice, pdevice, parameter, value, ts, ts_ns, measurement
from ranked
where rn = 1
