with trade_source as (
    select * from {{ ref('stg__bitcoin_data_all') }}
),
base as (
    select
        open,
        high,
        low,
        close,
        volume,
        dateid,
        row_number() over (partition by dateid order by time_time asc) as row_number_open,
        row_number() over (partition by dateid order by time_time desc) as row_number_close
    from trade_source
),
first_open as (
    select
        open,
        dateid
    from base
    where row_number_open=1
),
last_close as (
    select
        close,
        dateid
    from base
    where row_number_close=1
),
min_max as (
    select
        max(high) as high,
        min(low) as low,
        dateid,
        sum(volume) as volume,
    from base
    group by dateid
)

select 
  f.open,
  l.close,
  m.high,
  m.low,
  m.volume,
  f.dateid
from first_open f
join last_close l on f.dateid = l.dateid
join min_max m on f.dateid = m.dateid
order by dateid