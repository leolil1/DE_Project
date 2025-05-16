with trade_source as (
    select * from {{ ref('stg__bitcoin_data_all') }}
),
date_source as (
    select * from {{ ref('stg__date_data') }}
),
base as (
    select 
        t.open,
        t.high,
        t.low,
        t.close,
        t.volume,
        d.month,
        d.year,
        d.dateid,
        row_number() over (partition by d.month, d.year order by t.dateid, t.time_time asc) as row_number_open,
        row_number() over (partition by d.month, d.year order by t.dateid, t.time_time desc) as row_number_close
    from trade_source t
    join date_source d
    on t.dateid=d.dateid
),
first_open as(
    select
        open,
        month,
        year
    from base
    where row_number_open=1
),
last_close as (
    select
        close,
        month,
        year
    from base
    where row_number_close=1
),
min_max as (
    select
        max(high) as high,
        min(low) as low,
        sum(volume) as volume,
        month,
        year
    from base
    group by month,year
)

select 
  f.open,
  l.close,
  m.high,
  m.low,
  m.volume,
  concat(f.year,f.month) as YearMonth
from first_open f
join last_close l on f.year = l.year and f.month = l.month
join min_max m on f.year = m.year and f.month = m.month
order by YearMonth