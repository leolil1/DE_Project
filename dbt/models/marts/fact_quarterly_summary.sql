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
        d.quarter,
        d.dateid,
        row_number() over (partition by d.quarter, d.year order by t.dateid, t.time_time asc) as row_number_open,
        row_number() over (partition by d.quarter, d.year order by t.dateid, t.time_time desc) as row_number_close
    from trade_source t
    join date_source d
    on t.dateid=d.dateid
),
first_open as(
    select
        open,
        year,
        quarter
    from base
    where row_number_open=1
),
last_close as (
    select
        close,
        year,
        quarter
    from base
    where row_number_close=1
),
min_max as (
    select
        max(high) as high,
        min(low) as low,
        sum(volume) as volume,
        year,
        quarter
    from base
    group by quarter,year
)

select 
  f.open,
  l.close,
  m.high,
  m.low,
  m.volume,
  concat(f.year,f.quarter) as YearQuarter
from first_open f
join last_close l on f.year = l.year and f.quarter = l.quarter
join min_max m on f.year = m.year and f.quarter = m.quarter
order by YearQuarter