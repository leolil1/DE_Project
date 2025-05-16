with source as (

    select * from {{ source('staging', 'bitcoin_data_all') }}
    where volume!=0
)

select 
    dateid,
    year,
    month,
    day,
    {{get_which_quarter(month)}} as quarter
from (select
    DISTINCT replace(time_date, '-', '') as dateid,
    substr(Time_date,0,4) as year,
    substr(Time_date,6,2) as month,
    substr(Time_date,9,2) as day,
from source)