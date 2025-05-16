with source as (

    select * from {{ source('staging', 'bitcoin_data_all') }}
    where volume!=0
)

select
    {{ dbt_utils.generate_surrogate_key(['time_date', 'time_time']) }} as tradeid,
    open,
    high,
    low,
    close,
    round(volume,4) as volume,
    replace(time_date, '-', '') as dateid,
    time_time
    
from source