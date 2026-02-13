select
    from_address,
    to_address,
    sum(value_eth) as total_sent,  -- fix column name
    count(*) as tx_count
from {{ source('ethereum', 'transactions') }}
group by from_address, to_address
