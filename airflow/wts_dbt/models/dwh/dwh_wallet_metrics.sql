{{ config(
    materialized='incremental',
    unique_key='wallet'
) }}

with tx as (
    select
        from_address as wallet,
        sum(value_eth) as total_sent,
        max(fetched_at) as last_tx_time
    from {{ source('analytics', 'transactions') }}
    where from_address is not null
    group by from_address
)

select *
from tx
{% if is_incremental() %}
where last_tx_time > (
    select coalesce(max(last_tx_time), '1970-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
