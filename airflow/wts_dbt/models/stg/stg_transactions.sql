{{ config(materialized='view') }}

with raw_tx as (
    select
        hash as tx_hash,
        block_number,
        from_address,
        to_address,
        value_eth as value,
        gas as gas_used,
        gas_price,
        fetched_at as input_data
    from {{ source('analytics', 'transactions') }}
)
select * from raw_tx
