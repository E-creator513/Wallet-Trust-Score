{{ config(
    materialized='incremental',
    unique_key='wallet'
) }}

select
    from_address as wallet,
    max(block_number) as last_block
from {{ ref('stg_transactions') }}
group by 1

{% if is_incremental() %}
having max(block_number) > (select max(last_block) from {{ this }})
{% endif %}
