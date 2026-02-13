select
    wallet,
    tx_count,
    total_sent,
    extract(epoch from (last_tx_time - first_tx_time)) / 86400 as wallet_age_days,
    tx_count / nullif(extract(epoch from (last_tx_time - first_tx_time)) / 86400, 0) as tx_frequency
from {{ ref('dwh_wallet_metrics') }}
