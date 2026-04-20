{{
  config(
    materialized = 'table',
    schema = 'silver'
  )
}}

/*
  Collateral-weighted aggregation per (user, protocol).

  An Aave user can have collateral in WETH, WBTC, and USDC simultaneously.
  Each asset has a different liquidation threshold.  The health factor
  numerator sums (collateral_usd_i × liq_threshold_i) across all assets.

  This model pre-aggregates that weighted sum so the health factor
  calculation in fct_health_factors is a simple division.

  Output: one row per (user_address, protocol, ingestion_date)
*/

with positions as (
    select * from {{ ref('int_positions_unified') }}
),

agg as (
    select
        user_address,
        protocol,
        ingestion_date,

        -- Total collateral at current oracle prices
        sum(collateral_usd)                                         as total_collateral_usd,

        -- Total debt at current oracle prices
        sum(debt_usd)                                               as total_debt_usd,

        -- Weighted liquidation threshold:
        --   Σ(collateral_usd_i × liq_threshold_i) / Σ(collateral_usd_i)
        --   Used as the effective threshold in the health factor formula.
        sum(collateral_usd * liquidation_threshold)
            / nullif(sum(collateral_usd), 0)                        as weighted_liq_threshold,

        -- Number of individual asset positions
        count(*)                                                    as position_count,

        -- Latest snapshot within the day
        max(snapshot_ts)                                            as snapshot_ts
    from positions
    group by
        user_address,
        protocol,
        ingestion_date
)

select * from agg
