{{
  config(
    materialized = 'table',
    schema = 'gold',
    on_table_exists = 'replace'
  )
}}

/*
  fct_health_factors
  ==================
  Current health factor for every borrower across all three protocols.

  Health Factor formula
  ---------------------
    HF = Σ(collateral_usd_i × liq_threshold_i) / Σ(debt_usd_j)
       = (total_collateral_usd × weighted_liq_threshold) / total_debt_usd

  A position is LIQUIDATABLE when HF < 1.0.
  A position is AT RISK when 1.0 ≤ HF < 1.1 (common risk threshold).

  Collateral buffer
  -----------------
  The "buffer" is how far (in USD) collateral can drop before the position
  becomes liquidatable:
    buffer = total_collateral_usd - (total_debt_usd / weighted_liq_threshold)

  Percentage drop to liquidation
  --------------------------------
    pct_drop = 1 - (total_debt_usd / (total_collateral_usd × weighted_liq_threshold))
*/

with base as (
    select *
    from {{ ref('int_collateral_weighted') }}
    where
        total_debt_usd > 0
        and weighted_liq_threshold is not null
        and weighted_liq_threshold > 0
)

select
    -- Identity
    user_address,
    protocol,
    ingestion_date,
    snapshot_ts,

    -- Portfolio summary
    total_collateral_usd,
    total_debt_usd,
    weighted_liq_threshold,
    position_count,

    -- Health factor
    (total_collateral_usd * weighted_liq_threshold) / total_debt_usd   as health_factor,

    -- Risk tier
    case
        when (total_collateral_usd * weighted_liq_threshold) / total_debt_usd < 1.0
            then 'LIQUIDATABLE'
        when (total_collateral_usd * weighted_liq_threshold) / total_debt_usd < 1.05
            then 'CRITICAL'
        when (total_collateral_usd * weighted_liq_threshold) / total_debt_usd < 1.10
            then 'AT_RISK'
        when (total_collateral_usd * weighted_liq_threshold) / total_debt_usd < 1.25
            then 'WATCH'
        else 'HEALTHY'
    end                                                                  as risk_tier,

    -- Collateral buffer (USD)
    total_collateral_usd
        - (total_debt_usd / weighted_liq_threshold)                     as collateral_buffer_usd,

    -- How much collateral can drop (%) before liquidation
    case
        when total_collateral_usd > 0
        then 1.0 - (total_debt_usd / (total_collateral_usd * weighted_liq_threshold))
    end                                                                  as pct_drop_to_liquidation

from base
