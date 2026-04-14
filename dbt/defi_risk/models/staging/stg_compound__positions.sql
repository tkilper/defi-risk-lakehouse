{{
  config(
    materialized = 'view',
    schema = 'silver'
  )
}}

/*
  Staging model for Compound V3 positions.

  Compound V3 uses an isolated market model — each market (USDC, ETH) is
  separate.  Collateral is aggregated across all collateral tokens in the
  Spark silver job; here we just rename and filter.
*/

with source as (
    select *
    from {{ source('silver', 'compound_positions') }}
),

ranked as (
    select *,
        row_number() over (
            partition by user_address, reserve_address, ingestion_date
            order by ingestion_ts desc
        ) as _rn
    from source
),

deduped as (
    select * from ranked where _rn = 1
),

final as (
    select
        user_address,
        reserve_address,
        symbol                as asset_symbol,
        decimals              as asset_decimals,
        protocol,

        liquidation_threshold,
        ltv,
        liquidation_bonus,

        price_usd             as asset_price_usd,

        collateral_human      as collateral_balance,
        variable_debt_human   as variable_debt_balance,
        stable_debt_human     as stable_debt_balance,
        collateral_usd,
        debt_usd,

        is_collateral_enabled,

        ingestion_date,
        ingestion_ts          as snapshot_ts
    from deduped
    where
        collateral_usd > 0
        and debt_usd > 0
)

select * from final
