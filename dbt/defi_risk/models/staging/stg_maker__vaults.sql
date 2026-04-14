{{
  config(
    materialized = 'view',
    schema = 'silver'
  )
}}

/*
  Staging model for MakerDAO vaults (CDPs).

  MakerDAO uses a different risk model than Aave/Compound:
    - Each vault has a single collateral type (ETH-A, WBTC-B, etc.)
    - Debt is denominated in DAI (≈ $1 USD)
    - liquidation_threshold here = 1 / liquidationRatio, making it
      mathematically equivalent to the Aave threshold for HF computation

  MakerDAO vaults have a 1:1 relationship between owner and collateral type
  per vault (cdpId), so no deduplication on (user, reserve) is needed —
  we deduplicate on (reserve_address, ingestion_date) to handle re-ingestion.
*/

with source as (
    select *
    from {{ source('silver', 'maker_vaults') }}
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
        cast(null as double)  as ltv,               -- MakerDAO has no LTV concept
        cast(null as double)  as liquidation_bonus,

        price_usd             as asset_price_usd,

        collateral_human      as collateral_balance,
        cast(null as double)  as variable_debt_balance,
        cast(null as double)  as stable_debt_balance,
        collateral_usd,
        debt_usd,

        true                  as is_collateral_enabled,  -- always true for vaults

        ingestion_date,
        ingestion_ts          as snapshot_ts
    from deduped
    where
        collateral_usd > 0
        and debt_usd > 0
)

select * from final
