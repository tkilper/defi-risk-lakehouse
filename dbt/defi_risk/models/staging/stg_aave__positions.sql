{{
  config(
    materialized = 'view',
    schema = 'silver'
  )
}}

/*
  Staging model for Aave V3 positions.

  Reads from the silver Iceberg table written by the Spark transformer and
  applies:
    - Column renames for consistency across protocols
    - A derived ``current_debt_usd`` = variable_debt_usd + stable_debt_usd
    - Filtering to only collateral-enabled positions
      (non-collateral deposits don't protect borrows in the same account)
    - Deduplication: if the same user+reserve was fetched multiple times
      in the same day, keep the most recent record.
*/

with source as (
    select *
    from {{ source('silver', 'aave_positions') }}
),

deduped as (
    select *
    from source
    qualify
        row_number() over (
            partition by user_address, reserve_address, ingestion_date
            order by ingestion_ts desc
        ) = 1
),

final as (
    select
        -- Identity
        user_address,
        reserve_address,
        symbol                                            as asset_symbol,
        decimals                                          as asset_decimals,
        protocol,

        -- Risk parameters
        liquidation_threshold,
        ltv,
        liquidation_bonus,

        -- Prices
        price_usd                                         as asset_price_usd,

        -- Balances
        collateral_human                                  as collateral_balance,
        variable_debt_human                               as variable_debt_balance,
        stable_debt_human                                 as stable_debt_balance,
        collateral_usd,
        debt_usd,

        -- Flags
        is_collateral_enabled,

        -- Metadata
        ingestion_date,
        ingestion_ts                                      as snapshot_ts
    from deduped
    where
        -- Only count collateral that the user has actually enabled
        is_collateral_enabled = true
        and collateral_usd > 0
        and debt_usd > 0
)

select * from final
