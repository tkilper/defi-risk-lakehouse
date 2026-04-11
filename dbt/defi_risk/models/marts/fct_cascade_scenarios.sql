{{
  config(
    materialized = 'table',
    schema = 'gold',
    on_table_exists = 'replace'
  )
}}

/*
  fct_cascade_scenarios
  =====================
  Simulates the impact of ETH (and correlated asset) price shocks on
  protocol solvency.  Models a liquidation cascade for five scenarios.

  Methodology
  -----------
  For each scenario we apply a uniform price shock to all collateral USD
  values, then recompute the health factor:

    shocked_HF = (collateral_usd × (1 - shock) × weighted_liq_threshold) / debt_usd

  NOTE: In production you would apply per-asset ETH-correlation weights
  (e.g. WBTC might drop 70% as much as ETH; stablecoins 0%).  This model
  uses a uniform shock for clarity.  The seed table `price_shock_scenarios`
  defines the scenarios.

  Cascade interpretation
  ----------------------
  The first wave of liquidations (Wave 1) forces liquidators to sell
  collateral into thin markets, which can push prices down further and
  trigger Wave 2 liquidations.  This model quantifies Wave 1 exposure.

  Key metrics per (protocol, scenario)
  ─────────────────────────────────────
  - positions_liquidatable     — positions where shocked HF < 1.0
  - debt_at_risk_usd           — total debt in liquidatable positions
  - collateral_to_be_seized_usd— at liquidation bonus, collateral sold
  - protocol_insolvency_usd    — debt not covered by seized collateral
                                 (positive = insolvency risk)
  - pct_positions_liquidatable — share of positions affected
  - pct_debt_liquidatable      — share of protocol debt at risk
*/

with base as (
    select *
    from {{ ref('int_collateral_weighted') }}
    where
        total_debt_usd > 0
        and weighted_liq_threshold is not null
        and weighted_liq_threshold > 0
),

-- Price shock scenarios (loaded from seed file)
scenarios as (
    select *
    from {{ ref('price_shock_scenarios') }}
),

-- Cross-join each position with each scenario and compute shocked metrics
shocked as (
    select
        b.user_address,
        b.protocol,
        b.ingestion_date,
        b.total_collateral_usd,
        b.total_debt_usd,
        b.weighted_liq_threshold,

        s.scenario_id,
        s.scenario_name,
        s.eth_price_shock_pct,

        -- Shocked collateral value
        b.total_collateral_usd * (1.0 - s.eth_price_shock_pct)         as shocked_collateral_usd,

        -- Shocked health factor
        (
            b.total_collateral_usd
            * (1.0 - s.eth_price_shock_pct)
            * b.weighted_liq_threshold
        ) / b.total_debt_usd                                            as shocked_health_factor,

        -- Is this position liquidatable under the shock?
        case
            when (
                b.total_collateral_usd
                * (1.0 - s.eth_price_shock_pct)
                * b.weighted_liq_threshold
            ) / b.total_debt_usd < 1.0
            then true else false
        end                                                             as is_liquidatable

    from base b
    cross join scenarios s
),

aggregated as (
    select
        protocol,
        scenario_id,
        scenario_name,
        eth_price_shock_pct,
        ingestion_date,

        -- Position counts
        count(*)                                                         as total_positions,
        count(*) filter (where is_liquidatable)                          as positions_liquidatable,

        -- USD exposure
        sum(total_debt_usd)                                              as total_protocol_debt_usd,
        sum(case when is_liquidatable then total_debt_usd else 0 end)   as debt_at_risk_usd,
        sum(case when is_liquidatable then shocked_collateral_usd else 0 end)
                                                                         as shocked_collateral_seized_usd,

        -- Insolvency gap: if liquidation proceeds < debt, protocol absorbs the loss
        sum(case when is_liquidatable
            then greatest(total_debt_usd - shocked_collateral_usd, 0)
            else 0 end)                                                  as protocol_insolvency_usd,

        -- Percentages
        round(
            100.0 * count(*) filter (where is_liquidatable) / nullif(count(*), 0),
        2)                                                               as pct_positions_liquidatable,
        round(
            100.0
            * sum(case when is_liquidatable then total_debt_usd else 0 end)
            / nullif(sum(total_debt_usd), 0),
        2)                                                               as pct_debt_liquidatable,

        -- HF statistics under shock
        round(avg(shocked_health_factor), 4)                             as avg_shocked_health_factor,
        round(min(shocked_health_factor), 4)                             as min_shocked_health_factor

    from shocked
    group by
        protocol,
        scenario_id,
        scenario_name,
        eth_price_shock_pct,
        ingestion_date
)

select * from aggregated
order by
    ingestion_date desc,
    protocol,
    scenario_id
