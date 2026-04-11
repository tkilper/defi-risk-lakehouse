{{
  config(
    materialized = 'table',
    schema = 'gold',
    on_table_exists = 'replace'
  )
}}

/*
  fct_liquidation_risk
  ====================
  Protocol-level liquidation risk summary — answers the question:
  "How much collateral and debt is at risk RIGHT NOW at current prices?"

  Aggregated by (protocol, risk_tier, ingestion_date) so a downstream
  dashboard can show risk distribution across protocols.

  Metrics
  -------
  - position_count          — number of accounts in each risk bucket
  - total_debt_at_risk_usd  — sum of debt USD for at-risk positions
  - total_collateral_usd    — sum of collateral USD for at-risk positions
  - avg_health_factor       — mean HF within the bucket
  - min_health_factor       — worst HF in the bucket
  - pct_of_protocol_debt    — fraction of the protocol's total debt in this bucket
*/

with hf as (
    select * from {{ ref('fct_health_factors') }}
),

protocol_totals as (
    select
        protocol,
        ingestion_date,
        sum(total_debt_usd) as protocol_total_debt_usd
    from hf
    group by protocol, ingestion_date
),

risk_buckets as (
    select
        hf.protocol,
        hf.risk_tier,
        hf.ingestion_date,

        count(*)                                                    as position_count,
        sum(hf.total_debt_usd)                                     as total_debt_at_risk_usd,
        sum(hf.total_collateral_usd)                               as total_collateral_usd,
        avg(hf.health_factor)                                      as avg_health_factor,
        min(hf.health_factor)                                      as min_health_factor,
        max(hf.health_factor)                                      as max_health_factor,
        -- Collateral buffer available across this bucket
        sum(hf.collateral_buffer_usd)                              as total_collateral_buffer_usd
    from hf
    group by
        hf.protocol,
        hf.risk_tier,
        hf.ingestion_date
)

select
    rb.protocol,
    rb.risk_tier,
    rb.ingestion_date,
    rb.position_count,
    rb.total_debt_at_risk_usd,
    rb.total_collateral_usd,
    round(rb.avg_health_factor, 4)                                  as avg_health_factor,
    round(rb.min_health_factor, 4)                                  as min_health_factor,
    round(rb.max_health_factor, 4)                                  as max_health_factor,
    rb.total_collateral_buffer_usd,
    -- Percentage of protocol's total debt sitting in this risk tier
    round(
        100.0 * rb.total_debt_at_risk_usd
        / nullif(pt.protocol_total_debt_usd, 0),
    2)                                                              as pct_of_protocol_debt
from risk_buckets rb
left join protocol_totals pt
    on rb.protocol = pt.protocol
    and rb.ingestion_date = pt.ingestion_date
order by
    rb.ingestion_date desc,
    rb.protocol,
    case rb.risk_tier
        when 'LIQUIDATABLE' then 1
        when 'CRITICAL'     then 2
        when 'AT_RISK'      then 3
        when 'WATCH'        then 4
        else 5
    end
