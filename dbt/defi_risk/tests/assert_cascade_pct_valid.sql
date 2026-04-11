/*
Custom dbt test: assert_cascade_pct_valid
=========================================
Fails if pct_positions_liquidatable or pct_debt_liquidatable is outside
the valid range [0, 100].

These metrics are percentages; values outside this range indicate an
aggregation error (e.g. division by zero returning infinity, or a
negative count).
*/

select
    protocol,
    scenario_name,
    eth_price_shock_pct,
    pct_positions_liquidatable,
    pct_debt_liquidatable
from {{ ref('fct_cascade_scenarios') }}
where
    pct_positions_liquidatable < 0
    or pct_positions_liquidatable > 100
    or pct_debt_liquidatable < 0
    or pct_debt_liquidatable > 100
