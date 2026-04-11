/*
Custom dbt test: assert_health_factor_positive
==============================================
Fails if any computed health factor is negative.

A negative health factor is mathematically impossible under normal
circumstances (collateral_usd and debt_usd are both non-negative).
If this test fails, it indicates a data quality issue upstream:
  - Negative collateral or debt values from the GraphQL response
  - A bug in the Spark silver transformer's unit conversion
  - Unexpected NULL handling in int_collateral_weighted
*/

select
    user_address,
    protocol,
    ingestion_date,
    health_factor,
    total_collateral_usd,
    total_debt_usd
from {{ ref('fct_health_factors') }}
where health_factor < 0
