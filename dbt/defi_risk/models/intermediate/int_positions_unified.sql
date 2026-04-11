{{
  config(
    materialized = 'view',
    schema = 'silver'
  )
}}

/*
  Unified cross-protocol position view.

  UNIONs all three staging models into a single schema for downstream
  aggregation.  One row = one position of a user in one asset on one protocol.

  This view is the single source of truth for all downstream models
  (int_collateral_weighted, fct_health_factors, etc.).
*/

with aave as (
    select * from {{ ref('stg_aave__positions') }}
),

compound as (
    select * from {{ ref('stg_compound__positions') }}
),

maker as (
    select * from {{ ref('stg_maker__vaults') }}
),

unified as (
    select * from aave
    union all
    select * from compound
    union all
    select * from maker
)

select * from unified
