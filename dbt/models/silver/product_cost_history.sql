{{ config(materialized='table') }}
with changes as (
  select sku, round(unit_cost_sar, 2)::double as unit_cost_sar, valid_from,
         lag(round(unit_cost_sar, 2)::double) over (partition by sku order by valid_from) as prior_cost
  from {{ ref('stg_valid_supplier_prices') }}
), versions as (
  select sku, unit_cost_sar, valid_from,
         lead(valid_from) over (partition by sku order by valid_from) as valid_to
  from changes
  where prior_cost is null or unit_cost_sar <> prior_cost
)
select sku, unit_cost_sar, valid_from, valid_to, valid_to is null as is_current
from versions
