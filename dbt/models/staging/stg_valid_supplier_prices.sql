{{ config(materialized='view') }}
select p.sku, s.unit_cost_sar, s.valid_from
from {{ ref('stg_supplier_prices') }} s
join {{ ref('stg_products') }} p using (sku)
where s.rn = 1 and s.unit_cost_sar > 0
