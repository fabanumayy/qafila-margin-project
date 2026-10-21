{{ config(materialized='table') }}
select source_file, sku, valid_from, raw_cost,
       case when p.sku is null then 'unknown_sku' else 'invalid_cost' end as reason
from {{ ref('stg_supplier_prices') }} s
left join {{ ref('stg_products') }} p using (sku)
where p.sku is null or s.unit_cost_sar is null or s.unit_cost_sar <= 0
