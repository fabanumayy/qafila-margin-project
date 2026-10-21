{{ config(materialized='view') }}
select product_id, trim(sku) as sku, category from raw.products
