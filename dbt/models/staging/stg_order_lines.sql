{{ config(materialized='view') }}
select
  i.order_id, i.line_no, i.product_id, i.quantity, i.unit_price, i.discount,
  o.status, ((cast(o.created_at as timestamp) + interval 3 hour)::date) as order_date,
  extract(isoyear from (cast(o.created_at as timestamp) + interval 3 hour))::int as iso_year,
  extract(week from (cast(o.created_at as timestamp) + interval 3 hour))::int as iso_week,
  trim(s.city) as city, trim(p.category) as category, trim(p.sku) as sku
from raw.order_items i
join raw.orders o using (order_id)
join raw.stores s using (store_id)
join raw.products p using (product_id)
