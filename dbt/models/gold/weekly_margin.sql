{{ config(materialized='table') }}
with eligible as (
  select *, round(quantity * unit_price - discount, 2)::double as line_revenue
  from {{ ref('stg_order_lines') }}
  where status = 'delivered' and quantity > 0 and order_date >= date '2026-08-31'
), costed as (
  select e.*, h.unit_cost_sar,
         e.quantity * h.unit_cost_sar as line_cost
  from eligible e
  left join {{ ref('product_cost_history') }} h
    on h.sku = e.sku and e.order_date >= h.valid_from
   and (h.valid_to is null or e.order_date < h.valid_to)
)
select iso_week, city, category,
       count(*)::bigint as lines,
       round(sum(line_revenue), 2)::double as revenue_sar,
       round(sum(case when unit_cost_sar is not null then line_revenue else 0 end), 2)::double as costed_revenue_sar,
       round(sum(case when unit_cost_sar is not null then line_cost else 0 end), 2)::double as cost_sar,
       round(sum(case when unit_cost_sar is not null then line_revenue - line_cost else 0 end), 2)::double as margin_sar,
       sum(case when unit_cost_sar is null then 1 else 0 end)::bigint as uncosted_lines
from costed
group by iso_week, city, category
