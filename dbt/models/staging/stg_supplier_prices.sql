{{ config(materialized='view') }}

with parsed as (
    select
        source_file,
        trim(sku) as sku,
        cast(valid_from as date) as valid_from,
        coalesce(unit_cost, cost_sar) as raw_cost,
        case
          when unit_cost is not null then
            try_cast(translate(replace(trim(unit_cost), ',', '.'), '٠١٢٣٤٥٦٧٨٩٫', '0123456789.') as double)
          else try_cast(translate(replace(trim(cost_sar), ',', '.'), '٠١٢٣٤٥٦٧٨٩٫', '0123456789.') as double)
        end as unit_cost_sar
    from raw.supplier_prices
), ranked as (
    select *, row_number() over (partition by sku, valid_from order by source_file desc) as rn
    from parsed
)
select * from ranked
