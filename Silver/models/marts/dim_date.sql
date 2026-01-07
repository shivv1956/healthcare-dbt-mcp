{{ config(
    materialized='table'
) }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    )
    }}
),

final as (
    select
        to_varchar(date_day, 'YYYYMMDD')::integer as date_key,
        date_day as date,
        year(date_day) as year,
        month(date_day) as month,
        quarter(date_day) as quarter,
        monthname(date_day) as month_name,
        dayname(date_day) as day_name
    from date_spine
)

select * from final
