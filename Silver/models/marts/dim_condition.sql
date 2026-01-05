{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='condition_key'
) }}

with conditions_source as (
    select 
        code,
        system,
        description,
        row_number() over (partition by code, system order by code) as rn
    from {{ ref('stg_conditions') }}
),

unique_conditions as (
    select * from conditions_source where rn = 1
),

condition_metrics as (
    select
        code,
        system,
        count(distinct patient_id) as patient_count,
        count(distinct encounter_id) as encounter_count,
        avg(datediff(day, start_date, coalesce(stop_date,  dateadd('day', 1, start_date)))) as avg_duration_days
    from {{ ref('stg_conditions') }}
    where code is not null
    group by code, system
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.code', 'c.system']) }} as condition_key,
        
        -- Condition information
        c.code,
        c.description,
        c.system,
        
        -- Metrics
        coalesce(m.patient_count, 0) as patient_count,
        coalesce(m.encounter_count, 0) as encounter_count,
        coalesce(m.avg_duration_days, 0) as avg_duration_days
        
    from unique_conditions c
    left join condition_metrics m on c.code = m.code and c.system = m.system
)

select * from final
