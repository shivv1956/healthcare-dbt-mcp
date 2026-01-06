{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='condition_fact_key'
) }}

with conditions as (
    select * from {{ ref('stg_conditions') }}
),

final as (
    select
        -- Surrogate key for the fact
        {{ dbt_utils.generate_surrogate_key(['patient_id', 'encounter_id', 'code', 'start_date']) }} as condition_fact_key,
        
        -- Foreign keys to dimensions
        patient_id,
        encounter_id,
        {{ dbt_utils.generate_surrogate_key(['code', 'system']) }} as condition_key,
        
        -- Degenerate dimensions (transaction details)
        start_date,
        stop_date,
        
        -- Descriptive attributes
        code,
        system,
        description,
        
        -- Calculated measures
        case 
            when stop_date is not null 
            then datediff(day, start_date, stop_date)
            else null
        end as condition_duration_days
        
    from conditions
    where patient_id is not null
      and encounter_id is not null
)

select * from final
