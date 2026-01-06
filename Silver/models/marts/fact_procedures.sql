{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='procedure_fact_key'
) }}

with procedures as (
    select * from {{ ref('stg_procedures') }}
),

final as (
    select
        -- Surrogate key for the fact
        {{ dbt_utils.generate_surrogate_key(['patient_id', 'encounter_id', 'code', 'start_datetime']) }} as procedure_fact_key,
        
        -- Foreign keys to dimensions
        patient_id,
        encounter_id,
        {{ dbt_utils.generate_surrogate_key(['code', 'system']) }} as procedure_key,
        
        -- Degenerate dimensions (transaction details)
        start_datetime,
        stop_datetime,
        
        -- Descriptive attributes
        code,
        system,
        description,
        coalesce(reason_code, 'UNKNOWN') as reason_code,
        reason_description,
        
        -- Facts/Measures
        coalesce(base_cost, 0) as base_cost,
        
        -- Calculated measures
        case 
            when stop_datetime is not null 
            then datediff(minute, start_datetime, stop_datetime)
            else null
        end as procedure_duration_minutes
        
    from procedures
    where patient_id is not null
      and encounter_id is not null
)

select * from final
