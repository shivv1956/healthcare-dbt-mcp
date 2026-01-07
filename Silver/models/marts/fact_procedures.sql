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
        code as procedure_code,
        
        -- Facts/Measures
        coalesce(base_cost, 0) as base_cost
        
    from procedures
    where patient_id is not null
      and encounter_id is not null
)

select * from final
