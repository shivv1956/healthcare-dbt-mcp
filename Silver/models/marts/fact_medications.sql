{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='medication_fact_key'
) }}

with medications as (
    select * from {{ ref('stg_medications') }}
),

final as (
    select
        -- Surrogate key for the fact
        {{ dbt_utils.generate_surrogate_key(['patient_id', 'encounter_id', 'medication_code', 'start_datetime']) }} as medication_fact_key,
        
        -- Foreign keys to dimensions
        patient_id,
        encounter_id,
        payer_id,
        medication_code,
        
        -- Facts/Measures
        coalesce(totalcost, 0) as total_cost
        
    from medications
    where patient_id is not null
      and encounter_id is not null
)

select * from final
