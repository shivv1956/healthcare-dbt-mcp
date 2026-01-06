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
        medication_code as medication_key,
        
        -- Degenerate dimensions (transaction details)
        start_datetime,
        stop_datetime,
        
        -- Facts/Measures
        coalesce(totalcost, 0) as total_cost,
        coalesce(base_cost, 0) as base_cost,
        coalesce(payer_coverage, 0) as payer_coverage,
        coalesce(dispenses, 0) as dispenses,
        coalesce(reasoncode, 'UNKNOWN') as reason_code,
        reasondescription as reason_description
        
    from medications
    where patient_id is not null
      and encounter_id is not null
)

select * from final
