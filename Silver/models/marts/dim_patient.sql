{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='patient_id'
) }}

with patients_source as (
    select * from {{ ref('stg_patients') }}
),

final as (
    select
        -- Surrogate key (already generated in staging)
        patient_id,
        
        -- Patient demographics
        birth_date,
        death_date,
        first_name,
        last_name,
        gender,
        race,
        ethnicity,
        marital_status,
        
        -- Address information
        city,
        state,
        zip_code
        
    from patients_source
)

select * from final
