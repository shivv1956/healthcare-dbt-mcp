{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='claim_id'
) }}

with claims as (
    select * from {{ ref('stg_claims') }}
),

final as (
    select
        -- Primary key
        claim_id,
        
        -- Foreign keys to dimensions
        patient_id,
        provider_id,
        
        -- Degenerate dimensions (claim details)
        service_date,
        appointment_id,
        
        -- Status attributes
        coalesce(status_primary, 'UNKNOWN') as status_primary,
        coalesce(status_secondary, 'UNKNOWN') as status_secondary,
        coalesce(status_patient, 'UNKNOWN') as status_patient,
        
        -- Facts/Measures - Financial
        coalesce(outstanding_primary, 0) as outstanding_primary,
        coalesce(outstanding_secondary, 0) as outstanding_secondary,
        coalesce(outstanding_patient, 0) as outstanding_patient,
        
        -- Calculated measures
        coalesce(outstanding_primary, 0) + 
        coalesce(outstanding_secondary, 0) + 
        coalesce(outstanding_patient, 0) as total_outstanding
        
    from claims
    where patient_id is not null
      and provider_id is not null
)

select * from final
