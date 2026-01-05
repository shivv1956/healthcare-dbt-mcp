{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='provider_id'
) }}

with providers_source as (
    select * from {{ ref('stg_providers') }}
),

encounters as (
    select
        provider_id,
        count( patient_id) as patient_count
    from {{ ref('stg_encounters') }}
    where provider_id is not null
    group by provider_id
),

final as (
    select
        -- Surrogate key (already generated in staging)
        p.id as provider_id,
        
        -- Provider information
        p.name,
        p.gender,
        p.speciality,
        p.organization_id,
        
        -- Address information
        p.city,
        p.state,
        p.zip,
        
        -- Metrics
        coalesce(e.patient_count, 0) as patients_treated
        
    from providers_source p
    left join encounters e on p.id = e.provider_id
)

select * from final
