{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='organization_id'
) }}

with organizations_source as (
    select * from {{ ref('stg_organizations') }}
),

encounter_counts as (
    select
        organization_id,
        count(distinct encounter_id) as encounter_count,
        count(distinct patient_id) as patient_count
    from {{ ref('stg_encounters') }}
    where organization_id is not null
    group by organization_id
),

final as (
    select
        -- Surrogate key (already generated in staging)
        o.organization_id,
        
        -- Organization information
        o.name,
        o.phone,
        
        -- Address information
        o.address,
        o.city,
        o.state,
        o.zip,
        o.latitude,
        o.longitude,
        
        -- Financial metrics
        o.utilization,
        
        -- Activity metrics
        coalesce(e.encounter_count, 0) as total_encounters,
        coalesce(e.patient_count, 0) as total_patients
        
    from organizations_source o
    left join encounter_counts e on o.organization_id = e.organization_id
)

select * from final
