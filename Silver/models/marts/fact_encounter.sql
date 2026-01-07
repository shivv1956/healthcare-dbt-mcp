{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='encounter_id'
) }}

with encounters as (
    select * from {{ ref('stg_encounters') }}
),

final as (
    select
        -- Primary key
        encounter_id,
        
        -- Foreign keys to dimensions
        patient_id,
        provider_id,
        organization_id,
        payer_id,
        
        -- Date keys
        to_varchar(start_ts, 'YYYYMMDD')::integer as start_date_key,
        to_varchar(stop_ts, 'YYYYMMDD')::integer as stop_date_key,
        
        -- Facts/Measures
        coalesce(base_encounter_cost, 0) as base_encounter_cost,
        coalesce(total_claim_cost, 0) as total_claim_cost,
        coalesce(payer_coverage, 0) as payer_coverage,
        1 as encounter_count
        
    from encounters
    where patient_id is not null
)

select * from final
