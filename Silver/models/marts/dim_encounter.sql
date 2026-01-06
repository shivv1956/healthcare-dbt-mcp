{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='encounter_id'
) }}

with encounters_source as (
    select * from {{ ref('stg_encounters') }}
),

final as (
    select
        -- Primary key
        encounter_id,
        
        -- Foreign keys to other dimensions
        patient_id,
        provider_id,
        organization_id,
        
        
        -- Encounter attributes
        coalesce(encounter_class, 'UNKNOWN') as encounter_class,
        code as encounter_code,
        description as encounter_description,
        
        -- Temporal attributes
        start_ts as encounter_start,
        stop_ts as encounter_end,
        datediff(minute, start_ts, stop_ts) as encounter_duration_minutes,
        
        -- Financial attributes
        coalesce(base_encounter_cost, 0) as base_encounter_cost,
        coalesce(total_claim_cost, 0) as total_claim_cost,
        coalesce(payer_coverage, 0) as payer_coverage,
        
        -- Reason attributes
        coalesce(reason_code, 'UNKNOWN') as reason_code,
        coalesce(reason_description, 'UNKNOWN') as reason_description
        
    from encounters_source
)

select * from final
