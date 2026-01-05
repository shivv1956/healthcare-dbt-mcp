{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='encounter_id'
) }}

with encounters as (
    select 
        encounter_id,
        patient_id,
        provider_id,
        organization_id,
        payer_id,
        start_ts,
        stop_ts,
        encounter_class
    from {{ ref('stg_encounters') }}
),

medication_costs as (
    select
        encounter_id,
        sum(totalcost) as total_medication_cost,
        count(*) as medication_count
    from {{ ref('stg_medications') }}
    where encounter_id is not null
    group by encounter_id
),

final as (
    select
        -- Keys
        e.encounter_id,
        e.patient_id,
        e.provider_id,
        e.organization_id,
        
        -- Encounter details
        e.start_ts as encounter_start,
        e.stop_ts as encounter_end,
        e.encounter_class,
        datediff(hour, e.start_ts, e.stop_ts) as encounter_duration_hours,
        
        -- Medication metrics
        coalesce(m.total_medication_cost, 0) as total_medication_cost
        
    from encounters e
    left join medication_costs m on e.encounter_id = m.encounter_id
)

select * from final
