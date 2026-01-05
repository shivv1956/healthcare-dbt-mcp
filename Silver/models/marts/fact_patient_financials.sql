{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='patient_id'
) }}

with all_patients as (
    select distinct patient_id from {{ ref('stg_patients') }} where patient_id is not null

    union

    select distinct patient_id from {{ ref('stg_encounters') }} where patient_id is not null

    union

    select distinct patient_id from {{ ref('stg_medications') }} where patient_id is not null

    union

    select distinct patient_id from {{ ref('stg_claims') }} where patient_id is not null
),

patient_medications as (
    select
        patient_id,
        sum(totalcost) as total_medication_cost,
        count(distinct medication_code) as unique_medications
    from {{ ref('stg_medications') }}
    where patient_id is not null
    group by patient_id
),

patient_claims as (
    select
        patient_id,
        sum(outstanding_primary) as total_outstanding_primary,
        count(*) as total_claims
    from {{ ref('stg_claims') }}
    where patient_id is not null
    group by patient_id
),

patient_claim_amounts as (
    select
        c.patient_id,
        sum(ct.amount) as total_claim_payments,
        count(distinct ct.id) as total_transactions
    from {{ ref('stg_claims_transactions') }} ct
    join {{ ref('stg_claims') }} c on ct.claim_id = c.claim_id
    where c.patient_id is not null
    group by c.patient_id
),

patient_encounters as (
    select
        patient_id,
        count(*) as total_encounters,
        min(start_ts) as first_encounter_date,
        max(start_ts) as last_encounter_date
    from {{ ref('stg_encounters') }}
    where patient_id is not null
    group by patient_id
),

final as (
    select
        -- Key
        ap.patient_id,
        
        -- Encounter metrics
        coalesce(e.total_encounters, 0) as total_encounters,
        e.first_encounter_date,
        e.last_encounter_date,
        
        -- Medication metrics
        coalesce(m.total_medication_cost, 0) as total_medication_cost,
        coalesce(m.unique_medications, 0) as unique_medications,
        
        -- Claims metrics
        coalesce(c.total_outstanding_primary, 0) as total_outstanding_primary,
        coalesce(c.total_claims, 0) as total_claims,
        
        -- Payment metrics
        coalesce(ca.total_transactions, 0) as total_transactions,
        
        -- Calculated metrics
        coalesce(m.total_medication_cost, 0) +  coalesce(c.total_outstanding_primary, 0) as total_healthcare_cost
        
    from all_patients ap
    left join patient_medications m on ap.patient_id = m.patient_id
    left join patient_claims c on ap.patient_id = c.patient_id
    left join patient_claim_amounts ca on ap.patient_id = ca.patient_id
    left join patient_encounters e on ap.patient_id = e.patient_id
)

select * from final
