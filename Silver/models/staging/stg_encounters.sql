{{
    config(
        materialized = 'incremental',
        unique_key = 'encounter_id',
        incremental_strategy = 'merge'
    )
}}

with source as (

    select *
    from {{ source('raw', 'ENCOUNTERS') }}

    {% if is_incremental() %}
      WHERE LOAD_TIMESTAMP > (
          SELECT COALESCE(MAX(LOAD_TIMESTAMP), '1900-01-01')
          FROM {{ this }}
      )
    {% endif %}

),

validated as (

    select
        -- Primary key
        {{ validate_uuid('"Id"') }}::varchar(36) as encounter_id,

        "Start" as start_ts,
        "Stop"  as stop_ts,

        -- Foreign keys
        {{ validate_uuid('"Patient"') }}::varchar(36)      as patient_id,
        {{ validate_uuid('"Organization"') }}::varchar(36) as organization_id,
        -- provider_id is not in uuid format
        "Provider"::varchar(36)     as provider_id,
        {{ validate_uuid('"Payer"') }}::varchar(36)        as payer_id,

        CASE 
            WHEN "EncounterClass" = 'AMB' THEN 'AMBIGUOUS'
            WHEN "EncounterClass" = 'EMER' THEN 'EMERGENCY'
            WHEN "EncounterClass" = 'INP' THEN 'INPATIENT'
            WHEN "EncounterClass" = 'OUTP' THEN 'OUTPATIENT'
            WHEN "EncounterClass" = 'VIRT' THEN 'VIRTUAL'  
        END as encounter_class,
        "Code"           as code,
        "Description"    as description,

        "Base_Encounter_Cost"  as base_encounter_cost,
        "Total_Claim_Cost"     as total_claim_cost,
        "Payer_Coverage"       as payer_coverage,

        "ReasonCode"        as reason_code,
        "ReasonDescription" as reason_description,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by {{ validate_uuid('"Id"') }}
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and encounter_id is not null
    and patient_id is not null
    and organization_id is not null
    and provider_id is not null
    -- and payer_id is not null
