{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['patient_id','encounter_id','code','start_datetime']
  ) 
}}

with source as (

    select *
    from {{ source('raw', 'MEDICATIONS') }}

    {% if is_incremental() %}
    where load_timestamp >
      (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
    {% endif %}

),

validated as (

    select
        "Start"::timestamp_ntz as start_datetime,
        "Stop"::timestamp_ntz  as stop_datetime,

        {{ validate_uuid('"PATIENT"') }}   as patient_id,
        {{ validate_uuid('"PAYER"') }}     as payer_id,
        {{ validate_uuid('"ENCOUNTER"') }} as encounter_id,

        "CODE"        as medication_code,
        "DESCRIPTION" as medication_description,

        "Base_Cost" as base_cost,
        "Payer_Coverage" as payer_coverage,
        "Dispenses" as dispenses,
        "TotalCost" as totalcost,

        "ReasonCode" as reasoncode,
        "ReasonDescription" as reasondescription,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by {{ validate_uuid('"PATIENT"') }},
                         {{ validate_uuid('"ENCOUNTER"') }},
                         "CODE","Start"
            order by load_timestamp desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
    and patient_id is not null
    and encounter_id is not null
    and start_datetime is not null
