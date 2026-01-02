{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['patient_id','encounter_id','code','start_datetime']
  ) 
}}

with source as (

    select *
    from {{ source('raw', 'PROCEDURES') }}

    {% if is_incremental() %}
    where load_timestamp > (
        select coalesce(max(load_timestamp), '1900-01-01') 
        from {{ this }}
    )
    {% endif %}

),

validated as (

    select
        "Start"::timestamp_ntz as start_datetime,
        "Stop"::timestamp_ntz  as stop_datetime,

        {{ validate_uuid('"PATIENT"') }}   as patient_id,
        {{ validate_uuid('"ENCOUNTER"') }} as encounter_id,

        'SNOMED-CT' as system,
        "CODE" as code,
        "DESCRIPTION" as description,
        "Base_Cost" as base_cost,
        "ReasonCode" as reason_code,
        "ReasonDescription" as reason_description,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by
                {{ validate_uuid('"PATIENT"') }},
                {{ validate_uuid('"ENCOUNTER"') }},
                "CODE",
                "Start"
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and patient_id is not null
  and encounter_id is not null
  and code is not null
  and start_datetime is not null
