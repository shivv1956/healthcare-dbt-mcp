{{
    config(
        materialized = 'incremental',
        unique_key = ['patient_id', 'encounter_id', 'udi'],
        incremental_strategy = 'merge'
    )
}}

with source as (

    select *
    from {{ source('raw', 'DEVICES') }}

    {% if is_incremental() %}
      WHERE LOAD_TIMESTAMP > (
          SELECT COALESCE(MAX(LOAD_TIMESTAMP), '1900-01-01')
          FROM {{ this }}
      )
    {% endif %}

),

validated as (

    select

        "Start" as start_ts,
        "Stop"  as stop_ts,

        --Foreign key to the Patient and Encounter
        {{ validate_uuid('PATIENT') }}::varchar(36)   as patient_id,
        {{ validate_uuid('ENCOUNTER') }}::varchar(36) as encounter_id,

        "Code"        as code,
        "DESCRIPTION" as description,
        "UDI"         as udi,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by
                {{ validate_uuid('PATIENT') }},
                {{ validate_uuid('ENCOUNTER') }},
                UDI
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select
    start_ts,
    stop_ts,
    patient_id,
    encounter_id,
    code,
    description,
    udi,
    source_file_name,
    load_timestamp
from validated
where rn = 1
  and patient_id is not null
--   and encounter_id is not null
  and code is not null
  and udi is not null
