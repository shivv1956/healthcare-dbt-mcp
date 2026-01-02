{{
    config(
        materialized = 'incremental',
        unique_key = ['patient_id', 'encounter_id', 'code'],
        incremental_strategy = 'merge'
    )
}}

with source as (

    select *
    from {{ source('raw', 'CONDITIONS') }}

    {% if is_incremental() %}
      WHERE LOAD_TIMESTAMP > (
          SELECT COALESCE(MAX(LOAD_TIMESTAMP), '1900-01-01')
          FROM {{ this }}
      )
    {% endif %}

),

validated as (

    select
        "START" as start_date,
        "STOP"  as stop_date,

        --Foreign key to the Patient and Encounter
        {{ validate_uuid('PATIENT') }}::varchar(36)   as patient_id,
        {{ validate_uuid('ENCOUNTER') }}::varchar(36) as encounter_id,

        --Specifies the code system
        'SNOMED-CT' as system,

        CODE as code,
        DESCRIPTION as description,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by
                {{ validate_uuid('PATIENT') }},
                {{ validate_uuid('ENCOUNTER') }},
                CODE
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
