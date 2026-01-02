{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['patient_id', 'encounter_id', 'immunization_datetime', 'code']
    )
}}

with source as (

    select *
    from {{ source('raw', 'IMMUNIZATIONS') }}

    {% if is_incremental() %}
      WHERE LOAD_TIMESTAMP > (
          SELECT COALESCE(MAX(LOAD_TIMESTAMP), '1900-01-01')
          FROM {{ this }}
      )
    {% endif %}

),

validated as (

    select

        "Date" as immunization_datetime,

        -- Foreign keys
        {{ validate_uuid('"PATIENT"') }}::varchar(36)   as patient_id,
        {{ validate_uuid('"ENCOUNTER"') }}::varchar(36) as encounter_id,

        "CODE"        as code,
        "DESCRIPTION" as description,
        "COST" as cost,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by
                {{ validate_uuid('"PATIENT"') }},
                {{ validate_uuid('"ENCOUNTER"') }},
                "Date",
                "CODE"
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and immunization_datetime is not null
  and patient_id is not null
  and encounter_id is not null
  and code is not null
