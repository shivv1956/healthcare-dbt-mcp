{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['patient_id', 'encounter_id', 'code', 'observation_datetime','value']
  ) 
}}

with source as (

    select *
    from {{ source('raw', 'OBSERVATIONS') }}

    {% if is_incremental() %}
    where load_timestamp >
      (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
    {% endif %}

),

validated as (

    select
        "DATE"::timestamp_ntz as observation_datetime,

        {{ validate_uuid('PATIENT') }}   as patient_id,
        {{ validate_uuid('ENCOUNTER') }} as encounter_id,

        CATEGORY        as category,
        CODE            as code,
        DESCRIPTION     as description,
        VALUE           as value,
        UNITS           as units,
        TYPE            as type,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by
                patient_id,
                encounter_id,
                code,
                observation_datetime,
                value
            order by load_timestamp desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and patient_id is not null
  and observation_datetime is not null
  and encounter_id is not null
  and code is not null
  and value is not null
