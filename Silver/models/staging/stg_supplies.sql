{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['patient_id', 'encounter_id', 'code', 'date']
  ) 
}}

with source as (

    select *
    from {{ source('raw', 'SUPPLIES') }}

    {% if is_incremental() %}
    where load_timestamp > (
        select coalesce(max(load_timestamp), '1900-01-01') 
        from {{ this }}
    )
    {% endif %}

),

validated as (

    select
        "Date" as date,
        
        {{ validate_uuid('PATIENT') }} as patient_id,
        {{ validate_uuid('ENCOUNTER') }} as encounter_id,

        CODE as code,
        "DESCRIPTION" as description,
        QUANTITY as quantity,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by 
                {{ validate_uuid('PATIENT') }},
                {{ validate_uuid('ENCOUNTER') }},
                CODE,
                "Date"
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and patient_id is not null
--   and encounter_id is not null
  and code is not null
  and date is not null
