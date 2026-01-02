{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['patient_id', 'payer_id', 'start_year']
  ) 
}}

with source as (

    select *
    from {{ source('raw', 'PAYER_TRANSITIONS') }}

    {% if is_incremental() %}
    where load_timestamp >
      (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
    {% endif %}

),

validated as (

    select
        {{ validate_uuid('PATIENT') }} as patient_id,
        {{ validate_uuid('"MEMBER ID"') }} as member_id,

        -- HL7 wants YYYY, keep as DATE or NUMBER
        to_date(START_YEAR || '-01-01') as start_year,
        to_date(END_YEAR   || '-12-31') as end_year,

        'PAYER' as payer_id,
        {{ validate_uuid('"SECONDARY PAYER"') }} as secondary_payer_id,

        OWNERSHIP  as ownership,
        "OWNER NAME" as owner_name,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by
                {{ validate_uuid('PATIENT') }},
                'PAYER',
                START_YEAR
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and patient_id is not null
  and payer_id is not null
  and start_year is not null
