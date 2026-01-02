{{ 
    config(
        materialized = 'incremental',
        unique_key = 'careplan_id',
        incremental_strategy = 'merge'
    ) 
}}

with source as (

    select *
    from {{ source('raw', 'CAREPLANS') }}

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
        {{ validate_uuid('ID') }}::varchar(36) as careplan_id,

        
        "START" AS start_date,
        "STOP"  AS stop_date,

        --Foreign key to the Patient and encounter
        {{ validate_uuid('PATIENT') }}::VARCHAR(36)   AS patient_id,
        {{ validate_uuid('ENCOUNTER') }}::VARCHAR(36) AS encounter_id,

        
        CODE as code,
        DESCRIPTION as description,

        REASONCODE as reason_code,
        REASONDESCRIPTION as reason_description,
        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by {{ validate_uuid('ID') }}
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and careplan_id is not null
  and patient_id is not null
