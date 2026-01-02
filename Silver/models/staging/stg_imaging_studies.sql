{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['imaging_study_id', 'series_uid', 'instance_uid']
    )
}}

with source as (

    select *
    from {{ source('raw', 'IMAGING_STUDIES') }}

    {% if is_incremental() %}
      WHERE LOAD_TIMESTAMP > (
          SELECT COALESCE(MAX(LOAD_TIMESTAMP), '1900-01-01')
          FROM {{ this }}
      )
    {% endif %}

),

validated as (

    select
        -- Non-unique
        {{ validate_uuid('"Id"') }}::varchar(36) as imaging_study_id,

        "Date" as imaging_datetime,

        -- Foreign keys
        {{ validate_uuid('"Patient"') }}::varchar(36)   as patient_id,
        {{ validate_uuid('"Encounter"') }}::varchar(36) as encounter_id,

        -- DICOM identifiers
        "Series UID"   as series_uid,
        "Instance UID" as instance_uid,

        "Body Site Code"        as body_site_code,
        "Body Site Description" as body_site_description,

        "Modality Code"        as modality_code,
        "Modality Description" as modality_description,

        "SOP Code"        as sop_code,
        "SOP Description" as sop_description,

        "Procedure Code" as procedure_code,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by
                {{ validate_uuid('"Id"') }},
                "Series UID",
                "Instance UID"
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and imaging_study_id is not null
  and patient_id is not null
  and encounter_id is not null
  and series_uid is not null
  and instance_uid is not null
