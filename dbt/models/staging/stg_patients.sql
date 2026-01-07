{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['staging', 'patient', 'incremental']
  )
}}

/*
Staging Model: Patients
Extracts patient demographics from FHIR Patient resources in bundle_data
*/

WITH source AS (
  SELECT
    file_key,
    loaded_at,
    bundle_data
  FROM {{ source('raw', 'fhir_bundles') }}
  
  {% if is_incremental() %}
  WHERE loaded_at > (SELECT COALESCE(MAX(loaded_at), '1900-01-01'::TIMESTAMP) FROM {{ this }})
  {% endif %}
),

patient_resources AS (
  SELECT
    source.file_key,
    source.loaded_at,
    entry.value:resource AS resource
  FROM source,
  LATERAL FLATTEN(input => source.bundle_data:entry) entry
  WHERE entry.value:resource:resourceType::STRING = 'Patient'
)

SELECT
  MD5(CONCAT(file_key, '|', resource:id::STRING)) as surrogate_key,
  resource:id::STRING as id,
  TRY_CAST(resource:birthDate::STRING AS DATE) as birthdate,
  TRY_TO_TIMESTAMP(resource:deceasedDateTime::STRING) as deathdate,
  resource:gender::STRING as gender,
  resource:name[0]:prefix[0]::STRING as prefix,
  resource:name[0]:given[0]::STRING as first,
  resource:name[0]:given[1]::STRING as middle,
  resource:name[0]:family::STRING as last,
  resource:name[0]:suffix[0]::STRING as suffix,
  resource:maritalStatus:coding[0]:code::STRING as marital,
  resource:address[0]:line[0]::STRING as address,
  resource:address[0]:city::STRING as city,
  resource:address[0]:state::STRING as state,
  resource:address[0]:district::STRING as county,
  resource:address[0]:postalCode::STRING as zip,
  resource:communication[0]:language:coding[0]:code::STRING as language,
  resource:multipleBirthBoolean::BOOLEAN as multiple_birth,
  resource:multipleBirthInteger::INTEGER as birth_order,
  file_key,
  loaded_at,
  CURRENT_TIMESTAMP() as transformed_at
FROM patient_resources
