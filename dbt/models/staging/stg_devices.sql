{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['staging', 'device', 'incremental']
  )
}}

/*
Staging Model: Devices
Extracts medical device data from FHIR Device resources
Matches Synthea DEVICES.CSV schema
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

-- Get Device resources directly
device_resources AS (
  SELECT
    source.file_key,
    source.loaded_at,
    entry.value:resource AS resource
  FROM source,
  LATERAL FLATTEN(input => source.bundle_data:entry) entry
  WHERE entry.value:resource:resourceType::STRING = 'Device'
),

flattened AS (
  SELECT
    resource:id::STRING as id,
    
    -- Timing - use manufacturing/expiration dates if available
    COALESCE(
      TRY_TO_TIMESTAMP(resource:manufactureDate::STRING),
      loaded_at
    ) as "START",
    TRY_TO_TIMESTAMP(resource:expirationDate::STRING) as "STOP",
    
    -- Patient association from Device is indirect - will be resolved in intermediate
    {{ extract_uuid_from_reference('resource:patient:reference') }} as patient,
    NULL as encounter,
    
    -- Device details
    resource:type:coding[0]:code::STRING as code,
    COALESCE(
      resource:type:coding[0]:display::STRING,
      resource:type:text::STRING,
      resource:deviceName[0]:name::STRING
    ) as description,
    resource:udiCarrier[0]:deviceIdentifier::STRING as udi,
    
    loaded_at
  FROM device_resources
)

SELECT
  {{ generate_surrogate_key(['id', 'patient']) }} as surrogate_key,
  *
FROM flattened
