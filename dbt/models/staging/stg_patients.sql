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
Extracts patient demographics from FHIR Patient resources
Matches Synthea PATIENTS.CSV schema
*/

WITH source AS (
  SELECT
    file_key,
    patient_id,
    loaded_at,
    bundle
  FROM {{ source('raw', 'fhir_bundles') }}
  
  {% if is_incremental() %}
  WHERE loaded_at > (SELECT COALESCE(MAX(loaded_at), '1900-01-01'::TIMESTAMP) FROM {{ this }})
  {% endif %}
),

patient_resources AS (
  SELECT
    source.file_key,
    source.patient_id,
    source.loaded_at,
    entry.value:resource AS resource
  FROM source,
  LATERAL FLATTEN(input => source.bundle:entry) entry
  WHERE entry.value:resource:resourceType::STRING = 'Patient'
),

flattened AS (
  SELECT
    resource:id::STRING as id,
    {{ dbt_utils.safe_cast("resource:birthDate::STRING", api.Column.translate_type("date")) }} as birthdate,
    TRY_TO_TIMESTAMP(resource:deceasedDateTime::STRING) as deathdate,
    resource:gender::STRING as gender,
    
    -- Identifiers
    (
      SELECT id_elem.value:value::STRING
      FROM LATERAL FLATTEN(input => resource:identifier) id_elem
      WHERE id_elem.value:type:coding[0]:code::STRING = 'SS'
      LIMIT 1
    ) as ssn,
    
    (
      SELECT id_elem.value:value::STRING
      FROM LATERAL FLATTEN(input => resource:identifier) id_elem
      WHERE id_elem.value:type:coding[0]:code::STRING = 'DL'
      LIMIT 1
    ) as drivers,
    
    (
      SELECT id_elem.value:value::STRING
      FROM LATERAL FLATTEN(input => resource:identifier) id_elem
      WHERE id_elem.value:type:coding[0]:code::STRING = 'PPN'
      LIMIT 1
    ) as passport,
    
    -- Name components
    resource:name[0]:prefix[0]::STRING as prefix,
    resource:name[0]:given[0]::STRING as first,
    resource:name[0]:given[1]::STRING as middle,
    resource:name[0]:family::STRING as last,
    resource:name[0]:suffix[0]::STRING as suffix,
    
    -- Maiden name from extension
    (
      SELECT ext.value:valueString::STRING
      FROM LATERAL FLATTEN(input => resource:extension) ext
      WHERE ext.value:url::STRING = 'http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName'
      LIMIT 1
    ) as maiden,
    
    -- Marital status
    resource:maritalStatus:coding[0]:code::STRING as marital,
    
    -- Race from US Core extension
    (
      SELECT child_ext.value:valueString::STRING
      FROM LATERAL FLATTEN(input => resource:extension) parent_ext,
           LATERAL FLATTEN(input => parent_ext.value:extension) child_ext
      WHERE parent_ext.value:url::STRING = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'
        AND child_ext.value:url::STRING = 'text'
      LIMIT 1
    ) as race,
    
    -- Ethnicity from US Core extension
    (
      SELECT child_ext.value:valueString::STRING
      FROM LATERAL FLATTEN(input => resource:extension) parent_ext,
           LATERAL FLATTEN(input => parent_ext.value:extension) child_ext
      WHERE parent_ext.value:url::STRING = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity'
        AND child_ext.value:url::STRING = 'text'
      LIMIT 1
    ) as ethnicity,
    
    -- Birthplace from extension
    (
      SELECT ext.value:valueAddress:city::STRING
      FROM LATERAL FLATTEN(input => resource:extension) ext
      WHERE ext.value:url::STRING = 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace'
      LIMIT 1
    ) as birthplace,
    
    -- Address
    resource:address[0]:line[0]::STRING as address,
    resource:address[0]:city::STRING as city,
    resource:address[0]:state::STRING as state,
    resource:address[0]:district::STRING as county,
    resource:address[0]:postalCode::STRING as zip,
    
    -- Geolocation from address extension
    (
      SELECT geo_ext.value:valueDecimal::FLOAT
      FROM LATERAL FLATTEN(input => resource:address[0]:extension) addr_ext,
           LATERAL FLATTEN(input => addr_ext.value:extension) geo_ext
      WHERE addr_ext.value:url::STRING = 'http://hl7.org/fhir/StructureDefinition/geolocation'
        AND geo_ext.value:url::STRING = 'latitude'
      LIMIT 1
    ) as lat,
    
    (
      SELECT geo_ext.value:valueDecimal::FLOAT
      FROM LATERAL FLATTEN(input => resource:address[0]:extension) addr_ext,
           LATERAL FLATTEN(input => addr_ext.value:extension) geo_ext
      WHERE addr_ext.value:url::STRING = 'http://hl7.org/fhir/StructureDefinition/geolocation'
        AND geo_ext.value:url::STRING = 'longitude'
      LIMIT 1
    ) as lon,
    
    -- Placeholder columns (will be calculated in marts)
    NULL as fips,
    0.00 as healthcare_expenses,
    0.00 as healthcare_coverage,
    0 as income,
    
    loaded_at
  FROM patient_resources
)

SELECT * FROM flattened
