{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['patient_id', 'code']
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'ALLERGIES') }}

    {% if is_incremental() %}
      WHERE LOAD_TIMESTAMP > (
          SELECT COALESCE(MAX(LOAD_TIMESTAMP), '1900-01-01')
          FROM {{ this }}
      )
    {% endif %}
),

validated AS (

    SELECT
        
        "START" AS start_date,
        "STOP"  AS stop_date,

        --validation -- Foreign keys
        {{ validate_uuid('PATIENT') }}::VARCHAR(36)   AS patient_id,
        {{ validate_uuid('ENCOUNTER') }}::VARCHAR(36) AS encounter_id,

        CODE as code,
        SYSTEM as system,
        DESCRIPTION as description,
        TYPE as type,
        CATEGORY as category,
        REACTION1 as reaction1,
        DESCRIPTION1 as description1,

        --validation
        CASE 
            WHEN UPPER(SEVERITY1) IN ('MILD','MODERATE','SEVERE') 
            THEN UPPER(SEVERITY1)
            ELSE NULL
        END::VARCHAR(10) AS severity1,

        REACTION2 as reaction2,
        DESCRIPTION2 as description2,

        --validation
        CASE 
            WHEN UPPER(SEVERITY2) IN ('MILD','MODERATE','SEVERE')
            THEN UPPER(SEVERITY2)
            ELSE NULL
        END::VARCHAR(10) AS severity2,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP

    FROM source
)

SELECT *
FROM validated
WHERE patient_id IS NOT NULL
  AND code IS NOT NULL
