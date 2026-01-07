{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['staging', 'payer', 'incremental']
  )
}}

/*
Staging Model: Payers
Extracts insurance payer/plan data from FHIR Organization resources with payer type
Matches Synthea PAYERS.CSV schema
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

-- Extract unique payers from ExplanationOfBenefit resources
eob_resources AS (
  SELECT
    source.file_key,
    source.loaded_at,
    entry.value:resource:insurer:display::STRING as insurer_name,
    entry.value:resource:insurer:reference::STRING as insurer_ref
  FROM source,
  LATERAL FLATTEN(input => source.bundle_data:entry) entry
  WHERE entry.value:resource:resourceType::STRING = 'ExplanationOfBenefit'
    AND entry.value:resource:insurer:display::STRING IS NOT NULL
),

-- Get unique payers
unique_payers AS (
  SELECT DISTINCT
    insurer_name,
    insurer_ref,
    MAX(loaded_at) as loaded_at
  FROM eob_resources
  GROUP BY insurer_name, insurer_ref
),

flattened AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['insurer_name']) }} as id,
    insurer_name as name,
    
    -- Ownership type (Government vs Private)
    CASE
      WHEN insurer_name ILIKE '%medicare%' THEN 'Government'
      WHEN insurer_name ILIKE '%medicaid%' THEN 'Government'
      WHEN insurer_name ILIKE '%tricare%' THEN 'Government'
      WHEN insurer_name ILIKE '%dual eligible%' THEN 'Government'
      WHEN insurer_name = 'NO_INSURANCE' THEN 'Self-Pay'
      ELSE 'Private'
    END as ownership,
    
    -- Address fields - not available in EOB, using NULL
    NULL as address,
    NULL as city,
    NULL as state_headquartered,
    NULL as zip,
    
    -- Contact
    NULL as phone,
    
    -- Financial metrics - will be calculated in marts from Claims
    0.00 as amount_covered,
    0.00 as amount_uncovered,
    0.00 as revenue,
    0 as covered_encounters,
    0 as uncovered_encounters,
    0 as covered_medications,
    0 as uncovered_medications,
    0 as covered_procedures,
    0 as uncovered_procedures,
    0 as covered_immunizations,
    0 as uncovered_immunizations,
    0 as unique_customers,
    0.00 as qols_avg,
    0 as member_months,
    
    loaded_at
  FROM unique_payers
)

SELECT * FROM flattened
