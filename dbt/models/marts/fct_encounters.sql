{{
  config(
    materialized='table',
    tags=['marts', 'fact', 'encounter']
  )
}}

/*
Marts Model: Encounters Fact Table
Complete encounter/visit information with resolved references and costs
*/

WITH base_encounters AS (
  SELECT
    id,
    start,
    stop,
    patient,
    organization,
    provider,
    encounterclass,
    code,
    description,
    reasoncode,
    reasondescription,
    loaded_at
  FROM {{ ref('stg_encounters') }}
),

-- Enrich with costs from intermediate model
enriched_encounters AS (
  SELECT
    enc.*,
    COALESCE(costs.base_cost, 0.00) as base_encounter_cost,
    COALESCE(costs.total_cost, 0.00) as total_claim_cost,
    COALESCE(costs.payer_coverage, 0.00) as payer_coverage
  FROM base_encounters enc
  LEFT JOIN {{ ref('int_claims_enriched') }} costs
    ON enc.id = costs.resource_id
    AND costs.cost_type = 'encounter'
),

-- Resolve references
with_references AS (
  SELECT
    enc.*,
    ref_patient.display_name as patient_name,
    ref_org.display_name as organization_name,
    ref_provider.display_name as provider_name
  FROM enriched_encounters enc
  LEFT JOIN {{ ref('int_reference_map') }} ref_patient
    ON enc.patient = ref_patient.resource_id
    AND ref_patient.resource_type = 'Patient'
  LEFT JOIN {{ ref('int_reference_map') }} ref_org
    ON enc.organization = ref_org.resource_id
    AND ref_org.resource_type = 'Organization'
  LEFT JOIN {{ ref('int_reference_map') }} ref_provider
    ON enc.provider = ref_provider.resource_id
    AND ref_provider.resource_type = 'Practitioner'
)

SELECT
  id as encounter_key,
  patient as patient_id,
  patient_name,
  organization as organization_id,
  organization_name,
  provider as provider_id,
  provider_name,
  
  -- Encounter details
  start as encounter_start,
  stop as encounter_stop,
  DATEDIFF(hour, start, stop) as encounter_duration_hours,
  encounterclass as encounter_class,
  code as encounter_code,
  description as encounter_description,
  reasoncode as reason_code,
  reasondescription as reason_description,
  
  -- Financial
  base_encounter_cost,
  total_claim_cost,
  payer_coverage,
  total_claim_cost - payer_coverage as patient_responsibility,
  
  -- Metadata
  loaded_at as last_updated_at
  
FROM with_references
