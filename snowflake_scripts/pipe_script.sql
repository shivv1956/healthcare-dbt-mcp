-- Raw landing zone for all JSON bundles
CREATE TABLE IF NOT EXISTS RAW_LANDING_ZONE (
    JSON_CONTENT VARIANT,
    SRC_FILENAME STRING,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stream to capture new inserts in the landing table
CREATE STREAM IF NOT EXISTS FHIR_RAW_STREAM ON TABLE RAW_LANDING_ZONE;
select * from fhir_raw_stream;
-- PIPE
CREATE OR REPLACE PIPE FHIR_INGESTION_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO RAW_LANDING_ZONE (JSON_CONTENT, SRC_FILENAME)
FROM (
    SELECT $1, metadata$filename 
    FROM @AWS_STAGE
)
FILE_FORMAT = (
    TYPE = 'JSON' 
    STRIP_OUTER_ARRAY = FALSE
    IGNORE_UTF8_ERRORS = TRUE
)
ON_ERROR = 'SKIP_FILE';



-- task for insertion
CREATE OR REPLACE TASK AUTOMATED_FHIR_TRANSFORM
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('FHIR_RAW_STREAM')
AS
    call fhir_insert_script_procedure();

create or replace procedure fhir_insert_script_procedure()
returns string
language sql
as
$$
begin

    CREATE OR REPLACE TEMP TABLE SYNTHEA_HOSPITAL.RAW_DATA.FHIR_BATCH AS
    SELECT JSON_CONTENT, SRC_FILENAME
    FROM FHIR_RAW_STREAM
    WHERE METADATA$ACTION = 'INSERT';

    INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.ALLERGIES (
    "START", 
    "STOP", 
    PATIENT, 
    ENCOUNTER, 
    CODE, 
    "SYSTEM", 
    DESCRIPTION, 
    "TYPE", 
    CATEGORY, 
    REACTION1, 
    DESCRIPTION1, 
    SEVERITY1, 
    REACTION2, 
    DESCRIPTION2, 
    SEVERITY2, 
    SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    -- Read from the stream instead of the stage
    SELECT 
        JSON_CONTENT, 
        SRC_FILENAME 
    FROM FHIR_BATCH 
),
FLATTENED_ENTRIES AS (
    -- Flatten the bundle entries from the new files
    SELECT 
        entry.value:resource as res,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE entry.value:resource:resourceType::string = 'AllergyIntolerance'
)
SELECT 
    -- 1. START: Diagnosis Date
    try_to_date(substr(COALESCE(res:onsetDateTime::string, res:recordedDate::string), 1, 10)) as "START",
    
    -- 2. STOP: End Date
    try_to_date(substr(res:abatementDateTime::string, 1, 10)) as "STOP",
    
    -- 3. PATIENT: Clean UUID
    regexp_replace(res:patient:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 4. ENCOUNTER: Clean UUID
    regexp_replace(res:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 5. CODE: Allergy Code
    res:code:coding[0]:code::string as CODE,
    
    -- 6. SYSTEM: RxNorm vs SNOMED
    CASE 
        WHEN res:code:coding[0]:system::string ILIKE '%rxnorm%' THEN 'RxNorm'
        ELSE 'SNOMED-CT' 
    END as "SYSTEM",
    
    -- 7. DESCRIPTION
    res:code:text::string as DESCRIPTION,

    -- 8. TYPE
    res:type::string as "TYPE",

    -- 9. CATEGORY
    res:category[0]::string as CATEGORY,

    -- 10. REACTION1 Code
    res:reaction[0]:manifestation[0]:coding[0]:code::string as REACTION1,

    -- 11. DESCRIPTION1
    res:reaction[0]:manifestation[0]:coding[0]:display::string as DESCRIPTION1,

    -- 12. SEVERITY1
    res:reaction[0]:severity::string as SEVERITY1,

    -- 13. REACTION2 Code
    res:reaction[1]:manifestation[0]:coding[0]:code::string as REACTION2,

    -- 14. DESCRIPTION2
    res:reaction[1]:manifestation[0]:coding[0]:display::string as DESCRIPTION2,

    -- 15. SEVERITY2
    res:reaction[1]:severity::string as SEVERITY2,

    -- 16. Metadata from Stream
    SRC_FILENAME as SOURCE_FILE_NAME

FROM FLATTENED_ENTRIES;

INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.OBSERVATIONS (DATE, PATIENT, ENCOUNTER, CATEGORY, CODE, DESCRIPTION, VALUE, UNITS, TYPE, SOURCE_FILE_NAME)
WITH NEW_BATCH AS (
    -- Read only the new bundles from the stream
    SELECT 
        JSON_CONTENT, 
        SRC_FILENAME 
    FROM FHIR_BATCH
),
FLATTENED_RESOURCES AS (
    -- Flatten the bundle entries from the new files
    SELECT 
        SRC_FILENAME,
        entry.value:resource as resource
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE resource:resourceType::string = 'Observation'
)
-- PART 1: Standard Observations (Single Value)
SELECT 
    try_to_date(substr(resource:effectiveDateTime::string, 1, 10)) as DATE,
    regexp_replace(resource:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    regexp_replace(resource:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    resource:category[0]:coding[0]:display::string as CATEGORY,
    resource:code:coding[0]:code::string as CODE,
    resource:code:text::string as DESCRIPTION,
    -- Extract value from Quantity, CodeableConcept, or String
    COALESCE(
        resource:valueQuantity:value::string, 
        resource:valueCodeableConcept:text::string,
        resource:valueString::string
    ) as VALUE,
    resource:valueQuantity:unit::string as UNITS,
    -- Determine Type
    CASE 
        WHEN resource:valueQuantity IS NOT NULL THEN 'numeric'
        WHEN resource:valueCodeableConcept IS NOT NULL THEN 'text'
        WHEN resource:valueString IS NOT NULL THEN 'text'
        ELSE 'unknown'
    END as TYPE,
    SRC_FILENAME as SOURCE_FILE_NAME
FROM FLATTENED_RESOURCES
WHERE resource:component IS NULL

UNION ALL

-- PART 2: Component Observations (e.g., Blood Pressure, Surveys)
SELECT 
    try_to_date(substr(resource:effectiveDateTime::string, 1, 10)) as DATE,
    regexp_replace(resource:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    regexp_replace(resource:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    resource:category[0]:coding[0]:display::string as CATEGORY,
    -- Use Component Code
    comp.value:code:coding[0]:code::string as CODE,
    -- Use Component Description
    comp.value:code:text::string as DESCRIPTION,
    -- Extract Component Value
    COALESCE(
        comp.value:valueQuantity:value::string, 
        comp.value:valueCodeableConcept:text::string,
        comp.value:valueString::string
    ) as VALUE,
    comp.value:valueQuantity:unit::string as UNITS,
    CASE 
        WHEN comp.value:valueQuantity IS NOT NULL THEN 'numeric'
        WHEN comp.value:valueCodeableConcept IS NOT NULL THEN 'text'
        WHEN comp.value:valueString IS NOT NULL THEN 'text'
        ELSE 'unknown'
    END as TYPE,
    SRC_FILENAME as SOURCE_FILE_NAME
FROM FLATTENED_RESOURCES,
LATERAL FLATTEN(input => resource:component) as comp;


-- Add this inside your Task's BEGIN...END block
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.CAREPLANS (ID, "START", "STOP", PATIENT, ENCOUNTER, CODE, DESCRIPTION, REASONCODE, REASONDESCRIPTION, SOURCE_FILE_NAME)
WITH NEW_BATCH AS (
    -- Read incremental data from the stream
    SELECT 
        JSON_CONTENT, 
        SRC_FILENAME 
    FROM FHIR_BATCH 
),
FLATTENED_CAREPLANS AS (
    -- Flatten the bundle to find CarePlan resources
    SELECT 
        entry.value:resource as res,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE entry.value:resource:resourceType::string = 'CarePlan'
)
SELECT 
    -- 1. ID: Clean the UUID
    regexp_replace(res:id::string, 'urn:uuid:', '') as ID,
    
    -- 2. START: Care Plan Start Date
    try_to_date(substr(res:period:start::string, 1, 10)) as "START",
    
    -- 3. STOP: Care Plan End Date
    try_to_date(substr(res:period:end::string, 1, 10)) as "STOP",
    
    -- 4. PATIENT: Clean Patient UUID
    regexp_replace(res:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 5. ENCOUNTER: Clean Encounter UUID
    regexp_replace(res:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 6. CODE: Extract the SNOMED code
    COALESCE(
        res:category[1]:coding[0]:code::string, 
        res:category[0]:coding[0]:code::string
    ) as CODE,
    
    -- 7. DESCRIPTION: Extract the display text
    COALESCE(
        res:category[1]:coding[0]:display::string,
        res:category[0]:coding[0]:display::string
    ) as DESCRIPTION,

    -- 8. REASONCODE: Grab direct codes if present
    res:reasonCode[0]:coding[0]:code::string as REASONCODE,
    
    -- 9. REASONDESCRIPTION: 
    res:reasonCode[0]:coding[0]:display::string as REASONDESCRIPTION,

    -- Metadata from landing table
    SRC_FILENAME as SOURCE_FILE_NAME

FROM FLATTENED_CAREPLANS;


INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.CLAIMS (
    "Id",
    "Patient ID",
    "Provider ID",
    "Primary Patient Insurance ID",
    "Diagnosis1", "Diagnosis2", "Diagnosis3", "Diagnosis4",
    "Diagnosis5", "Diagnosis6", "Diagnosis7", "Diagnosis8",
    "Appointment ID",
    "Current Illness Date",
    "Service Date",
    "Supervising Provider ID",
    "Status1", 
    "Outstanding1",
    "HealthcareClaimTypeID1",
    SOURCE_FILE_NAME
)
WITH RAW_RESOURCES AS (
    -- Extract raw JSON resources for every Claim from the stream
    SELECT 
        regexp_replace(entry.value:resource:id::string, 'urn:uuid:', '') as CLAIM_UUID,
        entry.value:resource as RES,
        SRC_FILENAME as FILENAME
    FROM 
        FHIR_BATCH,
        LATERAL FLATTEN(input => JSON_CONTENT:entry) as entry
    WHERE 
        entry.value:resource:resourceType::string = 'Claim'
),
DIAGNOSIS_MAP AS (
    -- Flatten the 'item' and 'diagnosisSequence' arrays to link codes to sequences
    SELECT 
        R.CLAIM_UUID,
        diag_seq.value::int as SEQ_ID,
        item.value:productOrService:coding[0]:code::string as CODE
    FROM RAW_RESOURCES R,
    LATERAL FLATTEN(input => R.RES:item) as item,
    LATERAL FLATTEN(input => item.value:diagnosisSequence) as diag_seq
)
SELECT 
    -- 1. Id
    R.CLAIM_UUID,
    
    -- 2. Patient ID
    regexp_replace(R.RES:patient:reference::string, 'urn:uuid:', ''),
    
    -- 3. Provider ID
    CASE 
        WHEN R.RES:provider:reference::string LIKE 'urn:uuid:%' 
            THEN regexp_replace(R.RES:provider:reference::string, 'urn:uuid:', '')
        ELSE split_part(R.RES:provider:reference::string, '|', 2)
    END,

    -- 4. Primary Patient Insurance ID
    regexp_replace(R.RES:insurance[0]:coverage:reference::string, '#', ''),

    -- 6-13. Diagnosis 1-8 (Mapped and pivoted from CTE)
    MAX(CASE WHEN D.SEQ_ID = 1 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 2 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 3 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 4 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 5 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 6 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 7 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 8 THEN D.CODE END),

    -- 14. Appointment ID (Encounter)
    regexp_replace(R.RES:item[0]:encounter[0]:reference::string, 'urn:uuid:', ''),

    -- 15. Current Illness Date
    to_timestamp_ntz(R.RES:created::string),

    -- 16. Service Date
    to_timestamp_ntz(R.RES:billablePeriod:start::string),

    -- 17. Supervising Provider ID
    regexp_replace(R.RES:careTeam[0]:provider:reference::string, 'urn:uuid:', ''),

    -- 18. Status
    R.RES:status::string,

    -- 19. Outstanding (Total Cost)
    R.RES:total:value::number(18,2),

    -- 20. HealthcareClaimTypeID
    CASE 
        WHEN R.RES:type:coding[0]:code::string = 'professional' THEN 1
        WHEN R.RES:type:coding[0]:code::string = 'institutional' THEN 2
        ELSE NULL 
    END,

    -- Metadata
    R.FILENAME

FROM RAW_RESOURCES R
LEFT JOIN DIAGNOSIS_MAP D ON R.CLAIM_UUID = D.CLAIM_UUID
GROUP BY 
    R.CLAIM_UUID, 
    R.RES, 
    R.FILENAME;


-- This block goes inside your Task's BEGIN...END block
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.CONDITIONS (
    "START", 
    "STOP", 
    PATIENT, 
    ENCOUNTER, 
    CODE, 
    DESCRIPTION, 
    SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    -- Read incremental data from the stream
    SELECT 
        JSON_CONTENT, 
        SRC_FILENAME 
    FROM FHIR_BATCH 
),
FLATTENED_CONDITIONS AS (
    -- Flatten the bundle to find Condition resources
    SELECT 
        entry.value:resource as res,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE entry.value:resource:resourceType::string = 'Condition'
)
SELECT 
    -- 1. START: Map onsetDateTime or recordedDate to START
    try_to_date(substr(COALESCE(res:onsetDateTime::string, res:recordedDate::string), 1, 10)) as "START",
    
    -- 2. STOP: Map abatementDateTime to STOP
    try_to_date(substr(res:abatementDateTime::string, 1, 10)) as "STOP",
    
    -- 3. PATIENT: Extract UUID from reference
    regexp_replace(res:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 4. ENCOUNTER: Extract UUID from reference
    regexp_replace(res:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 5. CODE: Extract SNOMED code
    res:code:coding[0]:code::string as CODE,
    
    -- 6. DESCRIPTION: Extract Display Name
    res:code:coding[0]:display::string as DESCRIPTION,

    -- Metadata from landing table
    SRC_FILENAME as SOURCE_FILE_NAME

FROM FLATTENED_CONDITIONS;

-- This block goes inside your Task's BEGIN...END block
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.DEVICES (
    "Start", 
    "Stop", 
    PATIENT, 
    ENCOUNTER, 
    "Code", 
    DESCRIPTION, 
    UDI, 
    SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    -- Read incremental data from the stream
    SELECT 
        JSON_CONTENT, 
        SRC_FILENAME 
    FROM FHIR_BATCH 
),
FLATTENED_DEVICES AS (
    -- Flatten the bundle to find Device resources
    SELECT 
        entry.value:resource as res,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE entry.value:resource:resourceType::string = 'Device'
)
SELECT 
    -- 1. Start: Map manufactureDate
    to_timestamp_ntz(res:manufactureDate::string) as "Start",
    
    -- 2. Stop: Map expirationDate
    to_timestamp_ntz(res:expirationDate::string) as "Stop",
    
    -- 3. Patient: Clean UUID
    regexp_replace(res:patient:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 4. Encounter: Placeholder (Not in standard FHIR Device resource)
    NULL as ENCOUNTER,
    
    -- 5. Code: Extract SNOMED code
    res:type:coding[0]:code::string as "Code",
    
    -- 6. Description: Extract Display Name
    res:type:coding[0]:display::string as DESCRIPTION,

    -- 7. UDI: Extract distinctIdentifier
    res:distinctIdentifier::string as UDI,

    -- Metadata from landing table
    SRC_FILENAME as SOURCE_FILE_NAME

FROM FLATTENED_DEVICES;

-- 1. Automated Encounters Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.ENCOUNTERS (
    "Id", "Start", "Stop", "Patient", "Organization", "Provider", "Payer",
    "EncounterClass", "Code", "Description", "Base_Encounter_Cost",
    "Total_Claim_Cost", "Payer_Coverage", "ReasonCode", "ReasonDescription",
    SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    SELECT JSON_CONTENT, SRC_FILENAME FROM FHIR_BATCH
),
FLATTENED AS (
    SELECT entry.value:resource as res, SRC_FILENAME FROM NEW_BATCH, LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE res:resourceType::string = 'Encounter'
)
SELECT 
    regexp_replace(res:id::string, 'urn:uuid:', ''),
    to_timestamp_ntz(res:period:start::string),
    to_timestamp_ntz(res:period:end::string),
    regexp_replace(res:subject:reference::string, 'urn:uuid:', ''),
    CASE 
        WHEN res:serviceProvider:reference::string LIKE 'urn:uuid:%' 
            THEN regexp_replace(res:serviceProvider:reference::string, 'urn:uuid:', '')
        ELSE split_part(res:serviceProvider:reference::string, '|', 2)
    END,
    CASE 
        WHEN res:participant[0]:individual:reference::string LIKE 'urn:uuid:%' 
            THEN regexp_replace(res:participant[0]:individual:reference::string, 'urn:uuid:', '')
        ELSE split_part(res:participant[0]:individual:reference::string, '|', 2)
    END,
    NULL, -- Payer placeholder
    res:class:code::string,
    res:type[0]:coding[0]:code::string,
    res:type[0]:coding[0]:display::string,
    NULL, NULL, NULL, -- Cost placeholders
    res:reasonCode[0]:coding[0]:code::string,
    res:reasonCode[0]:coding[0]:display::string,
    SRC_FILENAME
FROM FLATTENED;

-- 2. Automated Imaging Studies Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.IMAGING_STUDIES (
    "Id", "Date", "Patient", "Encounter", "Series UID", "Body Site Code",
    "Body Site Description", "Modality Code", "Modality Description",
    "Instance UID", "SOP Code", "SOP Description", "Procedure Code",
    SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    SELECT JSON_CONTENT, SRC_FILENAME FROM FHIR_BATCH
),
FLATTENED AS (
    SELECT 
        entry.value:resource as res,
        series.value as series_val,
        instance.value as inst_val,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry,
    LATERAL FLATTEN(input => entry.value:resource:series) series,
    LATERAL FLATTEN(input => series.value:instance) instance
    WHERE entry.value:resource:resourceType::string = 'ImagingStudy'
)
SELECT 
    regexp_replace(res:id::string, 'urn:uuid:', ''),
    to_timestamp_ntz(res:started::string),
    regexp_replace(res:subject:reference::string, 'urn:uuid:', ''),
    regexp_replace(res:encounter:reference::string, 'urn:uuid:', ''),
    series_val:uid::string,
    series_val:bodySite:code::string,
    series_val:bodySite:display::string,
    series_val:modality:code::string,
    series_val:modality:display::string,
    inst_val:uid::string,
    inst_val:sopClass:code::string,
    inst_val:title::string,
    res:procedureCode[0]:coding[0]:code::string,
    SRC_FILENAME
FROM FLATTENED;

-- Automated Immunizations Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.IMMUNIZATIONS (
    "Date",
    PATIENT,
    ENCOUNTER,
    CODE,
    DESCRIPTION,
    COST,
    SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    -- Use the temporary table defined at the start of your procedure
    SELECT 
        JSON_CONTENT, 
        SRC_FILENAME 
    FROM FHIR_BATCH
),
FLATTENED_IMMUNIZATIONS AS (
    -- Flatten the bundle entries to find Immunization resources
    SELECT 
        entry.value:resource as res,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE entry.value:resource:resourceType::string = 'Immunization'
)
SELECT 
    -- 1. Date: Map occurrenceDateTime
    to_timestamp_ntz(res:occurrenceDateTime::string) as "Date",
    
    -- 2. Patient: Clean UUID
    regexp_replace(res:patient:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 3. Encounter: Clean UUID
    regexp_replace(res:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 4. Code: Extract CVX code
    res:vaccineCode:coding[0]:code::string as CODE,
    
    -- 5. Description: Extract Display Name or Text
    COALESCE(
        res:vaccineCode:text::string,
        res:vaccineCode:coding[0]:display::string
    ) as DESCRIPTION,

    -- 6. Cost: Not available in standard FHIR Immunization resource
    NULL as COST,

    -- Metadata from landing table
    SRC_FILENAME as SOURCE_FILE_NAME

FROM FLATTENED_IMMUNIZATIONS;

-- Automated Medications Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.MEDICATIONS (
    "Start", "Stop", PATIENT, PAYER, ENCOUNTER, CODE, DESCRIPTION,
    "Base_Cost", "Payer_Coverage", "Dispenses", "TotalCost",
    "ReasonCode", "ReasonDescription", SOURCE_FILE_NAME
)
WITH RAW_DATA AS (
    -- Access new bundles from the temporary batch table
    SELECT 
        SRC_FILENAME as FILENAME,
        value:resource:resourceType::string as RES_TYPE,
        regexp_replace(value:resource:id::string, 'urn:uuid:', '') as RES_ID,
        value:fullUrl::string as FULL_URL,
        value:resource as RES
    FROM FHIR_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry)
),
MED_DEFS AS (
    SELECT FULL_URL, RES:code:coding[0]:code::string as RX_CODE, RES:code:coding[0]:display::string as RX_DESC
    FROM RAW_DATA WHERE RES_TYPE = 'Medication'
),
CONDITIONS AS (
    SELECT FULL_URL, RES:code:coding[0]:code::string as SNOMED_CODE, RES:code:coding[0]:display::string as SNOMED_DESC
    FROM RAW_DATA WHERE RES_TYPE = 'Condition'
),
CLAIMS AS (
    SELECT 
        regexp_replace(RES:prescription:reference::string, 'urn:uuid:', '') as REF_MED_ID,
        RES:total:value::number(18,2) as COST,
        regexp_replace(RES:insurance[0]:coverage:reference::string, '#', '') as PAYER_ID
    FROM RAW_DATA 
    WHERE RES_TYPE = 'Claim' AND RES:type:coding[0]:code::string = 'pharmacy'
)
SELECT 
    try_to_date(substr(M.RES:authoredOn::string, 1, 10)) as "Start",
    try_to_date(substr(M.RES:dispenseRequest:validityPeriod:end::string, 1, 10)) as "Stop",
    regexp_replace(M.RES:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    C.PAYER_ID as PAYER,
    regexp_replace(M.RES:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    D.RX_CODE as CODE,
    D.RX_DESC as DESCRIPTION,
    C.COST as "Base_Cost",
    0.00 as "Payer_Coverage",
    COALESCE(M.RES:dispenseRequest:numberOfRepeatsAllowed::int, 1) as "Dispenses",
    C.COST as "TotalCost",
    COND.SNOMED_CODE as "ReasonCode",
    COND.SNOMED_DESC as "ReasonDescription",
    M.FILENAME
FROM RAW_DATA M
LEFT JOIN MED_DEFS D ON M.RES:medicationReference:reference::string = D.FULL_URL
LEFT JOIN CLAIMS C ON M.RES_ID = C.REF_MED_ID
LEFT JOIN CONDITIONS COND ON M.RES:reasonReference[0]:reference::string = COND.FULL_URL
WHERE M.RES_TYPE = 'MedicationRequest';

-- Automated Patients Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.PATIENTS (
    "Id", "BirthDate", "DeathDate", "SSN", "Drivers", "Passport",
    "Prefix", "First", "Middle", "Last", "Suffix", "Maiden",
    "Marital", "Race", "Ethnicity", "Gender", "BirthPlace",
    "Address", "City", "State", "County", "FIPS", "Zip", "Lat", "Lon",
    "Healthcare_Expenses", "Healthcare_Coverage", "Income",
    SOURCE_FILE_NAME
)
WITH RAW_PATIENTS AS (
    SELECT resource.value:resource as RES, SRC_FILENAME as FILENAME
    FROM FHIR_BATCH, LATERAL FLATTEN(input => JSON_CONTENT:entry) as resource
    WHERE resource.value:resource:resourceType::string = 'Patient'
),
PARSED_IDENTIFIERS AS (
    SELECT 
        RES:id::string as PAT_ID,
        MAX(CASE WHEN id.value:type:coding[0]:code::string = 'SS' THEN id.value:value::string END) as SSN,
        MAX(CASE WHEN id.value:type:coding[0]:code::string = 'DL' THEN id.value:value::string END) as DL,
        MAX(CASE WHEN id.value:type:coding[0]:code::string = 'PPN' THEN id.value:value::string END) as PP
    FROM RAW_PATIENTS, LATERAL FLATTEN(input => RES:identifier) as id
    GROUP BY PAT_ID
),
PARSED_NAMES AS (
    SELECT 
        RES:id::string as PAT_ID,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:prefix[0]::string END) as PREFIX,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:given[0]::string END) as FIRST_NAME,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:given[1]::string END) as MIDDLE_NAME,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:family::string END) as LAST_NAME,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:suffix[0]::string END) as SUFFIX,
        MAX(CASE WHEN nm.value:use::string = 'maiden' THEN nm.value:family::string END) as MAIDEN_NAME
    FROM RAW_PATIENTS, LATERAL FLATTEN(input => RES:name) as nm
    GROUP BY PAT_ID
),
PARSED_EXTENSIONS AS (
    SELECT
        RES:id::string as PAT_ID,
        MAX(CASE WHEN ext.value:url::string LIKE '%us-core-race' THEN ext.value:extension[0]:valueCoding:display::string END) as RACE,
        MAX(CASE WHEN ext.value:url::string LIKE '%us-core-ethnicity' THEN ext.value:extension[0]:valueCoding:display::string END) as ETHNICITY,
        MAX(CASE WHEN ext.value:url::string LIKE '%patient-birthPlace' THEN 
                concat(COALESCE(ext.value:valueAddress:city::string, ''), ', ', 
                       COALESCE(ext.value:valueAddress:state::string, ''), ', ', 
                       COALESCE(ext.value:valueAddress:country::string, ''))
            END) as BIRTHPLACE
    FROM RAW_PATIENTS, LATERAL FLATTEN(input => RES:extension) as ext
    GROUP BY PAT_ID
)
SELECT 
    regexp_replace(P.RES:id::string, 'urn:uuid:', ''),
    P.RES:birthDate::date,
    try_to_date(substr(P.RES:deceasedDateTime::string, 1, 10)),
    I.SSN, I.DL, I.PP,
    N.PREFIX, N.FIRST_NAME, N.MIDDLE_NAME, N.LAST_NAME, N.SUFFIX, N.MAIDEN_NAME,
    P.RES:maritalStatus:coding[0]:code::string,
    E.RACE, E.ETHNICITY,
    CASE 
        WHEN P.RES:gender::string = 'male' THEN 'M'
        WHEN P.RES:gender::string = 'female' THEN 'F'
        ELSE P.RES:gender::string 
    END,
    E.BIRTHPLACE,
    P.RES:address[0]:line[0]::string,
    P.RES:address[0]:city::string,
    P.RES:address[0]:state::string,
    NULL, NULL, -- County and FIPS placeholders
    P.RES:address[0]:postalCode::string,
    COALESCE(P.RES:address[0]:extension[0]:extension[0]:valueDecimal::number(18,6), P.RES:address[0]:extension[0]:valueDecimal::number(18,6)),
    COALESCE(P.RES:address[0]:extension[0]:extension[1]:valueDecimal::number(18,6), P.RES:address[0]:extension[1]:valueDecimal::number(18,6)),
    0.00, 0.00, 0.00, -- Financial placeholders
    P.FILENAME
FROM RAW_PATIENTS P
LEFT JOIN PARSED_IDENTIFIERS I ON P.RES:id::string = I.PAT_ID
LEFT JOIN PARSED_NAMES N ON P.RES:id::string = N.PAT_ID
LEFT JOIN PARSED_EXTENSIONS E ON P.RES:id::string = E.PAT_ID;

-- Automated Payer Transitions Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.PAYER_TRANSITIONS (
    PATIENT,
    "MEMBER ID",
    "START_YEAR",
    "END_YEAR",
    PAYER,
    "SECONDARY PAYER",
    OWNERSHIP,
    "OWNER NAME",
    SOURCE_FILE_NAME
)
WITH RAW_CLAIMS AS (
    -- Extract insurance details from Claims in the current batch
    SELECT 
        regexp_replace(resource.value:resource:patient:reference::string, 'urn:uuid:', '') as PATIENT_ID,
        resource.value:resource:patient:display::string as PATIENT_NAME,
        year(try_to_date(substr(resource.value:resource:billablePeriod:start::string, 1, 10))) as CLAIM_YEAR,
        COALESCE(
            resource.value:resource:insurance[0]:coverage:display::string,
            'NO_INSURANCE' 
        ) as PAYER_NAME,
        SRC_FILENAME
    FROM 
        FHIR_BATCH,
        LATERAL FLATTEN(input => JSON_CONTENT:entry) as resource
    WHERE 
        resource.value:resource:resourceType::string = 'Claim'
)
SELECT 
    PATIENT_ID,
    NULL as "MEMBER ID",
    MIN(CLAIM_YEAR) as "START_YEAR",
    MAX(CLAIM_YEAR) as "END_YEAR",
    PAYER_NAME as PAYER,
    NULL as "SECONDARY PAYER",
    'Self' as OWNERSHIP,
    PATIENT_NAME as "OWNER NAME",
    SRC_FILENAME as SOURCE_FILE_NAME
FROM RAW_CLAIMS
GROUP BY 
    PATIENT_ID, 
    PATIENT_NAME, 
    PAYER_NAME, 
    SRC_FILENAME;

-- Automated Procedures Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.PROCEDURES (
    "Start",
    "Stop",
    PATIENT,
    ENCOUNTER,
    CODE,
    DESCRIPTION,
    "Base_Cost",
    "ReasonCode",
    "ReasonDescription",
    SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    SELECT JSON_CONTENT, SRC_FILENAME FROM FHIR_BATCH
),
FLATTENED_PROCEDURES AS (
    -- Flatten bundle to find Procedure resources
    SELECT 
        resource.value:resource as res,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) as resource
    WHERE res:resourceType::string = 'Procedure'
)
SELECT 
    -- 1. Start: Map performedPeriod.start or performedDateTime
    to_timestamp_ntz(COALESCE(
        res:performedPeriod:start::string,
        res:performedDateTime::string
    )) as "Start",
    
    -- 2. Stop: Map performedPeriod.end
    to_timestamp_ntz(res:performedPeriod:end::string) as "Stop",
    
    -- 3. Patient: Clean UUID
    regexp_replace(res:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 4. Encounter: Clean UUID
    regexp_replace(res:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 5. Code: Extract SNOMED code
    res:code:coding[0]:code::string as CODE,
    
    -- 6. Description: Extract Display Name
    res:code:coding[0]:display::string as DESCRIPTION,

    -- 7. Base_Cost: Found in Claims, not standard Procedure resource
    NULL as "Base_Cost",

    -- 8. ReasonCode: Direct reason code
    res:reasonCode[0]:coding[0]:code::string as "ReasonCode",
    
    -- 9. ReasonDescription: extraction from reasonCode OR reasonReference display
    COALESCE(
        res:reasonCode[0]:coding[0]:display::string,
        res:reasonReference[0]:display::string
    ) as "ReasonDescription",

    -- Metadata from landing table
    SRC_FILENAME as SOURCE_FILE_NAME
FROM FLATTENED_PROCEDURES;


-- Automated Supplies Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.SUPPLIES (
    "Date",
    PATIENT,
    ENCOUNTER,
    CODE,
    DESCRIPTION,
    QUANTITY,
    SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    SELECT JSON_CONTENT, SRC_FILENAME FROM FHIR_BATCH
),
FLATTENED_SUPPLIES AS (
    -- Flatten bundle entries to find SupplyDelivery resources
    SELECT 
        entry.value:resource as res,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE entry.value:resource:resourceType::string = 'SupplyDelivery'
)
SELECT 
    -- 1. Date: Map occurrenceDateTime
    try_to_date(substr(res:occurrenceDateTime::string, 1, 10)) as "Date",
    
    -- 2. Patient: Clean UUID
    regexp_replace(res:patient:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 3. Encounter: Clean UUID (if present)
    regexp_replace(res:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 4. Code: Extract SNOMED code from suppliedItem
    res:suppliedItem:itemCodeableConcept:coding[0]:code::string as CODE,
    
    -- 5. Description: Extract Display Name
    res:suppliedItem:itemCodeableConcept:coding[0]:display::string as DESCRIPTION,

    -- 6. Quantity
    res:suppliedItem:quantity:value::number as QUANTITY,

    -- Metadata from landing table
    SRC_FILENAME as SOURCE_FILE_NAME
FROM FLATTENED_SUPPLIES;

-- Automated Organizations Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.ORGANIZATIONS (
    "Id", "Name", "Address", "City", "State", "Zip", 
    "Lat", "Lon", "Phone", "Revenue", "Utilization", SOURCE_FILE_NAME
)
WITH RAW_RESOURCES AS (
    -- Flatten the batch once
    SELECT 
        value:resource as RES, 
        value:resource:resourceType::string as RES_TYPE, 
        SRC_FILENAME as FILENAME
    FROM FHIR_BATCH, 
    LATERAL FLATTEN(input => JSON_CONTENT:entry)
),
ORG_BASE AS (
    -- Extract base fields
    SELECT 
        RES:id::string as ORG_ID, 
        RES:name::string as ORG_NAME, 
        RES:address[0]:line[0]::string as ADDRESS,
        RES:address[0]:city::string as CITY, 
        RES:address[0]:state::string as STATE, 
        RES:address[0]:postalCode::string as ZIP,
        RES:telecom[0]:value::string as PHONE, 
        FILENAME
    FROM RAW_RESOURCES 
    WHERE RES_TYPE = 'Organization'
),
ORG_EXTENSIONS AS (
    -- Flatten extensions in a separate step to avoid subquery errors
    SELECT 
        RES:id::string as ORG_ID,
        ext.value:valueInteger::number as UTIL_COUNT
    FROM RAW_RESOURCES, 
    LATERAL FLATTEN(input => RES:extension) ext
    WHERE RES_TYPE = 'Organization' 
    AND ext.value:url::string LIKE '%utilization-encounters-extension'
),
LOC_COORDS AS (
    -- Extract Location Coordinates
    SELECT 
        COALESCE(
             RES:managingOrganization:identifier:value::string, 
             regexp_replace(RES:managingOrganization:reference::string, '^(urn:uuid:|Organization/)', '')
        ) as LINK_ORG_ID,
        RES:position:latitude::number(18,6) as LAT, 
        RES:position:longitude::number(18,6) as LON
    FROM RAW_RESOURCES 
    WHERE RES_TYPE = 'Location'
)
SELECT 
    base.ORG_ID, 
    base.ORG_NAME, 
    base.ADDRESS, 
    base.CITY, 
    base.STATE, 
    base.ZIP, 
    loc.LAT, 
    loc.LON, 
    base.PHONE, 
    NULL, -- Revenue
    COALESCE(ext.UTIL_COUNT, 0), 
    base.FILENAME
FROM ORG_BASE base
LEFT JOIN ORG_EXTENSIONS ext ON base.ORG_ID = ext.ORG_ID
LEFT JOIN LOC_COORDS loc ON base.ORG_ID = loc.LINK_ORG_ID;

-- Automated Claims Transactions Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.CLAIMS_TRANSACTIONS (
    "Id", "Claim ID", "Charge ID", "Patient ID", "Type", "Amount", "Method",
    "From Date", "To Date", "Place of Service", "Procedure Code",
    "DiagnosisRef1", "DiagnosisRef2", "DiagnosisRef3", "DiagnosisRef4",
    "Units", "Department ID", "Notes", "Unit Amount", "Payments",
    "Adjustments", "Transfers", "Outstanding", "Appointment ID",
    "Patient Insurance ID", "Fee Schedule ID", "Provider ID",
    "Supervising Provider ID", SOURCE_FILE_NAME
)
WITH NEW_BATCH AS (
    SELECT JSON_CONTENT, SRC_FILENAME FROM FHIR_BATCH
),
FLATTENED_ITEMS AS (
    -- Flatten the bundle to find Claim resources and then flatten the items array
    SELECT 
        entry.value:resource as res,
        item.value as item_val,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry,
    LATERAL FLATTEN(input => entry.value:resource:item) item
    WHERE entry.value:resource:resourceType::string = 'Claim'
)
SELECT 
    -- 1. Id: Unique ID (ClaimUUID + LineSequence)
    concat(regexp_replace(res:id::string, 'urn:uuid:', ''), '-', item_val:sequence::string),
    
    -- 2. Claim ID
    regexp_replace(res:id::string, 'urn:uuid:', ''),
    
    -- 3. Charge ID
    item_val:sequence::number,
    
    -- 4. Patient ID
    regexp_replace(res:patient:reference::string, 'urn:uuid:', ''),
    
    -- 5. Type
    'CHARGE',
    
    -- 6. Amount
    item_val:net:value::number(18,2),
    
    -- 7. Method
    NULL,

    -- 8-9. Dates
    try_to_date(substr(item_val:servicedPeriod:start::string, 1, 10)),
    try_to_date(substr(item_val:servicedPeriod:end::string, 1, 10)),

    -- 10. Place of Service
    COALESCE(
        item_val:locationCodeableConcept:coding[0]:code::string,
        regexp_replace(item_val:locationReference:reference::string, 'urn:uuid:', '')
    ),

    -- 11. Procedure Code
    item_val:productOrService:coding[0]:code::string,

    -- 12-15. Diagnosis References
    item_val:diagnosisSequence[0]::number,
    item_val:diagnosisSequence[1]::number,
    item_val:diagnosisSequence[2]::number,
    item_val:diagnosisSequence[3]::number,

    -- 16. Units
    COALESCE(item_val:quantity:value::number, 1),

    -- 17. Department ID
    NULL,

    -- 18. Notes
    LEFT(item_val:productOrService:coding[0]:display::string, 100),

    -- 19. Unit Amount
    COALESCE(item_val:unitPrice:value::number(18,2), item_val:net:value::number(18,2)),

    -- 20-23. Financials
    0.00, 0.00, 0.00,
    item_val:net:value::number(18,2),

    -- 24. Appointment ID
    regexp_replace(item_val:encounter[0]:reference::string, 'urn:uuid:', ''),

    -- 25. Patient Insurance ID
    regexp_replace(res:insurance[0]:coverage:reference::string, '#', ''),

    -- 26. Fee Schedule ID
    1,

    -- 27. Provider ID
    CASE 
        WHEN res:provider:reference::string LIKE 'urn:uuid:%' 
            THEN regexp_replace(res:provider:reference::string, 'urn:uuid:', '')
        ELSE split_part(res:provider:reference::string, '|', 2)
    END,

    -- 28. Supervising Provider ID
    regexp_replace(res:careTeam[0]:provider:reference::string, 'urn:uuid:', ''),

    -- Metadata
    SRC_FILENAME
FROM FLATTENED_ITEMS;

-- Automated Providers Insert
-- Automated Providers Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.PROVIDERS (
    "Id", "Organization", "Name", "Gender", "Speciality", "Address", "City", 
    "State", "Zip", "Lat", "Lon", "Encounters", "Procedures", SOURCE_FILE_NAME
)
WITH RAW_RESOURCES AS (
    SELECT 
        value:resource as RES, 
        value:resource:resourceType::string as RES_TYPE, 
        SRC_FILENAME as FILENAME
    FROM FHIR_BATCH, 
    LATERAL FLATTEN(input => JSON_CONTENT:entry)
),
PRACTITIONER_BASE AS (
    -- Extract basic demographics
    SELECT 
        RES:identifier[0]:value::string as PRAC_ID,
        concat(
            COALESCE(RES:name[0]:prefix[0]::string, ''), ' ', 
            RES:name[0]:given[0]::string, ' ', 
            RES:name[0]:family::string
        ) as FULL_NAME,
        CASE 
            WHEN RES:gender::string = 'male' THEN 'M' 
            WHEN RES:gender::string = 'female' THEN 'F' 
            ELSE RES:gender::string 
        END as GENDER_CODE,
        RES:address[0]:line[0]::string as ADDRESS, 
        RES:address[0]:city::string as CITY,
        RES:address[0]:state::string as STATE, 
        RES:address[0]:postalCode::string as ZIP, 
        FILENAME
    FROM RAW_RESOURCES 
    WHERE RES_TYPE = 'Practitioner'
),
PRACTITIONER_EXTENSIONS AS (
    -- Extract and Pivot extensions using aggregation
    SELECT 
        RES:identifier[0]:value::string as PRAC_ID,
        MAX(CASE WHEN ext.value:url::string LIKE '%utilization-encounters-extension' THEN ext.value:valueInteger::number END) as ENCOUNTERS,
        MAX(CASE WHEN ext.value:url::string LIKE '%utilization-procedures-extension' THEN ext.value:valueInteger::number END) as PROCEDURES
    FROM RAW_RESOURCES, 
    LATERAL FLATTEN(input => RES:extension) ext
    WHERE RES_TYPE = 'Practitioner'
    GROUP BY 1
),
ROLE_DATA AS (
    -- Link Practitioner to Organization
    SELECT 
        RES:practitioner:identifier:value::string as LINK_ID, 
        RES:organization:identifier:value::string as ORG_ID,
        RES:specialty[0]:text::string as SPECIALTY
    FROM RAW_RESOURCES 
    WHERE RES_TYPE = 'PractitionerRole'
)
SELECT 
    base.PRAC_ID, 
    role.ORG_ID, 
    base.FULL_NAME, 
    base.GENDER_CODE, 
    role.SPECIALTY,
    base.ADDRESS, 
    base.CITY, 
    base.STATE, 
    base.ZIP, 
    NULL, NULL, -- Lat/Lon
    COALESCE(ext.ENCOUNTERS, 0), 
    COALESCE(ext.PROCEDURES, 0), 
    base.FILENAME
FROM PRACTITIONER_BASE base
LEFT JOIN PRACTITIONER_EXTENSIONS ext ON base.PRAC_ID = ext.PRAC_ID
LEFT JOIN ROLE_DATA role ON base.PRAC_ID = role.LINK_ID;

-- Automated Payers (Coverage) Insert
INSERT INTO SYNTHEA_HOSPITAL.RAW_DATA.PAYERS (
    "Id", 
    "Payer_Name", 
    "Payer_Organization_ID", 
    "Patient_ID", 
    "Subscriber_ID", 
    "Member_ID",
    "Coverage_Type", 
    "Start_Date", 
    "End_Date", 
    "Status", 
    "Source_File_Name"
)
WITH NEW_BATCH AS (
    SELECT JSON_CONTENT, SRC_FILENAME FROM FHIR_BATCH
),
FLATTENED_COVERAGE AS (
    -- Flatten bundle to find Coverage resources
    SELECT 
        entry.value:resource as res,
        SRC_FILENAME
    FROM NEW_BATCH,
    LATERAL FLATTEN(input => JSON_CONTENT:entry) entry
    WHERE entry.value:resource:resourceType::string = 'Coverage'
)
SELECT 
    -- 1. Id: Coverage UUID
    regexp_replace(res:id::string, 'urn:uuid:', '') as "Id",
    
    -- 2. Payer Name: Display name from the payor array
    res:payor[0]:display::string as "Payer_Name",
    
    -- 3. Payer Org ID: Clean UUID from the reference
    regexp_replace(res:payor[0]:reference::string, '^(urn:uuid:|Organization/)', '') as "Payer_Organization_ID",
    
    -- 4. Patient ID: Beneficiary reference
    regexp_replace(res:beneficiary:reference::string, 'urn:uuid:', '') as "Patient_ID",
    
    -- 5. Subscriber ID: Often matches the Member ID in Synthea
    res:subscriberId::string as "Subscriber_ID",
    
    -- 6. Member ID: Extracted from identifier array
    res:identifier[0]:value::string as "Member_ID",

    -- 7. Coverage Type: e.g., 'PPO', 'HMO' (extracted from type coding or class)
    COALESCE(
        res:type:coding[0]:display::string,
        res:type:text::string
    ) as "Coverage_Type",
    
    -- 8. Start Date: Policy start
    try_to_date(substr(res:period:start::string, 1, 10)) as "Start_Date",
    
    -- 9. End Date: Policy end
    try_to_date(substr(res:period:end::string, 1, 10)) as "End_Date",
    
    -- 10. Status: e.g., 'active', 'draft'
    res:status::string as "Status",

    -- Metadata
    SRC_FILENAME as "Source_File_Name"

FROM FLATTENED_COVERAGE;
end;
$$

-- initiation
show pipes;
desc pipe fhir_ingestion_pipe;
ALTER TASK AUTOMATED_FHIR_TRANSFORM RESUME;
ALTER TASK AUTOMATED_FHIR_TRANSFORM suspend;
desc task automated_fhir_transform;
select * from RAW_LANDING_ZONE;


-- task status
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'AUTOMATED_FHIR_TRANSFORM'
ORDER BY QUERY_START_TIME DESC;


SELECT 
    FILE_NAME, 
    STATUS, 
    FIRST_ERROR_MESSAGE, 
    FIRST_ERROR_LINE_NUMBER,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SYNTHEA_HOSPITAL.RAW_DATA.RAW_LANDING_ZONE', 
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE STATUS != 'LOADED'  -- This filters for failed or partially loaded files
ORDER BY LAST_LOAD_TIME DESC;


call fhir_insert_script_procedure();