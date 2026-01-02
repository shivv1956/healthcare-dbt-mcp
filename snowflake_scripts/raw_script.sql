USE DATABASE SYNTHEA_HOSPITAL;
USE SCHEMA RAW_DATA;
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE; -- Synthea bundles are objects containing an array
  

CREATE TABLE IF NOT EXISTS OBSERVATIONS (
    DATE DATE,
    PATIENT VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    CATEGORY VARCHAR(255),
    CODE VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    VALUE VARCHAR(255),
    UNITS VARCHAR(255),
    TYPE VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS ALLERGIES (
    "START" DATE,
    "STOP" DATE,
    PATIENT VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    CODE VARCHAR(255),
    "SYSTEM" VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    "TYPE" VARCHAR(50),
    CATEGORY VARCHAR(50),
    REACTION1 VARCHAR(255),
    DESCRIPTION1 VARCHAR(255),
    SEVERITY1 VARCHAR(50),
    REACTION2 VARCHAR(255),
    DESCRIPTION2 VARCHAR(255),
    SEVERITY2 VARCHAR(50),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE IF NOT EXISTS CAREPLANS (
    ID VARCHAR(255),
    "START" DATE,
    "STOP" DATE,
    PATIENT VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    CODE VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    REASONCODE VARCHAR(255),
    REASONDESCRIPTION VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



CREATE TABLE IF NOT EXISTS CLAIMS (
    "Id" VARCHAR(255),
    "Patient ID" VARCHAR(255),
    "Provider ID" VARCHAR(255),
    "Primary Patient Insurance ID" VARCHAR(255),
    "Secondary Patient Insurance ID" VARCHAR(255),
    "Department ID" VARCHAR(255),
    "Patient Department ID" VARCHAR(255),
    "Diagnosis1" VARCHAR(255),
    "Diagnosis2" VARCHAR(255),
    "Diagnosis3" VARCHAR(255),
    "Diagnosis4" VARCHAR(255),
    "Diagnosis5" VARCHAR(255),
    "Diagnosis6" VARCHAR(255),
    "Diagnosis7" VARCHAR(255),
    "Diagnosis8" VARCHAR(255),
    "Referring Provider ID" VARCHAR(255),
    "Appointment ID" VARCHAR(255),
    "Current Illness Date" TIMESTAMP_NTZ,
    "Service Date" TIMESTAMP_NTZ,
    "Supervising Provider ID" VARCHAR(255),
    "Status1" VARCHAR(50),
    "Status2" VARCHAR(50),
    "StatusP" VARCHAR(50),
    "Outstanding1" NUMBER(18, 2),
    "Outstanding2" NUMBER(18, 2),
    "OutstandingP" NUMBER(18, 2),
    "LastBilledDate1" TIMESTAMP_NTZ,
    "LastBilledDate2" TIMESTAMP_NTZ,
    "LastBilledDateP" TIMESTAMP_NTZ,
    "HealthcareClaimTypeID1" NUMBER,
    "HealthcareClaimTypeID2" NUMBER,
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE IF NOT EXISTS CLAIMS_TRANSACTIONS (
    "Id" VARCHAR(255),
    "Claim ID" VARCHAR(255),
    "Charge ID" NUMBER,
    "Patient ID" VARCHAR(255),
    "Type" VARCHAR(50),
    "Amount" NUMBER(18, 2),
    "Method" VARCHAR(50),
    "From Date" DATE,
    "To Date" DATE,
    "Place of Service" VARCHAR(255),
    "Procedure Code" VARCHAR(255),
    "Modifier1" VARCHAR(255),
    "Modifier2" VARCHAR(255),
    "DiagnosisRef1" NUMBER,
    "DiagnosisRef2" NUMBER,
    "DiagnosisRef3" NUMBER,
    "DiagnosisRef4" NUMBER,
    "Units" NUMBER,
    "Department ID" NUMBER,
    "Notes" VARCHAR(255),
    "Unit Amount" NUMBER(18, 2),
    "Transfer Out ID" NUMBER,
    "Transfer Type" VARCHAR(50),
    "Payments" NUMBER(18, 2),
    "Adjustments" NUMBER(18, 2),
    "Transfers" NUMBER(18, 2),
    "Outstanding" NUMBER(18, 2),
    "Appointment ID" VARCHAR(255),
    "Line Note" VARCHAR(255),
    "Patient Insurance ID" VARCHAR(255),
    "Fee Schedule ID" NUMBER,
    "Provider ID" VARCHAR(255),
    "Supervising Provider ID" VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CONDITIONS (
    "START" DATE,
    "STOP" DATE,
    PATIENT VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    CODE VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DEVICES (
    "Start" TIMESTAMP_NTZ,
    "Stop" TIMESTAMP_NTZ,
    PATIENT VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    "Code" VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    UDI VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ENCOUNTERS (
    "Id" VARCHAR(255),
    "Start" TIMESTAMP_NTZ,
    "Stop" TIMESTAMP_NTZ,
    "Patient" VARCHAR(255),
    "Organization" VARCHAR(255),
    "Provider" VARCHAR(255),
    "Payer" VARCHAR(255),
    "EncounterClass" VARCHAR(255),
    "Code" VARCHAR(255),
    "Description" VARCHAR(255),
    "Base_Encounter_Cost" NUMBER(18, 2),
    "Total_Claim_Cost" NUMBER(18, 2),
    "Payer_Coverage" NUMBER(18, 2),
    "ReasonCode" VARCHAR(255),
    "ReasonDescription" VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE IF NOT EXISTS IMAGING_STUDIES (
    "Id" VARCHAR(255),
    "Date" TIMESTAMP_NTZ,
    "Patient" VARCHAR(255),
    "Encounter" VARCHAR(255),
    "Series UID" VARCHAR(255),
    "Body Site Code" VARCHAR(255),
    "Body Site Description" VARCHAR(255),
    "Modality Code" VARCHAR(255),
    "Modality Description" VARCHAR(255),
    "Instance UID" VARCHAR(255),
    "SOP Code" VARCHAR(255),
    "SOP Description" VARCHAR(255),
    "Procedure Code" VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS IMMUNIZATIONS (
    "Date" TIMESTAMP_NTZ,
    PATIENT VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    CODE VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    COST NUMBER(18, 2),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MEDICATIONS (
    "Start" DATE,
    "Stop" DATE,
    PATIENT VARCHAR(255),
    PAYER VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    CODE VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    "Base_Cost" NUMBER(18, 2),
    "Payer_Coverage" NUMBER(18, 2),
    "Dispenses" NUMBER,
    "TotalCost" NUMBER(18, 2),
    "ReasonCode" VARCHAR(255),
    "ReasonDescription" VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- org. data fetched from hospitalinformation
CREATE TABLE IF NOT EXISTS ORGANIZATIONS (
    "Id" VARCHAR(255),
    "Name" VARCHAR(255),
    "Address" VARCHAR(255),
    "City" VARCHAR(255),
    "State" VARCHAR(255),
    "Zip" VARCHAR(255),
    "Lat" NUMBER(18, 6),
    "Lon" NUMBER(18, 6),
    "Phone" VARCHAR(255),
    "Revenue" NUMBER(18, 2),
    "Utilization" NUMBER,
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS PATIENTS (
    "Id" VARCHAR(255),
    "BirthDate" DATE,
    "DeathDate" DATE,
    "SSN" VARCHAR(50),
    "Drivers" VARCHAR(50),
    "Passport" VARCHAR(50),
    "Prefix" VARCHAR(50),
    "First" VARCHAR(255),
    "Middle" VARCHAR(255),
    "Last" VARCHAR(255),
    "Suffix" VARCHAR(50),
    "Maiden" VARCHAR(255),
    "Marital" VARCHAR(50),
    "Race" VARCHAR(50),
    "Ethnicity" VARCHAR(50),
    "Gender" VARCHAR(50),
    "BirthPlace" VARCHAR(255),
    "Address" VARCHAR(255),
    "City" VARCHAR(255),
    "State" VARCHAR(255),
    "County" VARCHAR(255),
    "FIPS" VARCHAR(50),
    "Zip" VARCHAR(50),
    "Lat" NUMBER(18, 6),
    "Lon" NUMBER(18, 6),
    "Healthcare_Expenses" NUMBER(18, 2),
    "Healthcare_Coverage" NUMBER(18, 2),
    "Income" NUMBER(18, 2),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SYNTHEA_HOSPITAL.RAW_DATA.PAYERS (
    "Id" VARCHAR(255),
    "Payer_Name" VARCHAR(255),
    "Payer_Organization_ID" VARCHAR(255),
    "Patient_ID" VARCHAR(255),
    "Subscriber_ID" VARCHAR(255),
    "Member_ID" VARCHAR(255),
    "Coverage_Type" VARCHAR(100),
    "Start_Date" DATE,
    "End_Date" DATE,
    "Status" VARCHAR(50),
    "Source_File_Name" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS PAYER_TRANSITIONS (
    PATIENT VARCHAR(255),
    "MEMBER ID" VARCHAR(255),
    "START_YEAR" INTEGER,
    "END_YEAR" INTEGER,
    PAYER VARCHAR(255),
    "SECONDARY PAYER" VARCHAR(255),
    OWNERSHIP VARCHAR(50),
    "OWNER NAME" VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- PAYER table not created


CREATE TABLE IF NOT EXISTS PROCEDURES (
    "Start" TIMESTAMP_NTZ,
    "Stop" TIMESTAMP_NTZ,
    PATIENT VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    CODE VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    "Base_Cost" NUMBER(18, 2),
    "ReasonCode" VARCHAR(255),
    "ReasonDescription" VARCHAR(255),
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- provider data fetched from practitionerInformation
CREATE TABLE IF NOT EXISTS PROVIDERS (
    "Id" VARCHAR(255),
    "Organization" VARCHAR(255),
    "Name" VARCHAR(255),
    "Gender" VARCHAR(50),
    "Speciality" VARCHAR(255),
    "Address" VARCHAR(255),
    "City" VARCHAR(255),
    "State" VARCHAR(50),
    "Zip" VARCHAR(50),
    "Lat" NUMBER(18, 6),
    "Lon" NUMBER(18, 6),
    "Encounters" NUMBER DEFAULT 0,
    "Procedures" NUMBER DEFAULT 0,
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE SUPPLIES (
    "Date" DATE,
    PATIENT VARCHAR(255),
    ENCOUNTER VARCHAR(255),
    CODE VARCHAR(255),
    DESCRIPTION VARCHAR(255),
    QUANTITY NUMBER,
    SOURCE_FILE_NAME VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

--- Data Insertions

INSERT INTO ALLERGIES (
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
SELECT 
    -- 1. START: Diagnosis Date
    try_to_date(substr(COALESCE(resource.value:resource:onsetDateTime::string, resource.value:resource:recordedDate::string), 1, 10)) as "START",
    
    -- 2. STOP: End Date (if applicable)
    try_to_date(substr(resource.value:resource:abatementDateTime::string, 1, 10)) as "STOP",
    
    -- 3. PATIENT: Clean UUID
    regexp_replace(resource.value:resource:patient:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 4. ENCOUNTER: Clean UUID
    regexp_replace(resource.value:resource:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 5. CODE: Allergy Code
    resource.value:resource:code:coding[0]:code::string as CODE,
    
    -- 6. SYSTEM: Determine RxNorm vs SNOMED based on the URL
    CASE 
        WHEN resource.value:resource:code:coding[0]:system::string ILIKE '%rxnorm%' THEN 'RxNorm'
        ELSE 'SNOMED-CT' 
    END as "SYSTEM",
    
    -- 7. DESCRIPTION
    resource.value:resource:code:text::string as DESCRIPTION,

    -- 8. TYPE (allergy vs intolerance)
    resource.value:resource:type::string as "TYPE",

    -- 9. CATEGORY (food, medication, etc.) - Taking the first one if array
    resource.value:resource:category[0]::string as CATEGORY,

    -- 10. REACTION1 Code
    resource.value:resource:reaction[0]:manifestation[0]:coding[0]:code::string as REACTION1,

    -- 11. DESCRIPTION1
    resource.value:resource:reaction[0]:manifestation[0]:coding[0]:display::string as DESCRIPTION1,

    -- 12. SEVERITY1
    resource.value:resource:reaction[0]:severity::string as SEVERITY1,

    -- 13. REACTION2 Code (Extract from second element of reaction array)
    resource.value:resource:reaction[1]:manifestation[0]:coding[0]:code::string as REACTION2,

    -- 14. DESCRIPTION2
    resource.value:resource:reaction[1]:manifestation[0]:coding[0]:display::string as DESCRIPTION2,

    -- 15. SEVERITY2
    resource.value:resource:reaction[1]:severity::string as SEVERITY2,

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @FHIR_STAGE (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource
WHERE 
    resource.value:resource:resourceType::string = 'AllergyIntolerance';


list @fhir_stage;



INSERT INTO OBSERVATIONS (DATE, PATIENT, ENCOUNTER, CATEGORY, CODE, DESCRIPTION, VALUE, UNITS, TYPE, SOURCE_FILE_NAME)
WITH JSON_DATA AS (
    SELECT 
        $1 as full_json,
        metadata$filename as filename
    FROM @FHIR_STAGE (file_format => 'JSON_FORMAT')
),
FLATTENED_RESOURCES AS (
    SELECT 
        filename,
        value:resource as resource
    FROM JSON_DATA,
    LATERAL FLATTEN(input => full_json:entry)
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
    -- Determine Type based on which field is present
    CASE 
        WHEN resource:valueQuantity IS NOT NULL THEN 'numeric'
        WHEN resource:valueCodeableConcept IS NOT NULL THEN 'text'
        WHEN resource:valueString IS NOT NULL THEN 'text'
        ELSE 'unknown'
    END as TYPE,
    filename as SOURCE_FILE_NAME
FROM FLATTENED_RESOURCES
WHERE resource:component IS NULL -- Only select observations WITHOUT sub-components

UNION ALL

-- PART 2: Component Observations (Blood Pressure, Surveys)
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
    filename as SOURCE_FILE_NAME
FROM FLATTENED_RESOURCES,
LATERAL FLATTEN(input => resource:component) as comp;



-- Note that in FHIR, the "Reason" for a CarePlan is often a reference to a Condition (Diagnosis). 
-- Synthea often creates CarePlans without a specific reason code listed directly in the resource, so those fields may appear as NULL depending on the specific care plan type (e.g., general wellness vs. disease management).

INSERT INTO CAREPLANS (ID, "START", "STOP", PATIENT, ENCOUNTER, CODE, DESCRIPTION, REASONCODE, REASONDESCRIPTION, SOURCE_FILE_NAME)
SELECT 
    -- 1. ID: Clean the UUID
    regexp_replace(resource.value:resource:id::string, 'urn:uuid:', '') as ID,
    
    -- 2. START: Care Plan Start Date
    try_to_date(substr(resource.value:resource:period:start::string, 1, 10)) as "START",
    
    -- 3. STOP: Care Plan End Date
    try_to_date(substr(resource.value:resource:period:end::string, 1, 10)) as "STOP",
    
    -- 4. PATIENT: Clean Patient UUID
    regexp_replace(resource.value:resource:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 5. ENCOUNTER: Clean Encounter UUID
    regexp_replace(resource.value:resource:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 6. CODE: Extract the SNOMED code (usually the second category in Synthea)
    COALESCE(
        resource.value:resource:category[1]:coding[0]:code::string, -- Standard Synthea location
        resource.value:resource:category[0]:coding[0]:code::string  -- Fallback
    ) as CODE,
    
    -- 7. DESCRIPTION: Extract the display text
    COALESCE(
        resource.value:resource:category[1]:coding[0]:display::string,
        resource.value:resource:category[0]:coding[0]:display::string
    ) as DESCRIPTION,

    -- 8. REASONCODE: Check 'reasonCode' array or 'addresses' reference
    -- Note: Synthea FHIR often links to a Condition UUID via 'addresses', making extraction of the raw code difficult in one pass. 
    -- We attempt to grab direct codes if present.
    resource.value:resource:reasonCode[0]:coding[0]:code::string as REASONCODE,
    
    -- 9. REASONDESCRIPTION: 
    resource.value:resource:reasonCode[0]:coding[0]:display::string as REASONDESCRIPTION,

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @FHIR_STAGE (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource
WHERE 
    resource.value:resource:resourceType::string = 'CarePlan';

INSERT INTO CLAIMS (
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
    -- First, grab the raw JSON resource for every Claim
    SELECT 
        regexp_replace(resource.value:resource:id::string, 'urn:uuid:', '') as CLAIM_UUID,
        resource.value:resource as RES,
        metadata$filename as FILENAME
    FROM 
        @fhir_stage (file_format => 'JSON_FORMAT') as S,
        LATERAL FLATTEN(input => S.$1:entry) as resource
    WHERE 
        resource.value:resource:resourceType::string = 'Claim'
),
DIAGNOSIS_MAP AS (
    -- Flatten the 'item' array, then flatten the 'diagnosisSequence' array within it
    -- This links a specific item (and its code) to a specific diagnosis sequence number (1, 2, etc.)
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

    -- 6-13. Diagnosis 1-8 (Mapped from the CTE above)
    -- We filter the mapped codes by their sequence number
    MAX(CASE WHEN D.SEQ_ID = 1 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 2 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 3 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 4 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 5 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 6 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 7 THEN D.CODE END),
    MAX(CASE WHEN D.SEQ_ID = 8 THEN D.CODE END),

    -- 14. Appointment ID (Encounter)
    -- Extracting reference from the first item's encounter array
    regexp_replace(R.RES:item[0]:encounter[0]:reference::string, 'urn:uuid:', ''),

    -- 15. Current Illness Date (Created date)
    to_timestamp_ntz(R.RES:created::string),

    -- 16. Service Date (Billable Period Start)
    to_timestamp_ntz(R.RES:billablePeriod:start::string),

    -- 17. Supervising Provider ID
    regexp_replace(R.RES:careTeam[0]:provider:reference::string, 'urn:uuid:', ''),

    -- 18. Status1
    R.RES:status::string,

    -- 19. Outstanding1 (Total Cost)
    R.RES:total:value::number(18,2),

    -- 20. HealthcareClaimTypeID1
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


INSERT INTO CLAIMS_TRANSACTIONS (
    "Id",
    "Claim ID",
    "Charge ID",
    "Patient ID",
    "Type",
    "Amount",
    "Method",
    "From Date",
    "To Date",
    "Place of Service",
    "Procedure Code",
    "DiagnosisRef1",
    "DiagnosisRef2",
    "DiagnosisRef3",
    "DiagnosisRef4",
    "Units",
    "Department ID",
    "Notes",
    "Unit Amount",
    "Payments",
    "Adjustments",
    "Transfers",
    "Outstanding",
    "Appointment ID",
    "Patient Insurance ID",
    "Fee Schedule ID",
    "Provider ID",
    "Supervising Provider ID",
    SOURCE_FILE_NAME
)
SELECT 
    -- 1. Id: Generate a unique ID for the transaction (ClaimUUID + LineSequence)
    concat(regexp_replace(resource.value:resource:id::string, 'urn:uuid:', ''), '-', item.value:sequence::string) as "Id",
    
    -- 2. Claim ID
    regexp_replace(resource.value:resource:id::string, 'urn:uuid:', '') as "Claim ID",
    
    -- 3. Charge ID: Use the sequence number
    item.value:sequence::number as "Charge ID",
    
    -- 4. Patient ID
    regexp_replace(resource.value:resource:patient:reference::string, 'urn:uuid:', '') as "Patient ID",
    
    -- 5. Type: Hardcoded to 'CHARGE' for initial claim lines (common Synthea pattern)
    'CHARGE' as "Type",
    
    -- 6. Amount: Line item net amount
    item.value:net:value::number(18,2) as "Amount",
    
    -- 7. Method: Payment method (often NULL in pure Claims, found in EOBs)
    NULL as "Method",

    -- 8. From Date: Serviced Period Start
    try_to_date(substr(item.value:servicedPeriod:start::string, 1, 10)) as "From Date",
    
    -- 9. To Date: Serviced Period End
    try_to_date(substr(item.value:servicedPeriod:end::string, 1, 10)) as "To Date",

    -- 10. Place of Service: Location Code (e.g., 21 for Inpatient) or UUID
    COALESCE(
        item.value:locationCodeableConcept:coding[0]:code::string,
        regexp_replace(item.value:locationReference:reference::string, 'urn:uuid:', '')
    ) as "Place of Service",

    -- 11. Procedure Code: Extract the actual code (SNOMED/CPT)
    item.value:productOrService:coding[0]:code::string as "Procedure Code",

    -- 12-15. DiagnosisRef 1-4: Extract sequence IDs from diagnosisSequence array
    item.value:diagnosisSequence[0]::number as "DiagnosisRef1",
    item.value:diagnosisSequence[1]::number as "DiagnosisRef2",
    item.value:diagnosisSequence[2]::number as "DiagnosisRef3",
    item.value:diagnosisSequence[3]::number as "DiagnosisRef4",

    -- 16. Units: Default to 1 if not specified (Synthea often omits quantity for simple visits)
    COALESCE(item.value:quantity:value::number, 1) as "Units",

    -- 17. Department ID
    NULL as "Department ID",

    -- 18. Notes: Procedure Display Name -- will take only first 100 characters
    LEFT(item.value:productOrService:coding[0]:display::string, 100) AS "Notes",

    -- 19. Unit Amount: Calculated or Net amount
    COALESCE(item.value:unitPrice:value::number(18,2), item.value:net:value::number(18,2)) as "Unit Amount",

    -- 20-23. Financials (Payments, Adjustments, Transfers, Outstanding)
    -- Initial Claims usually show the full charge as outstanding or 0 payments until adjudicated
    0.00 as "Payments",
    0.00 as "Adjustments",
    0.00 as "Transfers",
    item.value:net:value::number(18,2) as "Outstanding",

    -- 24. Appointment ID: Encounter UUID from line item
    regexp_replace(item.value:encounter[0]:reference::string, 'urn:uuid:', '') as "Appointment ID",

    -- 25. Patient Insurance ID: Coverage Reference
    regexp_replace(resource.value:resource:insurance[0]:coverage:reference::string, '#', '') as "Patient Insurance ID",

    -- 26. Fee Schedule ID: Fixed to 1 as per description
    1 as "Fee Schedule ID",

    -- 27. Provider ID
    CASE 
        WHEN resource.value:resource:provider:reference::string LIKE 'urn:uuid:%' 
            THEN regexp_replace(resource.value:resource:provider:reference::string, 'urn:uuid:', '')
        ELSE split_part(resource.value:resource:provider:reference::string, '|', 2)
    END as "Provider ID",

    -- 28. Supervising Provider ID
    regexp_replace(resource.value:resource:careTeam[0]:provider:reference::string, 'urn:uuid:', '') as "Supervising Provider ID",

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @FHIR_STAGE (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource,
    LATERAL FLATTEN(input => resource.value:resource:item) as item
WHERE 
    resource.value:resource:resourceType::string = 'Claim';



INSERT INTO CONDITIONS (
    "START", 
    "STOP", 
    PATIENT, 
    ENCOUNTER, 
    CODE, 
    DESCRIPTION, 
    SOURCE_FILE_NAME
)
SELECT 
    -- 1. START: Map onsetDateTime or recordedDate to START
    try_to_date(substr(COALESCE(resource.value:resource:onsetDateTime::string, resource.value:resource:recordedDate::string), 1, 10)) as "START",
    
    -- 2. STOP: Map abatementDateTime to STOP (NULL if active/resolved without date)
    try_to_date(substr(resource.value:resource:abatementDateTime::string, 1, 10)) as "STOP",
    
    -- 3. PATIENT: Extract UUID from reference (remove 'urn:uuid:')
    regexp_replace(resource.value:resource:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 4. ENCOUNTER: Extract UUID from reference (remove 'urn:uuid:')
    regexp_replace(resource.value:resource:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 5. CODE: Extract SNOMED code
    resource.value:resource:code:coding[0]:code::string as CODE,
    
    -- 6. DESCRIPTION: Extract Display Name
    resource.value:resource:code:coding[0]:display::string as DESCRIPTION,

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @fhir_stage (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource
WHERE 
    -- Filter specifically for Condition resources
    resource.value:resource:resourceType::string = 'Condition';


INSERT INTO DEVICES (
    "Start", 
    "Stop", 
    PATIENT, 
    ENCOUNTER, 
    "Code", 
    DESCRIPTION, 
    UDI, 
    SOURCE_FILE_NAME
)
SELECT 
    -- 1. Start: Map manufactureDate
    to_timestamp_ntz(resource.value:resource:manufactureDate::string) as "Start",
    
    -- 2. Stop: Map expirationDate
    to_timestamp_ntz(resource.value:resource:expirationDate::string) as "Stop",
    
    -- 3. Patient: Clean UUID
    regexp_replace(resource.value:resource:patient:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 4. Encounter: (Not present in standard FHIR Device resource in this file)
    NULL as ENCOUNTER,
    
    -- 5. Code: Extract SNOMED code
    resource.value:resource:type:coding[0]:code::string as "Code",
    
    -- 6. Description: Extract Display Name
    resource.value:resource:type:coding[0]:display::string as DESCRIPTION,

    -- 7. UDI: Extract distinctIdentifier (e.g. '98106548915166')
    resource.value:resource:distinctIdentifier::string as UDI,

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @fhir_stage (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource
WHERE 
    -- Filter specifically for Device resources
    resource.value:resource:resourceType::string = 'Device';


INSERT INTO ENCOUNTERS (
    "Id",
    "Start",
    "Stop",
    "Patient",
    "Organization",
    "Provider",
    "Payer",
    "EncounterClass",
    "Code",
    "Description",
    "Base_Encounter_Cost",
    "Total_Claim_Cost",
    "Payer_Coverage",
    "ReasonCode",
    "ReasonDescription",
    SOURCE_FILE_NAME
)
SELECT 
    -- 1. Id: Clean UUID
    regexp_replace(resource.value:resource:id::string, 'urn:uuid:', '') as "Id",
    
    -- 2. Start: Period Start
    to_timestamp_ntz(resource.value:resource:period:start::string) as "Start",
    
    -- 3. Stop: Period End
    to_timestamp_ntz(resource.value:resource:period:end::string) as "Stop",
    
    -- 4. Patient: Clean UUID
    regexp_replace(resource.value:resource:subject:reference::string, 'urn:uuid:', '') as "Patient",
    
    -- 5. Organization: Service Provider UUID
    CASE 
        WHEN resource.value:resource:serviceProvider:reference::string LIKE 'urn:uuid:%' 
            THEN regexp_replace(resource.value:resource:serviceProvider:reference::string, 'urn:uuid:', '')
        ELSE split_part(resource.value:resource:serviceProvider:reference::string, '|', 2)
    END as "Organization",

    -- 6. Provider: Extract from participant (usually the primary performer)
    CASE 
        WHEN resource.value:resource:participant[0]:individual:reference::string LIKE 'urn:uuid:%' 
            THEN regexp_replace(resource.value:resource:participant[0]:individual:reference::string, 'urn:uuid:', '')
        ELSE split_part(resource.value:resource:participant[0]:individual:reference::string, '|', 2)
    END as "Provider",

    -- 7. Payer: Not typically directly in Encounter resource (Placeholder)
    NULL as "Payer",

    -- 8. EncounterClass: Extract code (ambulatory, emergency, etc.)
    resource.value:resource:class:code::string as "EncounterClass",

    -- 9. Code: SNOMED Code
    resource.value:resource:type[0]:coding[0]:code::string as "Code",

    -- 10. Description: SNOMED Display
    resource.value:resource:type[0]:coding[0]:display::string as "Description",

    -- 11. Base_Encounter_Cost (Placeholder/Not in standard FHIR Encounter)
    NULL as "Base_Encounter_Cost",

    -- 12. Total_Claim_Cost (Placeholder)
    NULL as "Total_Claim_Cost",

    -- 13. Payer_Coverage (Placeholder)
    NULL as "Payer_Coverage",

    -- 14. ReasonCode: Diagnosis Code (if present)
    resource.value:resource:reasonCode[0]:coding[0]:code::string as "ReasonCode",

    -- 15. ReasonDescription: Diagnosis Description
    resource.value:resource:reasonCode[0]:coding[0]:display::string as "ReasonDescription",

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @fhir_stage (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource
WHERE 
    resource.value:resource:resourceType::string = 'Encounter';



INSERT INTO IMAGING_STUDIES (
    "Id",
    "Date",
    "Patient",
    "Encounter",
    "Series UID",
    "Body Site Code",
    "Body Site Description",
    "Modality Code",
    "Modality Description",
    "Instance UID",
    "SOP Code",
    "SOP Description",
    "Procedure Code",
    SOURCE_FILE_NAME
)
SELECT 
    -- 1. Id: Study ID (Clean UUID)
    regexp_replace(resource.value:resource:id::string, 'urn:uuid:', '') as "Id",
    
    -- 2. Date: Study Started Date
    to_timestamp_ntz(resource.value:resource:started::string) as "Date",
    
    -- 3. Patient: Clean UUID
    regexp_replace(resource.value:resource:subject:reference::string, 'urn:uuid:', '') as "Patient",
    
    -- 4. Encounter: Clean UUID
    regexp_replace(resource.value:resource:encounter:reference::string, 'urn:uuid:', '') as "Encounter",

    -- 5. Series UID
    series.value:uid::string as "Series UID",

    -- 6. Body Site Code (SNOMED)
    series.value:bodySite:code::string as "Body Site Code",

    -- 7. Body Site Description
    series.value:bodySite:display::string as "Body Site Description",

    -- 8. Modality Code (DICOM)
    series.value:modality:code::string as "Modality Code",

    -- 9. Modality Description
    series.value:modality:display::string as "Modality Description",

    -- 10. Instance UID
    instance.value:uid::string as "Instance UID",

    -- 11. SOP Code (DICOM Class)
    instance.value:sopClass:code::string as "SOP Code",

    -- 12. SOP Description (Often inferred or null in standard Synthea, mapping Title here)
    instance.value:title::string as "SOP Description",

    -- 13. Procedure Code (SNOMED from Study level)
    resource.value:resource:procedureCode[0]:coding[0]:code::string as "Procedure Code",

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @FHIR_STAGE (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource,
    LATERAL FLATTEN(input => resource.value:resource:series) as series,
    LATERAL FLATTEN(input => series.value:instance) as instance
WHERE 
    resource.value:resource:resourceType::string = 'ImagingStudy';


--Note on Cost: Standard FHIR Immunization resources do not typically contain financial cost information directly. In the Synthea architecture, costs are usually associated with the corresponding Claim or ExplanationOfBenefit. As such, the COST column in this extraction is set to NULL to maintain the table structure requested, unless you perform a complex join with the Claims data during a later transformation.

INSERT INTO IMMUNIZATIONS (
    "Date",
    PATIENT,
    ENCOUNTER,
    CODE,
    DESCRIPTION,
    COST,
    SOURCE_FILE_NAME
)
SELECT 
    -- 1. Date: Map occurrenceDateTime
    to_timestamp_ntz(resource.value:resource:occurrenceDateTime::string) as "Date",
    
    -- 2. Patient: Clean UUID
    regexp_replace(resource.value:resource:patient:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 3. Encounter: Clean UUID
    regexp_replace(resource.value:resource:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 4. Code: Extract CVX code
    resource.value:resource:vaccineCode:coding[0]:code::string as CODE,
    
    -- 5. Description: Extract Display Name or Text
    COALESCE(
        resource.value:resource:vaccineCode:text::string,
        resource.value:resource:vaccineCode:coding[0]:display::string
    ) as DESCRIPTION,

    -- 6. Cost: Not available in standard FHIR Immunization resource
    NULL as COST,

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @FHIR_STAGE (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource
WHERE 
    -- Filter specifically for Immunization resources
    resource.value:resource:resourceType::string = 'Immunization';


INSERT INTO MEDICATIONS (
    "Start",
    "Stop",
    PATIENT,
    PAYER,
    ENCOUNTER,
    CODE,
    DESCRIPTION,
    "Base_Cost",
    "Payer_Coverage",
    "Dispenses",
    "TotalCost",
    "ReasonCode",
    "ReasonDescription",
    SOURCE_FILE_NAME
)
WITH RAW_DATA AS (
    -- Flatten the Bundle into individual resources
    SELECT 
        metadata$filename as FILENAME,
        value:resource:resourceType::string as RES_TYPE,
        regexp_replace(value:resource:id::string, 'urn:uuid:', '') as RES_ID,
        value:fullUrl::string as FULL_URL,
        value:resource as RES
    FROM @FHIR_STAGE (file_format => 'JSON_FORMAT'),
    LATERAL FLATTEN(input => $1:entry)
),
-- 1. Get Drug Definitions (Code & Description)
MED_DEFS AS (
    SELECT 
        FULL_URL,
        RES:code:coding[0]:code::string as RX_CODE,
        RES:code:coding[0]:display::string as RX_DESC
    FROM RAW_DATA WHERE RES_TYPE = 'Medication'
),
-- 2. Get Reason/Condition Codes (ReasonCode)
CONDITIONS AS (
    SELECT 
        FULL_URL,
        RES:code:coding[0]:code::string as SNOMED_CODE,
        RES:code:coding[0]:display::string as SNOMED_DESC
    FROM RAW_DATA WHERE RES_TYPE = 'Condition'
),
-- 3. Get Costs from Claims (Base_Cost, TotalCost, Payer)
CLAIMS AS (
    SELECT 
        regexp_replace(RES:prescription:reference::string, 'urn:uuid:', '') as REF_MED_ID,
        RES:total:value::number(18,2) as COST,
        -- Attempt to get Payer ID from insurance coverage reference
        regexp_replace(RES:insurance[0]:coverage:reference::string, '#', '') as PAYER_ID
    FROM RAW_DATA 
    WHERE RES_TYPE = 'Claim' AND RES:type:coding[0]:code::string = 'pharmacy'
)
-- 4. Main Query: MedicationRequest joined with above CTEs
SELECT 
    -- Start: Authored On date
    try_to_date(substr(M.RES:authoredOn::string, 1, 10)) as "Start",
    
    -- Stop: Use validity period end, or null if active
    try_to_date(substr(M.RES:dispenseRequest:validityPeriod:end::string, 1, 10)) as "Stop",
    
    -- Patient
    regexp_replace(M.RES:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- Payer (from linked Claim)
    C.PAYER_ID as PAYER,
    
    -- Encounter
    regexp_replace(M.RES:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- Code (from Med Defs)
    D.RX_CODE as CODE,
    
    -- Description (from Med Defs)
    D.RX_DESC as DESCRIPTION,
    
    -- Base_Cost (from Claim)
    C.COST as "Base_Cost",
    
    -- Payer_Coverage (Usually 0.00 or calculated in Synthea CSVs, placeholder here)
    0.00 as "Payer_Coverage",
    
    -- Dispenses
    COALESCE(M.RES:dispenseRequest:numberOfRepeatsAllowed::int, 1) as "Dispenses",
    
    -- TotalCost (Base Cost * Dispenses approximation, or just Claim Cost)
    C.COST as "TotalCost",
    
    -- ReasonCode (from Condition join)
    COND.SNOMED_CODE as "ReasonCode",
    
    -- ReasonDescription
    COND.SNOMED_DESC as "ReasonDescription",
    
    M.FILENAME
FROM RAW_DATA M
-- Join to get Drug Code/Desc
LEFT JOIN MED_DEFS D 
    ON M.RES:medicationReference:reference::string = D.FULL_URL
-- Join to get Cost/Payer
LEFT JOIN CLAIMS C 
    ON M.RES_ID = C.REF_MED_ID
-- Join to get Reason Code (Diagnosis)
LEFT JOIN CONDITIONS COND 
    ON M.RES:reasonReference[0]:reference::string = COND.FULL_URL
WHERE M.RES_TYPE = 'MedicationRequest';


INSERT INTO PATIENTS (
    "Id", "BirthDate", "DeathDate", "SSN", "Drivers", "Passport",
    "Prefix", "First", "Middle", "Last", "Suffix", "Maiden",
    "Marital", "Race", "Ethnicity", "Gender", "BirthPlace",
    "Address", "City", "State", "County", "FIPS", "Zip", "Lat", "Lon",
    "Healthcare_Expenses", "Healthcare_Coverage", "Income",
    SOURCE_FILE_NAME
)
WITH RAW_PATIENTS AS (
    -- Extract raw Patient resources
    SELECT 
        resource.value:resource as RES,
        metadata$filename as FILENAME
    FROM 
        @FHIR_STAGE (file_format => 'JSON_FORMAT') as S,
        LATERAL FLATTEN(input => S.$1:entry) as resource
    WHERE 
        resource.value:resource:resourceType::string = 'Patient'
),
PARSED_IDENTIFIERS AS (
    -- Flatten and pivot identifiers to find SSN, Drivers License, Passport
    SELECT 
        RES:id::string as PAT_ID,
        MAX(CASE WHEN id.value:type:coding[0]:code::string = 'SS' THEN id.value:value::string END) as SSN,
        MAX(CASE WHEN id.value:type:coding[0]:code::string = 'DL' THEN id.value:value::string END) as DL,
        MAX(CASE WHEN id.value:type:coding[0]:code::string = 'PPN' THEN id.value:value::string END) as PP
    FROM RAW_PATIENTS,
    LATERAL FLATTEN(input => RES:identifier) as id
    GROUP BY PAT_ID
),
PARSED_NAMES AS (
    -- Flatten and pivot names to separate Official name components from Maiden name
    SELECT 
        RES:id::string as PAT_ID,
        -- Official Name Parts
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:prefix[0]::string END) as PREFIX,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:given[0]::string END) as FIRST_NAME,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:given[1]::string END) as MIDDLE_NAME,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:family::string END) as LAST_NAME,
        MAX(CASE WHEN nm.value:use::string = 'official' THEN nm.value:suffix[0]::string END) as SUFFIX,
        -- Maiden Name (Family name where use='maiden')
        MAX(CASE WHEN nm.value:use::string = 'maiden' THEN nm.value:family::string END) as MAIDEN_NAME
    FROM RAW_PATIENTS,
    LATERAL FLATTEN(input => RES:name) as nm
    GROUP BY PAT_ID
),
PARSED_EXTENSIONS AS (
    -- Extract Race, Ethnicity, Birthplace from Extensions
    -- Note: We scan the extension array to match specific URLs
    SELECT
        RES:id::string as PAT_ID,
        MAX(CASE WHEN ext.value:url::string LIKE '%us-core-race' THEN ext.value:extension[0]:valueCoding:display::string END) as RACE,
        MAX(CASE WHEN ext.value:url::string LIKE '%us-core-ethnicity' THEN ext.value:extension[0]:valueCoding:display::string END) as ETHNICITY,
        MAX(CASE WHEN ext.value:url::string LIKE '%patient-birthPlace' THEN 
                concat(
                    COALESCE(ext.value:valueAddress:city::string, ''), ', ', 
                    COALESCE(ext.value:valueAddress:state::string, ''), ', ', 
                    COALESCE(ext.value:valueAddress:country::string, '')
                )
            END) as BIRTHPLACE
    FROM RAW_PATIENTS,
    LATERAL FLATTEN(input => RES:extension) as ext
    GROUP BY PAT_ID
)
SELECT 
    -- 1. Id
    regexp_replace(P.RES:id::string, 'urn:uuid:', ''),
    
    -- 2. BirthDate
    P.RES:birthDate::date,
    
    -- 3. DeathDate
    try_to_date(substr(P.RES:deceasedDateTime::string, 1, 10)),
    
    -- 4-6. Identifiers (from CTE)
    I.SSN, I.DL, I.PP,
    
    -- 7-12. Names (from CTE)
    N.PREFIX, N.FIRST_NAME, N.MIDDLE_NAME, N.LAST_NAME, N.SUFFIX, N.MAIDEN_NAME,
    
    -- 13. Marital Status
    P.RES:maritalStatus:coding[0]:code::string, -- e.g. 'M'
    
    -- 14-15. Race & Ethnicity (from CTE)
    E.RACE, E.ETHNICITY,
    
    -- 16. Gender (Map male/female to M/F if strictly required, otherwise distinct string)
    CASE 
        WHEN P.RES:gender::string = 'male' THEN 'M'
        WHEN P.RES:gender::string = 'female' THEN 'F'
        ELSE P.RES:gender::string 
    END,
    
    -- 17. BirthPlace (from CTE)
    E.BIRTHPLACE,
    
    -- 18-20. Address components (taking first address in array)
    P.RES:address[0]:line[0]::string, -- Address
    P.RES:address[0]:city::string,    -- City
    P.RES:address[0]:state::string,   -- State
    
    -- 21. County (Not directly available in standard Synthea FHIR without lookup/extension - Placeholder)
    NULL as County,
    
    -- 22. FIPS (Often not in resource - Placeholder)
    NULL as FIPS,
    
    -- 23. Zip
    P.RES:address[0]:postalCode::string,
    
    -- 24-25. Lat/Lon (From Geolocation extension inside Address)
    -- Synthea nests geolocation in the address extension array
    COALESCE(
        P.RES:address[0]:extension[0]:extension[0]:valueDecimal::number(18,6), -- Standard Synthea path
        P.RES:address[0]:extension[0]:valueDecimal::number(18,6) -- Alternative
    ) as Lat,
    COALESCE(
        P.RES:address[0]:extension[0]:extension[1]:valueDecimal::number(18,6),
        P.RES:address[0]:extension[1]:valueDecimal::number(18,6)
    ) as Lon,

    -- 26-28. Financial Aggregates (Not present in Patient Resource - Placeholders)
    0.00 as "Healthcare_Expenses",
    0.00 as "Healthcare_Coverage",
    0.00 as "Income",

    -- Metadata
    P.FILENAME

FROM RAW_PATIENTS P
LEFT JOIN PARSED_IDENTIFIERS I ON P.RES:id::string = I.PAT_ID
LEFT JOIN PARSED_NAMES N ON P.RES:id::string = N.PAT_ID
LEFT JOIN PARSED_EXTENSIONS E ON P.RES:id::string = E.PAT_ID;




INSERT INTO PAYER_TRANSITIONS (
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
    SELECT 
        -- Extract Patient UUID
        regexp_replace(resource.value:resource:patient:reference::string, 'urn:uuid:', '') as PATIENT_ID,
        
        -- Extract Patient Name (to use as Owner Name)
        resource.value:resource:patient:display::string as PATIENT_NAME,
        
        -- Extract Year from Billable Period Start
        year(try_to_date(substr(resource.value:resource:billablePeriod:start::string, 1, 10))) as CLAIM_YEAR,
        
        -- Extract Payer Name from the insurance block
        -- Synthea typically puts the Payer Name in the 'display' field of the coverage
        COALESCE(
            resource.value:resource:insurance[0]:coverage:display::string,
            'NO_INSURANCE' 
        ) as PAYER_NAME,

        metadata$filename as FILENAME
    FROM 
        @fhir_stage (file_format => 'JSON_FORMAT') as S,
        LATERAL FLATTEN(input => S.$1:entry) as resource
    WHERE 
        resource.value:resource:resourceType::string = 'Claim'
)
SELECT 
    PATIENT_ID,
    NULL as "MEMBER ID", -- Member ID is not easily accessible in Claim resource summary
    MIN(CLAIM_YEAR) as "START_YEAR",
    MAX(CLAIM_YEAR) as "END_YEAR",
    PAYER_NAME as PAYER,
    NULL as "SECONDARY PAYER",
    'Self' as OWNERSHIP, -- Defaulting to Self as standard for inferred data
    PATIENT_NAME as "OWNER NAME",
    FILENAME as SOURCE_FILE_NAME
FROM RAW_CLAIMS
GROUP BY 
    PATIENT_ID, 
    PATIENT_NAME, 
    PAYER_NAME, 
    FILENAME;


INSERT INTO PROCEDURES (
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
SELECT 
    -- 1. Start: Map performedPeriod.start or performedDateTime
    to_timestamp_ntz(COALESCE(
        resource.value:resource:performedPeriod:start::string,
        resource.value:resource:performedDateTime::string
    )) as "Start",
    
    -- 2. Stop: Map performedPeriod.end
    to_timestamp_ntz(resource.value:resource:performedPeriod:end::string) as "Stop",
    
    -- 3. Patient: Clean UUID
    regexp_replace(resource.value:resource:subject:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 4. Encounter: Clean UUID
    regexp_replace(resource.value:resource:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 5. Code: Extract SNOMED code
    resource.value:resource:code:coding[0]:code::string as CODE,
    
    -- 6. Description: Extract Display Name
    resource.value:resource:code:coding[0]:display::string as DESCRIPTION,

    -- 7. Base_Cost: Not available in standard FHIR Procedure resource (Found in Claims)
    NULL as "Base_Cost",

    -- 8. ReasonCode: Direct reason code
    resource.value:resource:reasonCode[0]:coding[0]:code::string as "ReasonCode",
    
    -- 9. ReasonDescription: extraction from reasonCode OR reasonReference display
    COALESCE(
        resource.value:resource:reasonCode[0]:coding[0]:display::string,
        resource.value:resource:reasonReference[0]:display::string
    ) as "ReasonDescription",

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @fhir_Stage (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource
WHERE 
    -- Filter specifically for Procedure resources
    resource.value:resource:resourceType::string = 'Procedure';


select * from procedures;

INSERT INTO SUPPLIES (
    "Date",
    PATIENT,
    ENCOUNTER,
    CODE,
    DESCRIPTION,
    QUANTITY,
    SOURCE_FILE_NAME
)
SELECT 
    -- 1. Date: Map occurrenceDateTime
    try_to_date(substr(resource.value:resource:occurrenceDateTime::string, 1, 10)) as "Date",
    
    -- 2. Patient: Clean UUID
    regexp_replace(resource.value:resource:patient:reference::string, 'urn:uuid:', '') as PATIENT,
    
    -- 3. Encounter: Clean UUID (if present)
    regexp_replace(resource.value:resource:encounter:reference::string, 'urn:uuid:', '') as ENCOUNTER,
    
    -- 4. Code: Extract SNOMED code from suppliedItem
    resource.value:resource:suppliedItem:itemCodeableConcept:coding[0]:code::string as CODE,
    
    -- 5. Description: Extract Display Name
    resource.value:resource:suppliedItem:itemCodeableConcept:coding[0]:display::string as DESCRIPTION,

    -- 6. Quantity
    resource.value:resource:suppliedItem:quantity:value::number as QUANTITY,

    -- Metadata
    metadata$filename as SOURCE_FILE_NAME

FROM 
    @fhir_stage (file_format => 'JSON_FORMAT') as S,
    LATERAL FLATTEN(input => S.$1:entry) as resource
WHERE 
    -- Filter specifically for SupplyDelivery resources
    resource.value:resource:resourceType::string = 'SupplyDelivery';




INSERT INTO ORGANIZATIONS (
    "Id",
    "Name",
    "Address",
    "City",
    "State",
    "Zip",
    "Lat",
    "Lon",
    "Phone",
    "Revenue",
    "Utilization",
    SOURCE_FILE_NAME
)
WITH RAW_BUNDLE AS (
    -- 1. Flatten the bundle to access all individual resources
    SELECT 
        value:resource as RES,
        value:resource:resourceType::string as RES_TYPE,
        metadata$filename as FILENAME
    FROM @fhir_stage (file_format => 'JSON_FORMAT'),
    LATERAL FLATTEN(input => $1:entry)
),
ORG_BASE AS (
    -- 2. Extract Base Organization Details
    SELECT 
        RES:id::string as ORG_ID,
        RES:name::string as ORG_NAME,
        RES:address[0]:line[0]::string as ADDRESS,
        RES:address[0]:city::string as CITY,
        RES:address[0]:state::string as STATE,
        RES:address[0]:postalCode::string as ZIP,
        RES:telecom[0]:value::string as PHONE,
        RES as FULL_RES,
        FILENAME
    FROM RAW_BUNDLE
    WHERE RES_TYPE = 'Organization'
),
ORG_UTILIZATION AS (
    -- 3. Extract Utilization by flattening the extension array separately
    -- We filter for the specific URL for utilization-encounters
    SELECT 
        O.ORG_ID,
        ext.value:valueInteger::number as UTIL_COUNT
    FROM ORG_BASE O,
    LATERAL FLATTEN(input => O.FULL_RES:extension) ext
    WHERE ext.value:url::string = 'http://synthetichealth.github.io/synthea/utilization-encounters-extension'
),
LOC_COORDS AS (
    -- 4. Extract Location Coordinates to join with Organization
    SELECT 
        -- Clean the reference ID to match the Organization ID
        COALESCE(
             split_part(RES:managingOrganization:identifier:value::string, '|', 2), 
             regexp_replace(RES:managingOrganization:reference::string, '^(urn:uuid:|Organization/)', '')
        ) as LINK_ORG_ID,
        RES:position:latitude::number(18,6) as LAT,
        RES:position:longitude::number(18,6) as LON
    FROM RAW_BUNDLE
    WHERE RES_TYPE = 'Location'
)
-- 5. Final Select: Join Base + Utilization + Coordinates
SELECT 
    base.ORG_ID as "Id",
    base.ORG_NAME as "Name",
    base.ADDRESS as "Address",
    base.CITY as "City",
    base.STATE as "State",
    base.ZIP as "Zip",
    loc.LAT as "Lat",
    loc.LON as "Lon",
    base.PHONE as "Phone",
    NULL as "Revenue", -- Not present in standard Synthea FHIR
    util.UTIL_COUNT as "Utilization",
    base.FILENAME as SOURCE_FILE_NAME
FROM ORG_BASE base
LEFT JOIN ORG_UTILIZATION util ON base.ORG_ID = util.ORG_ID
LEFT JOIN LOC_COORDS loc ON base.ORG_ID = loc.LINK_ORG_ID;




INSERT INTO PROVIDERS (
    "Id",
    "Organization",
    "Name",
    "Gender",
    "Speciality",
    "Address",
    "City",
    "State",
    "Zip",
    "Lat",
    "Lon",
    "Encounters",
    "Procedures",
    SOURCE_FILE_NAME
)
WITH RAW_BUNDLE AS (
    -- 1. Flatten the bundle to access all individual resources
    SELECT 
        value:resource as RES,
        value:resource:resourceType::string as RES_TYPE,
        metadata$filename as FILENAME
    FROM @fhir_stage (file_format => 'JSON_FORMAT'),
    LATERAL FLATTEN(input => $1:entry)
),
PRACTITIONER_BASE AS (
    -- 2. Extract Basic Practitioner Details
    SELECT 
        RES:identifier[0]:value::string as PRAC_ID,
        concat(RES:name[0]:given[0]::string, ' ', RES:name[0]:family::string) as FULL_NAME,
        RES:gender::string as GENDER,
        RES:address[0]:line[0]::string as ADDRESS,
        RES:address[0]:city::string as CITY,
        RES:address[0]:state::string as STATE,
        RES:address[0]:postalCode::string as ZIP,
        RES as FULL_RES, -- Pass full object to next CTE for extension flattening
        FILENAME
    FROM RAW_BUNDLE
    WHERE RES_TYPE = 'Practitioner'
),
PRACTITIONER_UTILIZATION AS (
    -- 3. Extract Utilization Stats (Encounters/Procedures)
    -- We flatten the extensions here and aggregate to ensure one row per provider
    SELECT 
        PB.PRAC_ID,
        MAX(CASE WHEN ext.value:url::string LIKE '%utilization-encounters-extension' THEN ext.value:valueInteger::number ELSE 0 END) as ENCOUNTERS,
        MAX(CASE WHEN ext.value:url::string LIKE '%utilization-procedures-extension' THEN ext.value:valueInteger::number ELSE 0 END) as PROCEDURES
    FROM PRACTITIONER_BASE PB,
    LATERAL FLATTEN(input => PB.FULL_RES:extension) as ext
    GROUP BY PB.PRAC_ID
),
ROLE_DATA AS (
    -- 4. Extract Organization Link and Specialty from PractitionerRole
    SELECT 
        RES:practitioner:identifier:value::string as LINK_ID,
        -- Clean Organization ID
        COALESCE(
             split_part(RES:organization:identifier:value::string, '|', 2), 
             RES:organization:identifier:value::string
        ) as ORG_ID,
        RES:specialty[0]:coding[0]:display::string as SPECIALTY
    FROM RAW_BUNDLE
    WHERE RES_TYPE = 'PractitionerRole'
)
-- 5. Main Select: Join Base + Utilization + Role
SELECT 
    base.PRAC_ID as "Id",
    role.ORG_ID as "Organization",
    base.FULL_NAME as "Name",
    
    -- Map Gender
    CASE 
        WHEN base.GENDER = 'male' THEN 'M'
        WHEN base.GENDER = 'female' THEN 'F'
        ELSE base.GENDER 
    END as "Gender",
    
    role.SPECIALTY as "Speciality",
    base.ADDRESS as "Address",
    base.CITY as "City",
    base.STATE as "State",
    base.ZIP as "Zip",
    
    -- Lat/Lon are NULL as they are not present in Practitioner/Role resources
    NULL as "Lat",
    NULL as "Lon",
    
    COALESCE(util.ENCOUNTERS, 0) as "Encounters",
    COALESCE(util.PROCEDURES, 0) as "Procedures",
    
    base.FILENAME as SOURCE_FILE_NAME

FROM PRACTITIONER_BASE base
LEFT JOIN PRACTITIONER_UTILIZATION util ON base.PRAC_ID = util.PRAC_ID
LEFT JOIN ROLE_DATA role ON base.PRAC_ID = role.LINK_ID;

