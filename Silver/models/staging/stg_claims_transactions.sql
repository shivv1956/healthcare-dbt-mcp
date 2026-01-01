{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}

with source as (
    select *
    from {{ source('raw', 'CLAIMS_TRANSACTIONS') }}

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
        "Id"::varchar(255) as id,

        --foreign key to claim
        {{ validate_uuid('"Claim ID"') }}::varchar(36) as claim_id,
        
        "Charge ID"::number as charge_id,

        --foreign key to patient
        {{ validate_uuid('"Patient ID"') }}::varchar(36) as patient_id,

        "Type" as type,
        "Amount"::number as amount,
        "Method" as method,
        "From Date" as from_date,
        "To Date" as to_date,

        --Foreign key to the Organization
        {{ validate_uuid('"Place of Service"') }}::varchar(36) as place_of_service,

        "Procedure Code" as procedure_code,
        "Modifier1" as modifier1,
        "Modifier2" as modifier2,

        "DiagnosisRef1"::number as diagnosis_ref1,
        "DiagnosisRef2"::number as diagnosis_ref2,
        "DiagnosisRef3"::number as diagnosis_ref3,
        "DiagnosisRef4"::number as diagnosis_ref4,

        "Units"::number as units,
        "Department ID"::number as department_id,
        "Notes" as notes,
        "Unit Amount"::number as unit_amount,
        "Transfer Out ID"::number as transfer_out_id,
        "Transfer Type" as transfer_type,
        "Payments"::number as payments,
        "Adjustments"::number as adjustments,
        "Transfers"::number as transfers,
        "Outstanding"::number as outstanding,

        --Foreign key to the Encounter
        {{ validate_uuid('"Appointment ID"') }}::varchar(36) as appointment_id,

        "Line Note" as line_note,

        --Foreign key to the Payer Transitions table member ID.
        {{ validate_uuid('"Patient Insurance ID"') }}::varchar(36) as patient_insurance_id,

        "Fee Schedule ID"::number as fee_schedule_id,

        --Foreign key to the Provider
        {{ validate_uuid('"Provider ID"') }}::varchar(36) as provider_id,

        --Foreign key to the supervising Provider
        {{ validate_uuid('"Supervising Provider ID"') }}::varchar(36) as supervising_provider_id,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        -- row_number() over (
        --     partition by "Id"
        --     order by LOAD_TIMESTAMP desc
        -- ) as rn

    from source
)

select *
from validated
where id is not null
  
