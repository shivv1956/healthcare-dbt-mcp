{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'claim_id'
    ) 
}}

with source as (

    select *
    from {{ source('raw', 'CLAIMS') }}

    {% if is_incremental() %}
    where LOAD_TIMESTAMP > (
        select coalesce(max(LOAD_TIMESTAMP), '1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

validated as (

    select
        -- Primary key
        {{ validate_uuid('"Id"') }}::varchar(36) as claim_id,

        --Foreign key to the Patient_id , provider_id, primary_insurance_id and secondary_patient_insurance_id
        {{ validate_uuid('"Patient ID"') }}::varchar(36)  as patient_id,
        {{ validate_uuid('"Provider ID"') }}::varchar(36) as provider_id,
        {{ validate_uuid('"Primary Patient Insurance ID"') }}::varchar(36)   as Primary_patient_insurance_id,
        {{ validate_uuid('"Secondary Patient Insurance ID"') }}::varchar(36) as Secondary_patient_insurance_id,

        "Department ID"::number(38,0)         as department_id,
        "Patient Department ID"::number(38,0) as patient_department_id,

        "Diagnosis1",
        "Diagnosis2",
        "Diagnosis3",
        "Diagnosis4",
        "Diagnosis5",
        "Diagnosis6",
        "Diagnosis7",
        "Diagnosis8",

        {{ validate_uuid('"Referring Provider ID"') }}::varchar(36) as referring_provider_id,
        {{ validate_uuid('"Appointment ID"') }}::varchar(36)        as appointment_id,
        {{ validate_uuid('"Supervising Provider ID"') }}::varchar(36) as supervising_provider_id,

        "Current Illness Date"::timestamp_ntz as current_illness_date,
        "Service Date"::timestamp_ntz         as service_date,

        case when upper("Status1") in ('BILLED','CLOSED') then upper("Status1") end as status_primary,
        case when upper("Status2") in ('BILLED','CLOSED') then upper("Status2") end as status_secondary,
        case when upper("StatusP") in ('BILLED','CLOSED') then upper("StatusP") end as status_patient,

        -- "Status1" as status_primary,
        -- "Status2" as status_secondary,
        -- "StatusP" as status_patient,

        "Outstanding1"::number(18,2) as outstanding_primary,
        "Outstanding2"::number(18,2) as outstanding_secondary,
        "OutstandingP"::number(18,2) as outstanding_patient,

        "LastBilledDate1"::timestamp_ntz as last_billed_primary,
        "LastBilledDate2"::timestamp_ntz as last_billed_secondary,
        "LastBilledDateP"::timestamp_ntz as last_billed_patient,

        "HealthcareClaimTypeID1"::number(38,0) as claim_type_primary,
        "HealthcareClaimTypeID2"::number(38,0) as claim_type_secondary,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        -- Deduplication
        row_number() over (
            partition by "{{ validate_uuid('"Id"') }}"
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and claim_id is not null
  and patient_id is not null
