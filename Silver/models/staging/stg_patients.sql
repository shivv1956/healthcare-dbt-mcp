{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'patient_id'
  ) 
}}

with source as (

    select *
    from {{ source('raw', 'PATIENTS') }}

    {% if is_incremental() %}
    where load_timestamp >
      (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
    {% endif %}

),

validated as (

    select
        {{ validate_uuid('"Id"') }} as patient_id,

        "BirthDate"::date  as birth_date,
        "DeathDate"::date  as death_date,

        "SSN"       as ssn,
        "Drivers"   as drivers_license,
        "Passport"  as passport_number,

        "Prefix" as prefix,
        "First"  as first_name,
        "Middle" as middle_name,
        "Last"   as last_name,
        "Suffix" as suffix,
        "Maiden" as maiden_name,

        "Marital"   as marital_status,
        "Race"      as race,
        "Ethnicity" as ethnicity,
        "Gender"    as gender,

        "BirthPlace" as birth_place,

        "Address" as address,
        "City"    as city,
        "State"   as state,
        "County"  as county,
        "FIPS"    as fips_county_code,
        "Zip"     as zip_code,

        "Lat"::number(18,6) as latitude,
        "Lon"::number(18,6) as longitude,

        "Healthcare_Expenses"::number(18,2) as healthcare_expenses,
        "Healthcare_Coverage"::number(18,2) as healthcare_coverage,
        "Income"::number(18,2) as income,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by {{ validate_uuid('"Id"') }}
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and patient_id is not null
  
