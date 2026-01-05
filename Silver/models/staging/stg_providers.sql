{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id'
  ) 
}}

with source as (

    select *
    from {{ source('raw', 'PROVIDERS') }}

    {% if is_incremental() %}
    where load_timestamp > (
        select coalesce(max(load_timestamp), '1900-01-01') 
        from {{ this }}
    )
    {% endif %}

),

validated as (

    select
        {{ validate_uuid('"Id"') }} as id,
        {{ validate_uuid('"Organization"') }} as organization_id,

        "Name" as name,
        "Gender" as gender,
        "Speciality" as speciality,
        "Address" as address,
        "City" as city,
        "State" as state,
        "Zip" as zip,
        "Lat" as lat,
        "Lon" as lon,
        "Encounters" as encounters,
        "Procedures" as procedures,

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
  and id is not null;
