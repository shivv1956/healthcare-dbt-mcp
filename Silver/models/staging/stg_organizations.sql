{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'organization_id'
  ) 
}}

with source as (

    select *
    from {{ source('raw', 'ORGANIZATIONS') }}

    {% if is_incremental() %}
    where load_timestamp >
      (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
    {% endif %}

),

validated as (

    select
        {{ validate_uuid('Id') }} as organization_id,

        Name    as name,
        Address as address,
        City    as city,
        State   as state,
        Zip     as zip,

        Lat::number(18,6) as latitude,
        Lon::number(18,6) as longitude,

        Phone as phone,
        Revenue::number(18,2) as revenue,
        Utilization::number(38,0) as utilization,

        SOURCE_FILE_NAME,
        LOAD_TIMESTAMP,

        row_number() over (
            partition by {{ validate_uuid('Id') }}
            order by LOAD_TIMESTAMP desc
        ) as rn

    from source
)

select *
from validated
where rn = 1
  and organization_id is not null
  and name is not null
  and address is not null
  and city is not null
  and revenue is not null
  and utilization is not null;
