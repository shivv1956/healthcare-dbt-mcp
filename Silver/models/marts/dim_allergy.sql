{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='allergy_key'
) }}

with allergies_source as (
    select 
        code,
        system,
        description,
        type,
        row_number() over (partition by code, system order by code) as rn
    from {{ ref('stg_alergies') }}
),

unique_allergies as (
    select * from allergies_source where rn = 1
),

patient_counts as (
    select
        code,
        system,
        count(distinct patient_id) as patient_count
    from {{ ref('stg_alergies') }}
    where code is not null
    group by code, system
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['a.code', 'a.system']) }} as allergy_key,
        
        -- Allergy information
        a.code,
        a.description,
        a.system,
        a.type,
        
        -- Metrics
        coalesce(p.patient_count, 0) as patients_affected
        
    from unique_allergies a
    left join patient_counts p on a.code = p.code and a.system = p.system
)

select * from final
