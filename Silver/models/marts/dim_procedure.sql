{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='procedure_key'
) }}

with procedures_source as (
    select 
        code,
        system,
        description,
        row_number() over (partition by code, system order by code) as rn
    from {{ ref('stg_procedures') }}
),

unique_procedures as (
    select * from procedures_source where rn = 1
),

procedure_metrics as (
    select
        code,
        system,
        count(distinct patient_id) as patient_count,
        count(*) as procedure_count,
        avg(base_cost) as avg_cost,
        sum(base_cost) as total_cost,
        count(distinct encounter_id) as encounter_count
    from {{ ref('stg_procedures') }}
    where code is not null
    group by code, system
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.code', 'p.system']) }} as procedure_key,
        
        -- Procedure information
        p.code,
        p.description,
        p.system,
        
        -- Metrics
        coalesce(m.patient_count, 0) as patient_count,
        coalesce(m.procedure_count, 0) as procedure_count,
        coalesce(m.avg_cost, 0) as avg_cost,
        coalesce(m.total_cost, 0) as total_cost,
        coalesce(m.encounter_count, 0) as encounter_count
        
    from unique_procedures p
    left join procedure_metrics m on p.code = m.code and p.system = m.system
)

select * from final
