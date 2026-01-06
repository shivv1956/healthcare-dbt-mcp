{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='medication_key'
) }}

with medications_source as (
    select 
        medication_code as code,
        medication_description as description,
        row_number() over (partition by medication_code order by medication_code) as rn
    from {{ ref('stg_medications') }}
),

unique_medications as (
    select * from medications_source where rn = 1
),

medication_metrics as (
    select
        medication_code as code,
        count(distinct patient_id) as patient_count,
        count(*) as prescription_count,
        sum(totalcost) as total_cost,
        avg(totalcost) as avg_cost_per_prescription,
        sum(dispenses) as total_dispenses
    from {{ ref('stg_medications') }}
    where medication_code is not null
    group by medication_code
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['m.code']) }} as medication_key,
        
        -- Medication information
        coalesce(m.code, '0') as code,
        coalesce(m.description, 'not mentioned') as description,
        
        -- Metrics
        coalesce(me.patient_count, 0) as patient_count,
        coalesce(me.prescription_count, 0) as prescription_count,
        coalesce(me.total_cost, 0) as total_cost,
        coalesce(me.avg_cost_per_prescription, 0) as avg_cost_per_prescription,
        coalesce(me.total_dispenses, 0) as total_dispenses
        
    from unique_medications m
    left join medication_metrics me on m.code = me.code
)

select * from final
