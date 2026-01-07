{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='organization_id'
) }}

with organizations_source as (
    select * from {{ ref('stg_organizations') }}
),

-- Aggregate medication costs through encounters
organization_medication_costs as (
    select
        e.organization_id,
        sum(m.totalcost) as total_medication_revenue
    from {{ ref('stg_encounters') }} e
    join {{ ref('stg_medications') }} m on e.encounter_id = m.encounter_id
    where e.organization_id is not null
    group by e.organization_id
),

-- Aggregate claims outstanding amounts through providers
organization_claims as (
    select
        p.organization_id,
        sum(c.outstanding_primary) as total_outstanding_primary
    from {{ ref('stg_claims') }} c
    join {{ ref('stg_providers') }} p on c.provider_id = p.id
    where p.organization_id is not null
    group by p.organization_id
),

final as (
    select
        -- Surrogate key (already generated in staging)
        o.organization_id,
        
        -- Organization information
        o.name,
        
        -- Address information
        o.city,
        o.state,
        o.zip,
        
   
        -- Calculated total revenue
        coalesce(mc.total_medication_revenue, 0) + 
        coalesce(oc.total_outstanding_primary, 0)
        as revenue,
        
    
        
    from organizations_source o
    left join organization_medication_costs mc on o.organization_id = mc.organization_id
    left join organization_claims oc on o.organization_id = oc.organization_id
   
)

select * from final
