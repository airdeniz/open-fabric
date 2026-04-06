with policies as (
    select * from {{ ref('stg_policies') }}
),

versions as (
    select * from {{ ref('stg_policy_versions') }}
),

insureds as (
    select * from {{ ref('stg_insureds') }}
),

final as (
    select
        p.policy_id,
        p.policy_number,
        p.product_code,
        p.status,
        p.start_date,
        p.end_date,
        v.version_id,
        v.version_number,
        v.gross_premium,
        v.net_premium,
        v.endorsement_type,
        i.insured_id,
        i.full_name,
        i.tc_no,
        i.birth_date,
        i.phone,
        i.email,
        datediff('day', p.start_date, p.end_date)   as policy_duration_days,
        v.gross_premium - v.net_premium              as commission_amount,
        p.created_at
    from policies p
    left join versions v on p.policy_id = v.policy_id
    left join insureds i on p.policy_id = i.policy_id
)

select * from final