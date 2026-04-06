with source as (
    select *
    from read_parquet('C:/Users/deniz/open-fabric/data_generator/output/policy_versions.parquet')
),

renamed as (
    select
        version_id,
        policy_id,
        version_number,
        gross_premium,
        net_premium,
        endorsement_type,
        cast(created_at as timestamp) as created_at
    from source
)

select * from renamed