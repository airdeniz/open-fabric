with source as (
    select *
    from read_parquet('C:/Users/deniz/open-fabric/data_generator/output/insureds.parquet')
),

renamed as (
    select
        insured_id,
        policy_id,
        full_name,
        tc_no,
        cast(birth_date as date)      as birth_date,
        phone,
        email,
        cast(created_at as timestamp) as created_at
    from source
)

select * from renamed