with source as (
    select *
    from read_parquet('C:/Users/deniz/open-fabric/data_generator/output/policies.parquet')
),

renamed as (
    select
        policy_id,
        policy_number,
        product_code,
        status,
        cast(start_date as date)    as start_date,
        cast(end_date as date)      as end_date,
        cast(created_at as timestamp) as created_at
    from source
)

select * from renamed