with firms_raw as (
    select * from {{ source('harvey_raw', 'firms') }}
)
,
stg_firms as (
    select
        id::integer as firm_id,
        firm_size::integer as firm_size,
        arr_in_thousands::numeric * 1000 as arr,  -- converts to dollars
        created::date as created_date -- Formatting date to YYYY-MM-DD
    from firms_raw
)
select * from stg_firms