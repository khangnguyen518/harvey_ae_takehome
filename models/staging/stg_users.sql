with users_raw as (
    select * from {{ source('harvey_raw', 'users') }}
)
,
stg_users as (
    select
        id::varchar as user_id,
        title::varchar as job_title, 
        to_date(created, 'MM/DD/YYYY') as created_date -- Formatting date to YYYY-MM-DD
    from users_raw
)
select * from stg_users