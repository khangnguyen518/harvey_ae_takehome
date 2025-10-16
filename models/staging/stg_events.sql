with events_raw as (
    select * from {{ source('harvey_raw', 'events') }}
)
,
stg_events as (
    select
        user_id::varchar as user_id,
        firm_id::integer as firm_id,
        lower(event_type)::varchar as event_type,
        num_docs::integer as num_docs,
        feedback_score::numeric as feedback_score,
        created::timestamp as created_at -- Formatting date to YYYY-MM-DD
    from events_raw
)
select * from stg_events