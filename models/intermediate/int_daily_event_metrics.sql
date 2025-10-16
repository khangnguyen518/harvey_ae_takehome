with stg_events as (
    select * from {{ ref('stg_events') }}
)
,
stg_users as (
    select * from {{ ref('stg_users') }}
)
,
int_daily_event_metrics as (
    select
        created_at::date as event_date,

        count(distinct e.user_id) as num_users,
        count(distinct e.firm_id) as num_firms,

        count(*) as num_queries,
        
        sum(case when e.event_type = 'workflow' then 1 else 0 end) as num_workflow_queries,
        sum(case when e.event_type = 'vault' then 1 else 0 end) as num_vault_queries,
        sum(case when e.event_type = 'assistant' then 1 else 0 end) as num_assistant_queries,

        count(distinct case when u.job_title = 'Junior Associate' then u.user_id end) as num_jr_associate_queries,
        count(distinct case when u.job_title = 'Associate' then u.user_id end) as num_associate_queries,
        count(distinct case when u.job_title = 'Senior Associate' then u.user_id end) as num_sr_associate_queries,
        count(distinct case when u.job_title = 'Partner' then u.user_id end) as num_partner_queries,

        sum(num_docs) as num_docs,
        sum(num_docs)::float / nullif(count(distinct e.user_id), 0) as avg_docs_per_user,
        sum(num_docs)::float / nullif(count(*), 0) as avg_docs_per_query,
        
        avg(feedback_score)::float as avg_feedback_score

    from stg_events e
    inner join stg_users u
        on e.user_id = u.user_id    
    group by 1
)
select * from int_daily_event_metrics