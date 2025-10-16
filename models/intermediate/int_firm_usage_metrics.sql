with stg_firms as (
    select * from {{ ref('stg_firms') }}
)
,
stg_events as (
    select * from {{ ref('stg_events') }}
)
,
stg_users as (
    select * from {{ ref('stg_users') }}
)
,
int_firm_usage_metrics as (
    select
        f.firm_id,
        max(f.firm_size) as firm_size,
        max(f.arr) as arr,
        max(f.created_date) as firm_created_date,

        count(distinct e.created_at::date) as num_active_days,
        count(distinct e.user_id) as num_active_users,
        count(distinct e.user_id)::float / nullif(count(distinct e.created_at::date), 0) as avg_users_per_day,
        count(distinct e.user_id)::float / max(f.firm_size) as pct_users_of_firm_size,
        max(f.arr)::numeric / nullif(count(distinct e.user_id), 0) as arr_per_user,

        count(*) as num_queries,
        sum(case when e.event_type = 'workflow' then 1 else 0 end) as num_workflow_queries,
        sum(case when e.event_type = 'vault' then 1 else 0 end) as num_vault_queries,
        sum(case when e.event_type = 'assistant' then 1 else 0 end) as num_assistant_queries,
        
        sum(case when u.job_title = 'Junior Associate' then 1 else 0 end) as num_jr_associate_queries,
        sum(case when u.job_title = 'Associate' then 1 else 0 end) as num_associate_queries,
        sum(case when u.job_title = 'Senior Associate' then 1 else 0 end) as num_sr_associate_queries,
        sum(case when u.job_title = 'Partner' then 1 else 0 end) as num_partner_queries,
        count(*)::float / nullif(count(distinct e.user_id), 0) as avg_queries_per_user,
        count(*)::float / nullif(count(distinct e.created_at::date), 0) as avg_queries_per_active_day,

        sum(e.num_docs) as num_docs,
        sum(e.num_docs)::float / nullif(count(distinct e.user_id), 0) as avg_docs_per_user,
        avg(e.num_docs)::float as avg_docs_per_query,

        avg(e.feedback_score)::float as avg_feedback_score
    from stg_firms f
    inner join stg_events e
        on f.firm_id = e.firm_id
    inner join stg_users u
        on e.user_id = u.user_id
    group by 1
)
select * from int_firm_usage_metrics