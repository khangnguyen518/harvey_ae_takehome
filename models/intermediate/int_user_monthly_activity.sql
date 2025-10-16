with 
stg_events as (
    select * from {{ ref('stg_events') }}
)
,
user_monthly_activity as (
    select
        date_trunc('month', created_at::date)::date as activity_month,

        user_id,
        max(firm_id) as firm_id,

        count(distinct created_at::date) as num_active_days,
        max(created_at::date)::date as last_active_date,

        count(*) as num_queries,
        count(*)::float / count(distinct created_at::date) as avg_queries_per_active_day,
        sum(case when event_type = 'workflow' then 1 else 0 end) as num_workflow_queries,
        sum(case when event_type = 'vault' then 1 else 0 end) as num_vault_queries,
        sum(case when event_type = 'assistant' then 1 else 0 end) as num_assistant_queries,

        sum(num_docs) as num_docs,    
        avg(num_docs)::float as avg_docs_per_query,    

        avg(feedback_score)::float as avg_feedback_score
    from stg_events
    group by 1,2
)
,
percentile_metrics as (
    select
        activity_month,
        user_id,
        firm_id,
        num_active_days,
        last_active_date,
        num_queries,
        avg_queries_per_active_day,
        num_workflow_queries,
        num_vault_queries,
        num_assistant_queries,
        num_docs,
        avg_docs_per_query,
        avg_feedback_score,
        -- Percentile ranks for each metric. Normalizes the values between 0 and 1
        percent_rank() over (partition by activity_month order by num_queries) as pr_num_queries,
        percent_rank() over (partition by activity_month order by num_docs) as pr_num_docs,
        percent_rank() over (partition by activity_month order by avg_feedback_score) as pr_avg_feedback_score
    from user_monthly_activity
)
,
engagement_scores as (
    select
        activity_month,
        user_id,
        firm_id,
        num_active_days,
        last_active_date,
        num_queries,
        avg_queries_per_active_day,
        num_workflow_queries,
        num_vault_queries,
        num_assistant_queries,
        num_docs,
        avg_docs_per_query,
        avg_feedback_score,
        pr_num_queries,
        pr_num_docs,
        pr_avg_feedback_score,
        -- weighted engagement score using percentile ranks
        (0.6 * pr_num_queries + 0.3 * pr_num_docs + 0.1 * pr_avg_feedback_score) as engagement_score,
        -- pct rank of the engagement score to determine engagement level
        percent_rank() over (partition by activity_month order by (0.6 * pr_num_queries + 0.3 * pr_num_docs + 0.1 * pr_avg_feedback_score)) as pr_engagement_score
    from percentile_metrics
)
,
int_user_monthly_activity as (
    select 
        activity_month,
        user_id,
        firm_id,
        num_active_days,
        last_active_date,
        num_queries,
        avg_queries_per_active_day,
        num_workflow_queries,
        num_vault_queries,
        num_assistant_queries,
        num_docs,
        avg_docs_per_query,
        avg_feedback_score,
        pr_num_queries,
        pr_num_docs,
        pr_avg_feedback_score,
        engagement_score,
        pr_engagement_score,
        case 
            when pr_engagement_score >= 0.8 then 'high'
            when pr_engagement_score >= 0.3 then 'medium'
            else 'low'
        end as engagement_level
    from engagement_scores
)
select * from int_user_monthly_activity