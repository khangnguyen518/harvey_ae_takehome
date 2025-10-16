with int_user_monthly_activity as (
    select * from {{ ref('int_user_monthly_activity') }}
)
,
user_engagement as (
    select
        activity_month,
        user_id,
        firm_id,
        num_active_days,
        num_queries,
        num_workflow_queries,
        num_vault_queries,
        num_assistant_queries,
        avg_queries_per_active_day,
        num_docs,
        avg_docs_per_query,
        avg_feedback_score,
        last_active_date,
        engagement_score,
        pr_engagement_score,
        engagement_level
    from int_user_monthly_activity
)
select * from user_engagement