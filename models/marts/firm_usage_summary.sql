with int_firm_usage_metrics as (
    select * from {{ ref('int_firm_usage_metrics') }}
)
,
firm_usage_summary as (
    select 
        firm_id,
        firm_size,
        arr,
        firm_created_date,
        num_active_days,
        num_active_users,
        avg_users_per_day,
        pct_users_of_firm_size,
        arr_per_user,
        num_queries,
        num_workflow_queries,
        num_vault_queries,
        num_assistant_queries,
        num_jr_associate_queries,
        num_associate_queries,
        num_sr_associate_queries,
        num_partner_queries,
        avg_queries_per_user,
        avg_queries_per_active_day,
        num_docs,
        avg_docs_per_user,
        avg_docs_per_query,
        avg_feedback_score
    from int_firm_usage_metrics
)
select * from firm_usage_summary