with int_daily_event_metrics as (
    select * from {{ ref('int_daily_event_metrics') }}
)
,
daily_event_summary as (
    select
        event_date,
        num_users,
        num_firms,
        num_queries,
        num_workflow_queries
        num_vault_queries,
        num_assistant_queries,
        num_jr_associate_queries,
        num_associate_queries,
        num_sr_associate_queries,
        num_partner_queries,
        num_docs,
        avg_docs_per_user,
        avg_docs_per_query,
        avg_feedback_score
    from int_daily_event_metrics
)
select * from daily_event_summary