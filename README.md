## Assumptions:

    Data & Relationships
        -	users.id and firms.id are stable unique keys; events.user_id and events.firm_id reference them.
        -	Joins from events to users and firms have no missing references.
        -   arr_in_thousands and firm_size are 1:1 with firms.
        -   title is 1:1 in users.
        -   created dates are 1:1 in both firms and users.

    Time & Scope
        -	All timestamps/dates are treated as UTC.

    Data Integrity
        -	No nulls or orphan rows in source tables.

    Raw Tables

        -   Events
            -   Each row represents one user query (no resubmissions).
            -   feedback_score is required and ranges 1–5.
            -   In Google Sheets, created displayed as MM/DD/YYYY but retained time. 
                -   I duplicated the column with TEXT(cell, "YYYY-MM-DD HH:MM:SS"), pasted values over the original, and removed the helper column to ensure accurate export.

        -   Firms
            -   created is the firm’s account creation date (not necessarily first active date).
            -   firm_size is provisioned seats (not necessarily active users).
            -   arr_in_thousands is annual contract value (ACV) in thousands of USD.

        -   Users
            -   created is the user's account creation date (not necessarily the first active date).
            -   title uses a finite set: Associate, Junior Associate, Senior Associate, Partner.

    Global Metrics:
        -   num_queries: Engagement volume. Shows how much users use the product.
        -   num_docs: Engagement depth. Signals deeper use than a general query.
        -   avg_feedback_score: Quality metric. Tells you how satisfied they are, but most subjective metric.


## Approach:

    Quick Setup
        -	Prerequisites: python 3.10+, postgres 14 running, dbt-postgres 1.10.x
        -   Import raw tables into 
        -	Create venv: python3 -m venv harvey_dbt_env && source harvey_dbt_env/bin/activate
        -	Install dbt: pip install dbt-postgres
        -	Profile (~/.dbt/profiles.yml)
            ```
            harvey_ae_takehome:
            outputs:
                dev:
                dbname: harvey_db
                host: localhost
                pass: dbt_password
                port: 5432
                schema: analytics
                threads: 4
                type: postgres
                user: dbt_user
            target: dev
            ```
        -   Import data from sheets to harvey_raw schema
        -   Create staging, intermediate, and marts folders, as well as dbt_project.yml
        -   Create harvey_raw_sources.yml in staging folder

    Modeling
        -	Staging models (stg_users, stg_firms, stg_events) rename column names and cast types.
            -   materialized as views
            -   converted arr_in_thousands to arr in stg_firms
        -   Intermediate:
            -   materialized as views
            -   int_user_monthly_activity: per-user per-month metrics (counts, event-type splits, avg feedback, percentile-based engagement score).
        	-   int_firm_usage_metrics: per-firm metrics (active_days, users, events, docs, per-day/per-user rates).
        	-   int_daily_event_metrics: daily events (distinct users/firms, docs, avg feedback).
        -	Marts:
            -   materialized as tables
            -   user_engagement: dependent on int_user_monthly_activity.
            -   firm_usage_summary: dependent on int_firm_usage_metrics.
            -   daily_event_summary: dependent on int_daily_event_metrics.

    Metrics (created in intermediate models)
        
        int_user_monthly_activity
        -   engagement_score:
            -   engagement_score: weighted mix of monthly percentiles
            -   Percentiles computed per month using percent_rank().
                -   Scores and levels reset monthly to reflect relative engagement changes over time
            	-   Inputs: num_queries (60%), num_docs (30%), avg_feedback (10%).
        -	engagement_level:
            -   Based on the percentile of the monthly engagement score for a user:
                -	high: top 20%  |  medium: 30-80th percentile  |  low: bottom 30%
                -   engagement scores are weighted against
        
        int_firm_usage_metrics
        -	"Active" user: 1+ event in a month.
        -   arr_per_user based on number of active users, not firm_size.
