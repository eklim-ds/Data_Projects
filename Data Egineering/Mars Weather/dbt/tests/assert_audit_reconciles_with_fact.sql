{#-
    Reconciliation: the audit's running count of distinct sols must end at exactly the number of
    sols in the fact table, and no run can report more new sols than it returned.
-#}
with audit as (
    select
        max(cumulative_distinct_sols) as audited_sols,
        bool_or(new_sols > sols_returned) as impossible_row
    from {{ ref('mart_ingestion_audit') }}
),

fact as (
    select count(*) as fact_sols from {{ ref('fct_sol_weather') }}
)

select audit.audited_sols, fact.fact_sols, audit.impossible_row
from audit
cross join fact
where audit.audited_sols is distinct from fact.fact_sols
   or audit.impossible_row
