{#
  generate_date_spine_months
  ─────────────────────────────────────────────────────────────────────────
  Convenience wrapper around dbt_utils.date_spine that generates a
  continuous monthly series between the project's start_date and end_date
  variables. Use in marts to avoid missing months in time-series outputs.

  Usage:
    with months as (
        {{ generate_date_spine_months() }}
    )
    select * from months
#}
{% macro generate_date_spine_months() %}
    {{ dbt_utils.date_spine(
        datepart = "month",
        start_date = "cast('" ~ var('start_date') ~ "' as date)",
        end_date   = "cast('" ~ var('end_date')   ~ "' as date)"
    ) }}
{% endmacro %}
