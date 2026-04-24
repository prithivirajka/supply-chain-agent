{#
  delivery_status_label
  ─────────────────────────────────────────────────────────────────────────
  Returns a human-readable delivery status label from days_vs_estimate.
  Positive days_vs_estimate = delivered early; negative = late.

  Uses the project variables:
    - late_delivery_threshold_days
    - critical_delay_threshold_days

  Usage:
    {{ delivery_status_label('days_vs_estimate') }} as delivery_status
#}
{% macro delivery_status_label(days_vs_estimate_col) %}
    case
        when {{ days_vs_estimate_col }} < (-1 * {{ var('critical_delay_threshold_days') }})
            then 'critical'
        when {{ days_vs_estimate_col }} < {{ var('late_delivery_threshold_days') }}
            then 'late'
        when {{ days_vs_estimate_col }} between 0 and 2
            then 'on_time'
        else
            'early'
    end
{% endmacro %}
