{% macro safe_divide(numerator, denominator) -%}
    ({{ numerator }}) / nullif(({{ denominator }}), 0)
{%- endmacro %}

{% macro budget_health_status(spend_expression, budget_expression) -%}
    case
        when {{ budget_expression }} is null or {{ budget_expression }} <= 0 then 'Missing Budget'
        when {{ spend_expression }} > {{ budget_expression }} then 'Over Budget'
        when {{ safe_divide(spend_expression, budget_expression) }} >= {{ var('budget_warning_threshold') }} then 'At Risk'
        else 'On Track'
    end
{%- endmacro %}
