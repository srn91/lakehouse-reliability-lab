{% macro as_decimal_money(column_name) %}
cast({{ column_name }} as decimal(12, 2))
{% endmacro %}
