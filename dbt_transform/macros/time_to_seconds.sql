-- This macro takes a string column in 'MM:SS' format and converts it to total seconds as an integer.

{% macro time_to_seconds(column) %}
(
  CAST(SPLIT({{ column }}, ':')[OFFSET(0)] AS INT64) * 60
  +
  CAST(SPLIT({{ column }}, ':')[OFFSET(1)] AS INT64)
)
{% endmacro %}