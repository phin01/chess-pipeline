-- This macro takes a UTC timestamp and returns the date in local time (considering Sao Paulo/BRA)
{% macro local_date(column) %}
  DATE({{ column }}, "America/Sao_Paulo")
{% endmacro %}

-- This macro takes a UTC timestamp and returns the time in local time (considering Sao Paulo/BRA)
{% macro local_time(column) %}
  TIME({{ column }}, "America/Sao_Paulo")
{% endmacro %}

-- This macro takes a UTC timestamp and returns the day of week in local time (considering Sao Paulo/BRA)
-- %w returns the day of week as a number (0-6, where 0 is Sunday and 6 is Saturday)
{% macro local_day_of_week(column) %}
  FORMAT_TIMESTAMP('%w', {{ column }}, "America/Sao_Paulo")
{% endmacro %}