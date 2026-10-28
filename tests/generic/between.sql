-- Fails on any value outside [min_value, max_value]. For shares and ratios (0 to 1).
-- NULLs pass; pair with not_null where a value is required.
{% test between(model, column_name, min_value, max_value) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}
{% endtest %}
