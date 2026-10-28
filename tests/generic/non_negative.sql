-- Fails on any negative value. For durations, counts, and rates that can't go below 0.
{% test non_negative(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
