-- Fails if any combination of `columns` appears more than once, e.g. (repo, number):
-- PR numbers repeat across repos, so a single-column unique test isn't enough.
{% test unique_together(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
