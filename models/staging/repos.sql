-- The repos under analysis, from the `repos` var rather than the data, so a repo whose
-- extract returned nothing still gets (zero) rows in the marts.
select unnest([
    {%- for repo in var('repos') %}
    '{{ repo }}'{{ "," if not loop.last }}
    {%- endfor %}
]) as repo
