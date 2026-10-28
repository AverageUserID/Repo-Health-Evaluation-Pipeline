-- run_context is cross joined into other models, so more than one row would multiply them.
select count(*) as n_rows
from {{ ref('run_context') }}
having count(*) <> 1
