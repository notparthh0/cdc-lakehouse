{% test valid_scd2(model) %}
-- fails for any id with two current versions, two versions from the same WAL position, or a negative interval
select id
from {{ model }}
group by id
having count(*) filter (where is_current) > 1
    or count(*) > count(distinct _lsn)
    or count(*) filter (where valid_to < valid_from) > 0
{% endtest %}
