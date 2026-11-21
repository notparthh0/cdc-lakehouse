{% macro dedupe_changes(relation) %}
-- Debezium is at-least-once (it can re-send events after a Kafka Connect crash), so keep one row per (id, lsn).
select * from {{ relation }}
qualify row_number() over (partition by id, _lsn order by _kafka_partition, _kafka_offset) = 1
{% endmacro %}

{% macro scd2(relation) %}
-- One row per version of each source row. Versions are ordered by WAL position (_lsn), not by
-- commit time, which can tie. A delete closes the last version and adds none.
select * exclude (_op, _tx_id, _kafka_partition, _kafka_offset, _ingested_at, _source_ts, valid_to, _next_lsn),
       _source_ts as valid_from,
       valid_to,
       _next_lsn is null as is_current
from (
    select *, lead(_source_ts) over w as valid_to, lead(_lsn) over w as _next_lsn
    from {{ relation }}
    window w as (partition by id order by _lsn)
)
where _op <> 'd'
{% endmacro %}

{% macro current_state(history) %}
select * exclude (_lsn, valid_from, valid_to, is_current) from {{ history }} where is_current
{% endmacro %}
