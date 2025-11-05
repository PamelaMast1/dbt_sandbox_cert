{% docs INSERT_TIMESTAMP %}
Timestamp indicating when the record was ingested or processed into the raw layer.

This field is used by dbt to:
- Track source freshness (e.g., detect late-arriving data)
- Audit data ingestion timelines
- Support incremental model strategies that rely on load time

Typical format: `TIMESTAMP` in UTC.
{% enddocs %}

{% docs workout_id %}
Source workout identifier.
{% enddocs %}