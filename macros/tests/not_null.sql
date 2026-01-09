{% test not_null(model, column_name) %}
    {% set cols = adapter.get_columns_in_relation(model) %}
    {% set col_names = cols | map(attribute='name') | map('lower') | list %}
    {% set has_is_deleted = 'is_deleted' in col_names %}

    with base as (
        select *
        from {{ model }}
    ),

    filtered as (
        select *
        from base
        {% if has_is_deleted %}
        where coalesce(is_deleted, false) = false
        {% endif %}
    )

    select *
    from filtered
    where {{ column_name }} is null

{% endtest %}