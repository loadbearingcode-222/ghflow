{#
  A payload field exists only in its era (ghflow.fields: from_ inclusive, until exclusive).
  Fails with the rows that carry a value outside it, so a field GitHub dropped is NULL because
  of its era, never by accident. Bounds are UTC timestamp strings, or none for an open side.
#}
{% test in_era(model, column_name, from_, until) %}
{%- set after_start = "true" if from_ is none else "created_at >= cast('" ~ from_ ~ "' as timestamptz)" -%}
{%- set before_end = "true" if until is none else "created_at < cast('" ~ until ~ "' as timestamptz)" -%}
select id, created_at, {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and not ({{ after_start }} and {{ before_end }})
{% endtest %}
