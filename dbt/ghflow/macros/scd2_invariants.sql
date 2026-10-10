{#
  The SCD2 invariants (M3-S1): one current row per id; validity never overlaps or gaps (each
  version ends exactly where the next begins); valid_from strictly increasing per id; a version
  never ends before it starts. Every violating row is returned, named by its rule.
#}
{% test scd2_invariants(model, id_column) %}
with ordered as (
    select
        {{ id_column }} as key_id, valid_from, valid_to, is_current,
        lead(valid_from) over (partition by {{ id_column }} order by valid_from) as next_from,
        count(*) filter (where is_current) over (partition by {{ id_column }}) as currents
    from {{ model }}
)
select 'one_current_row_per_id' as rule, key_id, valid_from from ordered where currents <> 1
union all
select 'valid_from_strictly_increasing', key_id, valid_from from ordered where next_from <= valid_from
union all
select 'no_overlap_no_gap', key_id, valid_from from ordered where valid_to is distinct from next_from
union all
select 'ends_after_it_starts', key_id, valid_from from ordered where valid_to is not null and valid_to <= valid_from
union all
select 'current_means_open', key_id, valid_from from ordered where is_current <> (valid_to is null)
{% endtest %}
