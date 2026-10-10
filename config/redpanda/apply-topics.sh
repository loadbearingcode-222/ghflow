#!/bin/bash
# Apply config/redpanda/topics.conf to the broker: create missing topics, grow partitions, set
# configs. Idempotent; refuses to shrink a topic (that would need a new topic, by hand).
set -euo pipefail
RPK=(rpk -X brokers=redpanda:9092)
while read -r name partitions configs; do
  [[ -z "${name}" || "${name}" == \#* ]] && continue
  current=$("${RPK[@]}" topic describe "${name}" --print-partitions 2>/dev/null | tail -n +2 | grep -c . || true)
  if [[ "${current}" == "0" ]]; then
    args=()
    for kv in ${configs}; do args+=(-c "${kv}"); done
    "${RPK[@]}" topic create "${name}" -p "${partitions}" -r 1 "${args[@]}"
    echo "apply-topics: created ${name} with ${partitions} partitions"
    continue
  fi
  if (( current > partitions )); then
    echo "apply-topics: REFUSED ${name} has ${current} partitions, spec says ${partitions} (never shrink)" >&2
    exit 1
  fi
  if (( current < partitions )); then
    "${RPK[@]}" topic add-partitions "${name}" --num $(( partitions - current ))
  fi
  for kv in ${configs}; do "${RPK[@]}" topic alter-config "${name}" --set "${kv}" >/dev/null; done
  echo "apply-topics: ${name} matches the spec (${partitions} partitions, ${configs})"
done < /opt/ghflow/topics.conf
