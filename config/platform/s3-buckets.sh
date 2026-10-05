#!/bin/sh
# One-shot, idempotent and additive: create the buckets ghflow needs; never delete one.
#
# `s3.bucket.create` errors on a bucket that already exists, so this lists first and creates
# only what is missing, then verifies all of them are present (exit 1 otherwise).
set -eu
MASTER=seaweedfs:9333
BUCKETS="raw bronze stream marts"

list() { printf 's3.bucket.list\n' | weed shell -master="$MASTER" 2>/dev/null | awk '{print $1}'; }

existing=$(list)
for bucket in $BUCKETS; do
  if printf '%s\n' "$existing" | grep -qx "$bucket"; then
    echo "s3-init: bucket $bucket exists"
  else
    printf 's3.bucket.create -name %s\n' "$bucket" | weed shell -master="$MASTER" >/dev/null
    echo "s3-init: created bucket $bucket"
  fi
done

present=$(list)
for bucket in $BUCKETS; do
  printf '%s\n' "$present" | grep -qx "$bucket" || { echo "s3-init: bucket $bucket missing after create" >&2; exit 1; }
done
echo "s3-init: buckets ready: $BUCKETS"
