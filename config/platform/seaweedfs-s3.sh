#!/bin/sh
# SeaweedFS entrypoint: write the S3 identities file from the environment, then start master,
# volume, filer and S3 gateway in one process. Run as `sh <this file>`, so no exec bit is needed.
#
# There is no anonymous identity: every S3 request must be signed (an unsigned GET answers 403).
set -eu
umask 077

cat > /tmp/s3.json <<EOF
{"identities": [{"name": "ghflow",
  "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
  "actions": ["Admin", "Read", "List", "Tagging", "Write"]}]}
EOF

# -volume.max=0 sizes the volume count from free disk; 1 GB volumes keep compaction cheap.
exec weed server -dir=/data -s3 -s3.config=/tmp/s3.json \
  -master.volumeSizeLimitMB=1024 -volume.max=0
