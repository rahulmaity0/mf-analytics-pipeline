#!/bin/sh
# Start SeaweedFS (master + volume + filer + S3 gateway in one process) with
# a single access key taken from the environment.
set -eu

cat > /tmp/s3.json <<EOF
{
  "identities": [
    {
      "name": "pipeline",
      "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
EOF

exec weed server -dir=/data -s3 -s3.port=8333 -s3.config=/tmp/s3.json
