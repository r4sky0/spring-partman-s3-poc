#!/bin/bash
# LocalStack init hook (mounted at /etc/localstack/init/ready.d/) — creates the
# archive bucket once LocalStack is ready to serve requests.
set -euo pipefail
awslocal s3 mb s3://archive-bucket || true
echo "archive-bucket ready"
