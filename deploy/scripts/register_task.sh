#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
# upload runtime env file to S3
aws s3 cp "$DEPLOY_DIR/task.env" "$ENV_FILE_S3_URI"
# render + register task definition
envsubst < "$DEPLOY_DIR/task-definition.json" > /tmp/s3-pipeline-dbt-taskdef.json
aws ecs register-task-definition --region "$AWS_REGION" \
  --cli-input-json file:///tmp/s3-pipeline-dbt-taskdef.json
