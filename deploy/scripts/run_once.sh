#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
aws ecs run-task --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --launch-type FARGATE \
  --task-definition "$TASK_FAMILY" \
  --network-configuration "awsvpcConfiguration={subnets=[${SUBNETS}],securityGroups=[${SECURITY_GROUPS}],assignPublicIp=ENABLED}"
