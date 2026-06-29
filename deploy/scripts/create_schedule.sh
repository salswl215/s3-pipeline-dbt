#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
TASK_DEF_ARN="$(aws ecs describe-task-definition --region "$AWS_REGION" \
  --task-definition "$TASK_FAMILY" --query 'taskDefinition.taskDefinitionArn' --output text)"
aws scheduler create-schedule --region "$AWS_REGION" \
  --name s3-pipeline-dbt-daily \
  --schedule-expression "$SCHEDULE_EXPRESSION" \
  --flexible-time-window '{"Mode":"OFF"}' \
  --target "{
    \"Arn\": \"arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:cluster/${ECS_CLUSTER}\",
    \"RoleArn\": \"${SCHEDULER_ROLE_ARN}\",
    \"EcsParameters\": {
      \"TaskDefinitionArn\": \"${TASK_DEF_ARN}\",
      \"LaunchType\": \"FARGATE\",
      \"NetworkConfiguration\": {\"awsvpcConfiguration\": {\"Subnets\": [${SUBNETS//,/\",\"}], \"SecurityGroups\": [${SECURITY_GROUPS//,/\",\"}], \"AssignPublicIp\": \"ENABLED\"}}
    }
  }"
