#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
aws ecr describe-repositories --repository-names "$ECR_REPO" --region "$AWS_REGION" >/dev/null 2>&1 \
  || aws ecr create-repository --repository-name "$ECR_REPO" --region "$AWS_REGION"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
docker build -f "$DEPLOY_DIR/Dockerfile" -t "$ECR_IMAGE_URI" "$ROOT_DIR"
docker push "$ECR_IMAGE_URI"
