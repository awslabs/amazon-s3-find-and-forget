#!/usr/bin/env bash

set -e

# Obtain stack and account details
REGION=$(aws configure get region)
QUEUE_URL=$(aws cloudformation describe-stacks \
  --stack-name S3F2 \
  --query 'Stacks[0].Outputs[?OutputKey==`DeletionQueueUrl`].OutputValue' \
  --output text)
DLQ_URL=$(aws cloudformation describe-stacks \
  --stack-name S3F2 \
  --query 'Stacks[0].Outputs[?OutputKey==`DLQUrl`].OutputValue' \
  --output text)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
# Assume IAM Role to be passed to container
SESSION_DATA=$(aws sts assume-role \
  --role-session-name s3f2-local \
  --role-arn arn:aws:iam::"${ACCOUNT_ID}":role/"${ROLE_NAME}" \
  --query Credentials \
  --output json)
AWS_ACCESS_KEY_ID=$(echo "${SESSION_DATA}" | jq -r ".AccessKeyId")
AWS_SECRET_ACCESS_KEY=$(echo "${SESSION_DATA}" | jq -r ".SecretAccessKey")
AWS_SESSION_TOKEN=$(echo "${SESSION_DATA}" | jq -r ".SessionToken")
# Run the container with local changes mounted
docker run \
	-v "$(pwd)"/backend/ecs_tasks/delete_files/delete_files.py:/app/delete_files.py:ro \
	-e DELETE_OBJECTS_QUEUE="${QUEUE_URL}" \
	-e DLQ="${DLQ_URL}" \
	-e AWS_DEFAULT_REGION="${REGION}" \
	-e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
	-e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
	-e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
	s3f2
