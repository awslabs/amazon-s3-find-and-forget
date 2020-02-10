SHELL := /bin/bash

.PHONY : deploy deploy-containers pre-deploy setup test test-cov test-acceptance test-acceptance-cov test-no-state-machine test-no-state-machine-cov test-unit test-unit-cov
	
pre-deploy:
ifndef TEMP_BUCKET
	$(error TEMP_BUCKET is undefined)
endif
ifndef ADMIN_EMAIL
	$(error ADMIN_EMAIL is undefined)
endif

pre-run:
ifndef ROLE_NAME
	$(error ROLE_NAME is undefined)
endif

build-frontend:
	cd frontend && npm run build

deploy:
	make pre-deploy
	make deploy-artefacts
	make deploy-cfn
	make setup-frontend-local-dev

deploy-cfn:
	aws cloudformation package --template-file templates/template.yaml --s3-bucket $(TEMP_BUCKET) --output-template-file packaged.yaml
	aws cloudformation deploy --template-file ./packaged.yaml --stack-name S3F2 --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --parameter-overrides CreateCloudFrontDistribution=false EnableContainerInsights=true AdminEmail=$(ADMIN_EMAIL) AccessControlAllowOriginOverride=* PreBuiltArtefactsBucketOverride=$(TEMP_BUCKET) SafeMode=false

deploy-artefacts:
	$(eval VERSION := $(shell cfn-flip templates/template.yaml | python -c 'import sys, json; print(json.load(sys.stdin)["Mappings"]["Solution"]["Constants"]["Version"])'))
	make build-frontend
	zip -r build.zip backend/lambda_layers backend/ecs_tasks/delete_files/ frontend/build -x backend/ecs_tasks/delete_files/__pycache* -x *settings.js
	aws s3 cp build.zip s3://$(TEMP_BUCKET)/amazon-s3-find-and-forget/$(VERSION)/build.zip

deploy-containers-override:
	$(eval ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text))
	$(eval REGION := $(shell aws configure get region))
	$(eval ECR_REPOSITORY := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`ECRRepository`].OutputValue' --output text))
	$(shell aws ecr get-login --no-include-email --region $(REGION))
	docker build -t $(ECR_REPOSITORY) -f backend/ecs_tasks/delete_files/Dockerfile .
	docker tag $(ECR_REPOSITORY):latest $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(ECR_REPOSITORY):latest
	docker push $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(ECR_REPOSITORY):latest

deploy-frontend-override:
	$(eval WEBUI_BUCKET := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIBucket`].OutputValue' --output text))
	make build-frontend
	cd frontend/build && aws s3 cp --recursive . s3://$(WEBUI_BUCKET) --acl public-read --exclude *settings.js

run-local-container:
	make pre-run
	./docker_run_with_creds.sh

setup:
	virtualenv venv
	source venv/bin/activate
	pip install -r backend/lambda_layers/aws_sdk/requirements.txt -t backend/lambda_layers/aws_sdk/python
	pip install -r backend/lambda_layers/cr_helper/requirements.txt -t backend/lambda_layers/cr_helper/python
	pip install -r backend/lambda_layers/decorators/requirements.txt -t backend/lambda_layers/decorators/python
	pip install -r requirements.txt
	cd frontend && npm i
	gem install cfn-nag

setup-frontend-local-dev:
	$(eval WEBUI_BUCKET := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIBucket`].OutputValue' --output text))
	aws s3 cp s3://$(WEBUI_BUCKET)/settings.js frontend/public/settings.js

start-frontend-local:
	cd frontend && npm start

start-frontend-remote:
	$(eval WEBUI_URL := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIUrl`].OutputValue' --output text))
	open $(WEBUI_URL)

test-cfn:
	cfn_nag templates/* --blacklist-path ci/cfn_nag_blacklist.yaml

test-frontend:
	cd frontend && npm t

test-unit:
	pytest -m unit --log-cli-level info --cov=backend.lambdas --cov=decorators --cov backend.ecs_tasks

test-acceptance:
	pytest -m acceptance --log-cli-level info

test-no-state-machine:
	pytest -m "not state_machine" --log-cli-level info  --cov=backend.lambdas --cov=decorators --cov backend.ecs_tasks

test:
	make test-cfn
	pytest --log-cli-level info --cov=backend.lambdas --cov=decorators --cov backend.ecs_tasks
	make test-frontend
