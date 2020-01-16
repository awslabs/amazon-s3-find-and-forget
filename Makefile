SHELL := /bin/bash
VERSION := $(shell cat templates/template.yaml | shyaml get-value Mappings.Solution.Constants.Version)

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

deploy:
	make pre-deploy
	make deploy-containers
	make deploy-frontend
	make deploy-cfn
	make setup-frontend-local-dev

deploy-cfn:
	aws cloudformation package --template-file templates/template.yaml --s3-bucket $(TEMP_BUCKET) --output-template-file packaged.yaml
	aws cloudformation deploy --template-file ./packaged.yaml --stack-name S3F2 --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --parameter-overrides CreateCloudFrontDistribution=false EnableContainerInsights=true AdminEmail=$(ADMIN_EMAIL) AccessControlAllowOriginOverride=* PreBuiltArtefactsBucket=$(TEMP_BUCKET)

deploy-containers:
	zip -r backend.zip backend/lambda_layers backend/ecs_tasks/delete_files/ -x backend/ecs_tasks/delete_files/__pycache*
	aws s3 cp backend.zip s3://$(TEMP_BUCKET)/amazon-s3-find-and-forget/$(VERSION)/backend.zip

deploy-containers-override:
	$(eval ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text))
	$(eval REGION := $(shell aws configure get region))
	$(eval ECR_REPOSITORY := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`ECRRepository`].OutputValue' --output text))
	$(shell aws ecr get-login --no-include-email --region $(REGION))
	docker build -t $(ECR_REPOSITORY) -f backend/ecs_tasks/delete_files/Dockerfile .
	docker tag $(ECR_REPOSITORY):latest $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(ECR_REPOSITORY):latest
	docker push $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(ECR_REPOSITORY):latest

deploy-frontend:
	cd frontend && npm run build
	cd frontend/build && zip -r ../../frontend.zip . -x *settings.js
	aws s3 cp frontend.zip s3://$(TEMP_BUCKET)/amazon-s3-find-and-forget/$(VERSION)/frontend.zip

deploy-frontend-override:
	$(eval WEBUI_BUCKET := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIBucket`].OutputValue' --output text))
	cd frontend && npm run build
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

setup-frontend-local-dev:
	$(eval WEBUI_BUCKET := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIBucket`].OutputValue' --output text))
	aws s3 cp s3://$(WEBUI_BUCKET)/settings.js frontend/public/settings.js

start-frontend-local:
	cd frontend && npm start

start-frontend-remote:
	$(eval WEBUI_URL := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIUrl`].OutputValue' --output text))
	open $(WEBUI_URL)

test-frontend:
	cd frontend && npm t

test-unit:
	pytest -m unit --log-cli-level info --cov=backend.lambdas --cov=decorators --cov backend.ecs_tasks

test-acceptance:
	pytest -m acceptance --log-cli-level info

test-no-state-machine:
	pytest -m "not state_machine" --log-cli-level info  --cov=backend.lambdas --cov=decorators --cov backend.ecs_tasks

test:
	pytest --log-cli-level info --cov=backend.lambdas --cov=decorators --cov backend.ecs_tasks
	make test-frontend
