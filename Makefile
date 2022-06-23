SHELL := /bin/bash

.PHONY : deploy deploy-containers pre-deploy setup test test-cov test-acceptance test-acceptance-cov test-no-state-machine test-no-state-machine-cov test-unit test-unit-cov

# The name of the virtualenv directory to use
VENV ?= venv

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
	npm run build --workspace frontend

deploy:
	make pre-deploy
	make deploy-artefacts
	make deploy-cfn
	make setup-frontend-local-dev

deploy-vpc:
	aws cloudformation create-stack --template-body file://templates/vpc.yaml --stack-name S3F2-VPC

deploy-cfn:
	aws cloudformation package --template-file templates/template.yaml --s3-bucket $(TEMP_BUCKET) --output-template-file packaged.yaml
	aws cloudformation deploy --template-file ./packaged.yaml --stack-name S3F2 --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
		--parameter-overrides CreateCloudFrontDistribution=false EnableContainerInsights=true AdminEmail=$(ADMIN_EMAIL) \
		AccessControlAllowOriginOverride=* PreBuiltArtefactsBucketOverride=$(TEMP_BUCKET) KMSKeyArns=$(KMS_KEYARNS)

deploy-artefacts:
	$(eval VERSION := $(shell $(MAKE) -s version))
	make package-artefacts
	aws s3 cp build.zip s3://$(TEMP_BUCKET)/amazon-s3-find-and-forget/$(VERSION)/build.zip

.PHONY: format-cfn
format-cfn:
	$(eval VERSION := $(shell $(MAKE) -s version))
	TEMP_FILE="$$(mktemp)" ; \
		sed  -e '3s/.*/Description: Amazon S3 Find and Forget \(uksb-1q2j8beb0\) \(version:$(VERSION)\)/' templates/template.yaml > "$$TEMP_FILE" ; \
		mv "$$TEMP_FILE" templates/template.yaml 
	git add templates/template.yaml

.PHONY: format-docs
format-docs:
	npx prettier ./*.md ./docs/*.md --write
	git add *.md
	git add docs/*.md

.PHONY: format-js
format-js:
	npx prettier ./frontend/src/**/*.js --write
	git add frontend/src/

.PHONY: format-python
format-python: | $(VENV)
	for src in \
		tests/ \
		backend/ecs_tasks/ \
		backend/lambdas/ \
		backend/lambda_layers/boto_utils/python/boto_utils.py \
		backend/lambda_layers/decorators/python/decorators.py \
	; do \
		$(VENV)/bin/black "$$src" \
	; done

generate-api-docs:
	TEMP_FILE="$$(mktemp)" ; \
		$(VENV)/bin/yq -y .Resources.Api.Properties.DefinitionBody ./templates/api.yaml > "$$TEMP_FILE" ; \
		npx openapi-generator generate -i "$$TEMP_FILE" -g markdown -t ./docs/templates/ -o docs/api
	git add docs/api

.PHONY: generate-pip-requirements
generate-pip-requirements: $(patsubst %.in,%.txt,$(shell find . -type f -name requirements.in))

.PHONY: lint-cfn
lint-cfn:
	cfn-lint templates/*

package:
	make package-artefacts
	zip -r packaged.zip \
		backend/lambda_layers \
		backend/lambdas \
		build.zip \
		cfn-publish.config \
		templates \
		-x '**/__pycache*' '*settings.js' @

package-artefacts: backend/ecs_tasks/python_3.7-slim.tar
	make build-frontend
	zip -r build.zip \
		backend/ecs_tasks/ \
		backend/lambda_layers/boto_utils/ \
		frontend/build \
		-x '**/__pycache*' '*settings.js' @

backend/ecs_tasks/python_3.7-slim.tar:
	docker pull python:3.7-slim
	docker save python:3.7-slim -o "$@"

redeploy-containers:
	$(eval ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text))
	$(eval REGION := $(shell aws configure get region))
	$(eval ECR_REPOSITORY := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`ECRRepository`].OutputValue' --output text))
	$(shell aws ecr get-login --no-include-email --region $(REGION))
	docker build -t $(ECR_REPOSITORY) -f backend/ecs_tasks/delete_files/Dockerfile .
	docker tag $(ECR_REPOSITORY):latest $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(ECR_REPOSITORY):latest
	docker push $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(ECR_REPOSITORY):latest

redeploy-frontend:
	$(eval WEBUI_BUCKET := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIBucket`].OutputValue' --output text))
	make build-frontend
	cd frontend/build && aws s3 cp --recursive . s3://$(WEBUI_BUCKET) --acl public-read --exclude *settings.js

run-local-container:
	make pre-run
	./docker_run_with_creds.sh

setup: | $(VENV) lambda-layer-deps
	(! [[ -d .git ]] || $(VENV)/bin/pre-commit install)
	npm i
	gem install cfn-nag

# virtualenv setup
.PHONY: $(VENV)
$(VENV): $(VENV)/pip-sync.sentinel

$(VENV)/pip-sync.sentinel: requirements.txt | $(VENV)/bin/pip-sync
	$(VENV)/bin/pip-sync $<
	touch $@

$(VENV)/bin/activate:
	test -d $(VENV) || virtualenv $(VENV)

$(VENV)/bin/pip-compile $(VENV)/bin/pip-sync: $(VENV)/bin/activate
	$(VENV)/bin/pip install pip-tools

# Lambda layers
.PHONY: lambda-layer-deps
lambda-layer-deps: \
	backend/lambda_layers/aws_sdk/requirements-installed.sentinel \
	backend/lambda_layers/cr_helper/requirements-installed.sentinel \
	backend/lambda_layers/decorators/requirements-installed.sentinel \
	;

backend/lambda_layers/%/requirements-installed.sentinel: backend/lambda_layers/%/requirements.txt | $(VENV)
	@# pip-sync only works with virtualenv, so we can't use it here.
	$(VENV)/bin/pip install -r $< -t $(subst requirements-installed.sentinel,python,$@)
	touch $@

setup-frontend-local-dev:
	$(eval WEBUI_URL := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIUrl`].OutputValue' --output text))
	$(eval WEBUI_BUCKET := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIBucket`].OutputValue' --output text))
	$(if $(filter none, $(WEBUI_URL)), @echo "WebUI not deployed.", aws s3 cp s3://$(WEBUI_BUCKET)/settings.js frontend/public/settings.js)

setup-predeploy:
	virtualenv venv
	source venv/bin/activate
	pip install cfn-flip==1.2.2

start-frontend-local:
	npm start --workspace frontend

start-frontend-remote:
	$(eval WEBUI_URL := $(shell aws cloudformation describe-stacks --stack-name S3F2 --query 'Stacks[0].Outputs[?OutputKey==`WebUIUrl`].OutputValue' --output text))
	$(if $(filter none, $(WEBUI_URL)), @echo "WebUI not deployed.", open $(WEBUI_URL))

test-cfn:
	cfn_nag templates/*.yaml --blacklist-path ci/cfn_nag_blacklist.yaml

test-frontend:
	npm t --workspace frontend

test-unit: | $(VENV)
	$(VENV)/bin/pytest -m unit --log-cli-level info --cov=backend.lambdas --cov=decorators --cov=boto_utils --cov=backend.ecs_tasks --cov-report term-missing

test-ci: | $(VENV)
	$(VENV)/bin/pytest -m unit --log-cli-level info --cov=backend.lambdas --cov=decorators --cov=boto_utils --cov=backend.ecs_tasks --cov-report xml

test-acceptance-cognito: | $(VENV)
	$(VENV)/bin/pytest -m acceptance_cognito --log-cli-level info

test-acceptance-iam: | $(VENV)
	$(VENV)/bin/pytest -m acceptance_iam --log-cli-level info

test-no-state-machine: | $(VENV)
	$(VENV)/bin/pytest -m "not state_machine" --log-cli-level info  --cov=backend.lambdas --cov=boto_utils --cov=decorators --cov=backend.ecs_tasks

test: | $(VENV)
	make test-cfn
	$(VENV)/bin/pytest --log-cli-level info --cov=backend.lambdas --cov=decorators --cov=boto_utils --cov=backend.ecs_tasks
	make test-frontend

version:
	@echo $(shell $(VENV)/bin/cfn-flip templates/template.yaml | $(VENV)/bin/python -c 'import sys, json; print(json.load(sys.stdin)["Mappings"]["Solution"]["Constants"]["Version"])')

%/requirements.txt: %/requirements.in | $(VENV)/bin/pip-compile
	$(VENV)/bin/pip-compile -q -o $@ $<

requirements.txt: requirements.in $(shell awk '/^-r / { print $$2 }' requirements.in) | $(VENV)/bin/pip-compile
	$(VENV)/bin/pip-compile -q -o $@ $<
