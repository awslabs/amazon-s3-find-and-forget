SHELL = /bin/bash

.PHONY : deploy pre-deploy setup test test-cov test-acceptance test-acceptance-cov test-no-state-machine test-no-state-machine-cov test-unit test-unit-cov

pre-deploy:
ifndef TEMP_BUCKET
	$(error TEMP_BUCKET is undefined)
endif

deploy:
	make pre-deploy
	aws cloudformation package --template-file templates/template.yaml --s3-bucket $(TEMP_BUCKET) --output-template-file packaged.yaml
	aws cloudformation deploy --template-file ./packaged.yaml --stack-name S3F2 --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND

setup:
	virtualenv venv
	source venv/bin/activate
	pip install -r backend/lambda_layers/aws_sdk/requirements.txt -t backend/lambda_layers/aws_sdk/python
	pip install -r backend/lambda_layers/decorators/requirements.txt -t backend/lambda_layers/decorators/python
	pip install -r requirements.txt 

test-unit:
	pytest -m unit --log-cli-level info

test-unit-cov:
	pytest -m unit --log-cli-level info --cov=backend.lambdas --cov=decorators

test-acceptance:
	pytest -m acceptance --log-cli-level info

test-acceptance-cov:
	pytest -m acceptance --log-cli-level info --cov=backend.lambdas --cov=decorators

test-no-state-machine:
	pytest -m "not state_machine" --log-cli-level info

test-no-state-machine-cov:
	pytest -m "not state_machine" --log-cli-level info  --cov=backend.lambdas --cov=decorators

test:
	pytest --log-cli-level info

test-cov:
	pytest --log-cli-level info --cov=backend.lambdas --cov=decorators
