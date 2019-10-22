jane-doe
=================

TODO: Project description

> Warning: This project is currently being developed and the code shouldn't be used in production.

# Deploy

1. Setup a virtual environment

```
virtualenv venv
source venv/bin/activate
```

2. Install the layers
```
pip install -r lambdas/layers/aws_sdk/requirements.txt -t lambdas/layers/aws_sdk/python
pip install -r lambdas/layers/decorators/requirements.txt -t lambdas/layers/decorators/python
```

3. Deploy using the CLI
```
aws cloudformation package --template-file templates/template.yaml --s3-bucket your-temp-bucket --output-template-file packaged.yaml
aws cloudformation deploy --template-file ./packaged.yaml --stack-name jane-doe --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
```

## Testing

### Unit Tests
Unit tests can be ran by using the pytest mark `unit` i.e.
```
pytest -m unit --log-cli-level info
```

### API Acceptance Tests

#### Using AWS
Some acceptance tests require the full AWS stack to be deployed. To run the
full acceptance test suite, you'll need to setup a `.env` file containing
the following info:
```
ApiUrl=APIGW_URL_FROM_STACK_OUTPUTS
TablePrefix=TABLE_PREFIX_USED_WHEN_DEPLOYING_STACK
ClientId=COGNITO_CLIENT_ID_FROM_STACK_OUTPUTS
UserPoolId=COGNITO_USER_POOL_ID_FROM_STACK_OUTPUTS
StepFunctionsRoleArn=ROLE_ARN_FOR_STEP_FUNCTIONS
```

Then run the acceptance tests:
```
export AWS_PROFILE=default
pytest -m acceptance --log-cli-level info
```

All tests which rely on AWS not being mocked should be marked using the `needs_aws`
marker. e.g.:
```python
import pytest

@pytest.mark.needs_aws
def test_something():
    pass
```

#### Using SAM Local
To run end to end tests using SAM local, DDB local and Step Functions local, you first need to 
have SAM local and DDB local running. The easiest way to do this is using Docker: 
```
docker network create lambda-local
docker run -p 8000:8000 --name dynamodb --network=lambda-local amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory -sharedDb
docker run -p 8083:8083 --name stepfunctions --network=lambda-local -e AWS_DEFAULT_REGION=eu-west-1 amazon/aws-stepfunctions-local
sam local start-api --template templates/api.yaml --docker-network lambda-local --env-vars tests/acceptance/env_vars.json
```
Then run the tests not marked as requiring AWS:
```
RunningLocal=true pytest -m acceptance -m "not needs_aws" --log-cli-level info
```
