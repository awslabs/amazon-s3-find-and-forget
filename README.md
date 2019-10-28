jane-doe
=================

TODO: Project description

> Warning: This project is currently being developed and the code shouldn't be used in production.

# Deploy

1. Setup a virtual environment

```bash
virtualenv venv
source venv/bin/activate
```

2. Install the layers
```bash
pip install -r lambdas/layers/aws_sdk/requirements.txt -t lambdas/layers/aws_sdk/python
pip install -r lambdas/layers/decorators/requirements.txt -t lambdas/layers/decorators/python
```

3. Deploy using the CLI
```bash
aws cloudformation package --template-file templates/template.yaml --s3-bucket your-temp-bucket --output-template-file packaged.yaml
aws cloudformation deploy --template-file ./packaged.yaml --stack-name jane-doe --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
```

## Testing

Before running tests, install the dependencies in your virtualenv:
```bash
pip install -r requirements.txt 
```

### Unit Tests
Unit tests can be ran by using the pytest mark `unit` i.e.
```bash
pytest -m unit --log-cli-level info
```

Append `--cov=lambdas.src --cov=decorators` if using `pytest-cov` to get coverage
stats

### API Acceptance Tests

#### Using AWS
Acceptance tests require the full AWS stack to be deployed. To run the
full acceptance test suite, you'll need to setup a `.env` file containing
the following info:
```dotenv
ApiUrl=APIGW_URL_FROM_STACK_OUTPUTS
TablePrefix=TABLE_PREFIX_USED_WHEN_DEPLOYING_STACK
ClientId=COGNITO_CLIENT_ID_FROM_STACK_OUTPUTS
UserPoolId=COGNITO_USER_POOL_ID_FROM_STACK_OUTPUTS
StateMachineArn=ARN_OF_STATE_MACHINE
DatabaseName=GLUE_DATABASE_NAME
```

Then run the acceptance tests:
```bash
export AWS_PROFILE=default
pytest -m acceptance --log-cli-level info
```
