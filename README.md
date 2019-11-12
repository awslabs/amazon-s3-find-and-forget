amazon-s3-find-and-forget
=========================

> Warning: This project is currently being developed and the code shouldn't be used in production.

S3 Find & Forget is a solution for helping customers to find and delete user data in Amazon S3.
It is designed to help customers to fulfil their GDPR’s Right To Be Forgotten obligations when operating in a large Data Lake in a performant, reliable, secure and cost-effective way.

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
aws cloudformation deploy --template-file ./packaged.yaml --stack-name s3f2 --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
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
full acceptance test suite, you'll need to pass the stack name when
running the tests:
```bash
export AWS_PROFILE=default
StackName=yourstack pytest -m acceptance --log-cli-level info
```

>**Important**: The state machine tests are quite slow as they involve
>running end to end state machines. Therefore if you wish to skip these
>tests, use the `-m not state_machine` marker when running the tests

#### Test Data
Available test data files:
- `basic.parquet`:  A simple parquet file containing the following data:
    ```json
    [
      {"customer_id": "12345"},
      {"customer_id": "23456"},
      {"customer_id": "34567"}
    ]
    ```