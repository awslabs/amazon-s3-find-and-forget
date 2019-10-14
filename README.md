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
```

3. Deploy using the CLI
```
aws cloudformation package --template-file templates/template.yaml --s3-bucket your-temp-bucket --output-template-file packaged.yaml
aws cloudformation deploy --template-file ./packaged.yaml --stack-name jane-doe --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
```

## Testing

### Acceptance Tests
To run end to end tests using SAM local and DDB local, you first need to 
have SAM local and DDB local running on a local docker network: 
```
docker network create lambda-local
docker run -p 8000:8000 --name dynamodb --network=lambda-local amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory -sharedDb
sam local start-lambda --template templates/api.yaml --docker-network lambda-local --env-vars tests/acceptance/env_vars.json
```
To run the tests:
```
pytest --log-cli-level info
```
