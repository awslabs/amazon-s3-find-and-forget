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
