## Testing

Before running tests, install the dependencies in your virtualenv:
```bash
make setup
```

### Unit Tests

Unit tests are ran using the following command:
```bash
make test-unit
```

### API Acceptance Tests

> **Important**: Acceptance tests require the full CloudFormation stack to be
> deployed. 

Acceptance tests are ran using the following command:
```bash
make test-acceptance
```

Because the state machine tests involve running full end to end
job executions, tests can take >20s each. Therefore if you wish
to skip these tests, run `make test-no-state-machine`.


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
