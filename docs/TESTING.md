## Testing

Before running tests, install the dependencies in your virtualenv:
```bash
make setup
```

### Unit Tests

Unit tests can be ran by running
```bash
make test-unit
```

To get coverage stats using `pytest-cov` run:
```bash
make test-unit-cov
```

### API Acceptance Tests

> **Important**: Acceptance tests require the full AWS stack to be deployed.

To run the full acceptance test suite run:
```bash
make test-acceptance
```

To get coverage stats using `pytest-cov` run:
```bash
make test-acceptance-cov
```

> **Important**: The state machine tests are quite slow as they involve running end to end state machines. Therefore if you wish to skip these tests, run `make test-no-state-machine`.

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
