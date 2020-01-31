from types import SimpleNamespace

from mock import patch
import pytest
from backend.lambdas.tasks.emit_event import handler


pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.handle_failure.emit_event")
def test_it_logs_event(mock_emit):
    handler({
        "version": "0",
        "id": "315c1398-40ff-a850-213b-158f73e60175",
        "detail-type": "Step Functions Execution Status Change",
        "source": "aws.states",
        "account": "012345678912",
        "time": "2019-02-26T19:42:21Z",
        "region": "us-east-1",
        "resources": [
          "arn:aws:states:us-east-1:012345678912:execution:state-machine-name:execution-name"
        ],
        "detail": {
            "executionArn": "arn:aws:states:us-east-1:012345678912:execution:state-machine-name:execution-name",
            "stateMachineArn": "arn:aws:states:us-east-1:012345678912:stateMachine:state-machine",
            "name": "execution-name",
            "status": "TIMED_OUT",
            "startDate": 1551224926156,
            "stopDate": 1551224927157,
            "input": "{}",
            "output": None
        }
    }, SimpleNamespace())
    mock_emit.assert_called_with("execution-name", "Exception", {
        "Error": "State Machine TIMED_OUT",
        "Cause": "Unknown error occurred. Check the execution history for more details"
    }, "CloudWatchEvents")
