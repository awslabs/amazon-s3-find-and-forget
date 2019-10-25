import logging
import time
import pytest
from botocore.exceptions import WaiterError, ClientError

logger = logging.getLogger()

pytestmark = [pytest.mark.acceptance, pytest.mark.state_machine, pytest.mark.usefixtures("empty_queue")]


def test_it_executes_successfully_for_deletion_queue(sf_client, execution, del_queue_item, execution_waiter):
    # TODO: Create dummy lake
    try:
        execution_waiter.wait(executionArn=execution["executionArn"])
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))
    finally:
        sf_client.stop_execution(executionArn=execution["executionArn"])
    # TODO: assert user is no longer in the data lake


def test_it_skips_empty_deletion_queue(sf_client, execution, execution_waiter):
    try:
        execution_waiter.wait(executionArn=execution["executionArn"])
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    history = sf_client.get_execution_history(executionArn=execution["executionArn"])
    did_enter_success = next((event for event in history["events"] if event["type"] == "SucceedStateEntered"), False)
    assert did_enter_success, "Did not enter the success state associated with an empty deletion queue"


def test_it_only_permits_single_executions(state_machine, sf_client):
    # Start 2 concurrent executions
    first_execution = sf_client.start_execution(stateMachineArn=state_machine["stateMachineArn"])
    second_execution = sf_client.start_execution(stateMachineArn=state_machine["stateMachineArn"])

    # Check if the second execution entered wait state. Timeout 5 seconds
    did_wait = False
    limit = time.time() + 5
    while time.time() < limit:
        history = sf_client.get_execution_history(executionArn=second_execution["executionArn"])
        did_wait = next((event for event in history["events"] if event["type"] == "WaitStateEntered"), False)

    # Cleanup and perform the assertion
    try:
        sf_client.stop_execution(executionArn=first_execution["executionArn"])
        sf_client.stop_execution(executionArn=second_execution["executionArn"])
    except ClientError as e:
        logger.info("Unable to stop execution: %s", str(e))
    finally:
        assert did_wait, "Did not enter the wait state associated with concurrent executions"


def test_it_errors_for_non_existent_configuration(execution, execution_waiter):
    # TODO: Create queue with misconfiguration
    with pytest.raises(WaiterError):
        execution_waiter.wait(executionArn=execution["executionArn"])
    # TODO: Assert fails at query generation state


def test_it_errors_for_invalid_configuration_schema(execution, execution_waiter):
    # TODO: Create lake with JSON data in
    with pytest.raises(WaiterError):
        execution_waiter.wait(executionArn=execution["executionArn"])
    # TODO: Assert fails at query execution state
