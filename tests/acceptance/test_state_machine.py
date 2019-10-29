import logging
import tempfile
import time
import pytest
from botocore.exceptions import WaiterError, ClientError

from . import generate_parquet_file, query_parquet_file

logger = logging.getLogger()

pytestmark = [pytest.mark.acceptance, pytest.mark.state_machine, pytest.mark.usefixtures("empty_queue"),
              pytest.mark.usefixtures("empty_lake"), pytest.mark.skip]


@pytest.mark.parametrize('del_queue_item', [["12345", []]], indirect=True)
def test_it_executes_successfully_for_deletion_queue(del_queue_item, dummy_lake, execution_waiter, execution):
    # Generate a parquet file and add it to the lake
    object_key = "{}/2019/08/20/test.parquet".format(dummy_lake["prefix"])
    parquet_file = generate_parquet_file(["customer_id"], [
        ["12345"],
        ["23456"],
        ["34567"],
    ])
    bucket = dummy_lake["bucket"]
    bucket.upload_fileobj(parquet_file, object_key)
    # Act
    try:
        execution_waiter.wait(executionArn=execution["executionArn"])
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))


def test_it_skips_empty_deletion_queue(sf_client, execution, execution_waiter):
    try:
        execution_waiter.wait(executionArn=execution["executionArn"])
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    history = sf_client.get_execution_history(executionArn=execution["executionArn"])
    did_enter_success = next((event for event in history["events"] if event["type"] == "SucceedStateEntered"), False)
    assert did_enter_success, "Did not enter the success state associated with an empty deletion queue"
    assert did_enter_success["stateEnteredEventDetails"]["name"] == "No"


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
        logger.warning("Unable to stop execution: %s", str(e))
    finally:
        assert did_wait, "Did not enter the wait state associated with concurrent executions"

