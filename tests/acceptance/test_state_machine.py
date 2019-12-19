import logging
import tempfile
import time
import pytest
from botocore.exceptions import WaiterError, ClientError

from . import query_parquet_file

logger = logging.getLogger()

pytestmark = [pytest.mark.acceptance, pytest.mark.state_machine, pytest.mark.usefixtures("empty_queue"),
              pytest.mark.usefixtures("empty_lake")]


@pytest.mark.skip
def test_it_executes_successfully_for_deletion_queue(del_queue_factory, dummy_lake, execution_waiter, stack, sf_client,
                                                     glue_data_mapper_factory, data_loader):
    # Generate a parquet file and add it to the lake
    glue_data_mapper_factory("test", partition_keys=["year", "month", "day"], partitions=[["2019", "08", "20"]])
    del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    # Act
    execution_arn = sf_client.start_execution(stateMachineArn=stack["StateMachineArn"])["executionArn"]
    try:
        execution_waiter.wait(executionArn=execution_arn)
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))
    finally:
        sf_client.stop_execution(executionArn=execution_arn)

    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))


def test_it_logs_phases(del_queue_factory, dummy_lake, execution_waiter, stack, sf_client, logs_client,
                        glue_data_mapper_factory, data_loader):
    # Generate a parquet file and add it to the lake
    glue_data_mapper_factory("test", partition_keys=["year", "month", "day"], partitions=[["2019", "08", "20"]])
    del_queue_factory("not_in_dataset")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    # Act
    execution_arn = sf_client.start_execution(stateMachineArn=stack["StateMachineArn"])["executionArn"]
    try:
        execution_waiter.wait(executionArn=execution_arn)
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))
    finally:
        sf_client.stop_execution(executionArn=execution_arn)

    # Assert
    log_group = stack["AuditLogGroup"]
    job_id = sf_client.describe_execution(executionArn=execution_arn)["name"]
    for phase in ["FindPhaseStarted", "FindPhaseEnded", "ForgetPhaseStarted", "ForgetPhaseEnded"]:
        resp = logs_client.filter_log_events(logGroupName=log_group, logStreamNamePrefix=job_id, filterPattern=phase)
        assert 1 == len(resp["events"])


def test_it_skips_empty_deletion_queue(sf_client, execution, execution_waiter):
    try:
        execution_waiter.wait(executionArn=execution["executionArn"])
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    history = sf_client.get_execution_history(executionArn=execution["executionArn"])
    did_enter_skip = next((event for event in history["events"]
                           if event["stateEnteredEventDetails"]["name"] == "No"), False)
    assert did_enter_skip, "Did not enter the success state associated with an empty deletion queue"


def test_it_only_permits_single_executions(stack, sf_client, del_queue_factory):
    # Start 2 concurrent executions
    del_queue_factory("12345")
    first_execution = sf_client.start_execution(stateMachineArn=stack["StateMachineArn"])
    second_execution = sf_client.start_execution(stateMachineArn=stack["StateMachineArn"])

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

