import logging
import tempfile
import pytest
from botocore.exceptions import WaiterError

from . import query_parquet_file

logger = logging.getLogger()

pytestmark = [pytest.mark.acceptance, pytest.mark.state_machine, pytest.mark.usefixtures("empty_queue"),
              pytest.mark.usefixtures("empty_lake", "empty_jobs")]


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


def test_it_skips_empty_deletion_queue(sf_client, execution, execution_waiter):
    try:
        execution_waiter.wait(executionArn=execution["executionArn"])
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))
    finally:
        sf_client.stop_execution(executionArn=execution["executionArn"])

    history = sf_client.get_execution_history(executionArn=execution["executionArn"])
    did_enter_skip = next((event for event in history["events"]
                           if event.get("stateEnteredEventDetails", {}).get("name") == "No"), False)
    assert did_enter_skip, "Did not enter the success state associated with an empty deletion queue"
